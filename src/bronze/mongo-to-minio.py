from pymongo import MongoClient
import duckdb
from tabulate import tabulate
import boto3
import os
from dotenv import load_dotenv
from io import BytesIO

# Caminho relativo
env_path = os.path.join(os.getcwd(), '.env')
load_dotenv(dotenv_path=env_path)

# Conexão ao MongoDB
client = MongoClient("mongodb://fececa:fececa13@localhost:27017/")
db = client["azurecost"]  

# Variável para armazenar todas as linhas
todas_as_linhas = []

# Listando todas as coleções no banco de dados
colecoes = db.list_collection_names()

# Iterar sobre as coleções e processar os dados
for colecao_nome in colecoes:
    colecao = db[colecao_nome]
    documentos = colecao.find()
    for documento in documentos:
        # Extrair colunas e linhas da propriedade 'properties'
        colunas = [col["name"] for col in documento.get("properties", {}).get("columns", [])]
        linhas = documento.get("properties", {}).get("rows", [])
        for linha in linhas:
            # Criar um dicionário para cada linha com base nas colunas
            todas_as_linhas.append(dict(zip(colunas, linha)))

# Conectar ao DuckDB
con = duckdb.connect()

# Criar uma tabela temporária e inserir os dados manualmente
con.execute("""
CREATE TABLE todas_as_linhas (
    PreTaxCost DOUBLE,
    UsageDate INT,
    ResourceGroup VARCHAR,
    ResourceId VARCHAR,
    Currency VARCHAR
)
""")

# Inserir os dados da lista todas_as_linhas na tabela
for linha in todas_as_linhas:
    con.execute(
        "INSERT INTO todas_as_linhas VALUES (?, ?, ?, ?, ?)",
        (linha["PreTaxCost"], linha["UsageDate"], linha["ResourceGroup"], linha["ResourceId"], linha["Currency"])
    )

# Executar consultas diretamente na tabela
result = con.execute("SELECT * FROM todas_as_linhas LIMIT 10").fetchall()

# Obter os nomes das colunas
columns = [desc[0] for desc in con.execute("DESCRIBE todas_as_linhas").fetchall()]

# Exibir os resultados em formato tabular usando tabulate
print(tabulate(result, headers=columns, tablefmt="psql"))

# Exportar os dados para Parquet
parquet_buffer = BytesIO()
con.execute("COPY todas_as_linhas TO 'dados.parquet' (FORMAT 'parquet')")

# Ler o arquivo Parquet gerado
with open('dados.parquet', 'rb') as parquet_file:
    parquet_buffer.write(parquet_file.read())

parquet_buffer.seek(0)

# Configurar o cliente S3 (MinIO)
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',  # URL do MinIO
    aws_access_key_id=os.getenv('KEY_ACCESS'),        # Substitua pela sua chave de acesso
    aws_secret_access_key=os.getenv('KEY_SECRETS')    # Substitua pela sua chave secreta
)

# Nome do bucket e arquivo no MinIO
bucket_name = 'azurecost'
file_name = 'bronze/dados.parquet'

# Fazer o upload do arquivo Parquet para o bucket MinIO
s3_client.put_object(
    Bucket=bucket_name,
    Key=file_name,
    Body=parquet_buffer.getvalue(),
    ContentType='application/octet-stream'
)

print(f'Arquivo {file_name} salvo com sucesso no bucket {bucket_name}!')

os.remove('dados.parquet')
