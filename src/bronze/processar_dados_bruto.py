from pymongo import MongoClient
from minio import Minio
from minio.error import S3Error
import duckdb
from tabulate import tabulate
import os
from dotenv import load_dotenv

endpoint_mongo = "mongo:27017"
endpoint_minio = 'minio:9000'

def processar_dados(endpoint_mongo,endpoint_minio):

    # Caminho relativo
    env_path = os.path.join(os.getcwd(), '.env')
    load_dotenv(dotenv_path=env_path)

    mongo_user = os.getenv('MONGO_INITDB_ROOT_USERNAME')
    mongo_password = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
    mongo_host = endpoint_mongo

    # Conexão ao MongoDB
    client = MongoClient(f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}/")
    db = client["azurecost"]

    # Variável para armazenar todas as linhas
    dados_mongo = []

    # Listando todas as coleções no banco de dados
    colecoes = db.list_collection_names()

    # Iterar sobre as coleções e processar os dados
    for colecao_nome in colecoes:
        colecao = db[colecao_nome]
        documentos = colecao.find()
        for documento in documentos:
            linha = {
                "_id": documento.get("_id"),
                "PreTaxCost": documento.get("PreTaxCost"),
                "UsageDate": documento.get("UsageDate"),
                "ResourceGroup": documento.get("ResourceGroup"),
                "ResourceId": documento.get("ResourceId"),
                "Currency": documento.get("Currency")
            }
            dados_mongo.append(linha)

    # Conectar ao DuckDB diretamente a memoria RAM
    con = duckdb.connect('src//azurecost.db')

    # Nome da tabela processada ao extrair dados do MongoDB
    nome_tabela = "dados_mongo"

    # Criar uma tabela temporária e inserir os dados manualmente
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {nome_tabela} (
        PreTaxCost DOUBLE,
        UsageDate INT,
        ResourceGroup VARCHAR,
        ResourceId VARCHAR,
        Currency VARCHAR
    )
    """)

    # Inserir os dados da lista dados_mongo na tabela
    for linha in dados_mongo:
        con.execute(
            f"INSERT INTO {nome_tabela} VALUES (?, ?, ?, ?, ?)",
            (linha["PreTaxCost"], linha["UsageDate"], linha["ResourceGroup"], linha["ResourceId"], linha["Currency"])
        )

    query_data = f"SELECT * FROM {nome_tabela} LIMIT 10;"
    # Obter os nomes das colunas
    columns = [desc[0] for desc in con.execute(f"DESCRIBE {nome_tabela}").fetchall()]
    headers = [desc[0] for desc in con.execute(query_data).description]
    # Executar consultas diretamente na tabela
    result = con.execute(query_data).fetchall()

    # Executar uma consulta COUNT para obter o número de linhas
    query_count = f"SELECT COUNT(*) FROM {nome_tabela};"
    count_result = con.execute(query_count).fetchone()
    num_linhas = count_result[0] if count_result else 0

    # Exibir os resultados em formato tabular usando tabulate
    print(tabulate(result, headers=columns, tablefmt="psql"))

    if result:
        print(f"\nDados da tabela '{nome_tabela}':")
        print(tabulate(result, headers=headers, tablefmt="grid"))
        print(f"\nNúmero total de linhas na tabela '{nome_tabela}': {num_linhas}")
    else:
        print(f"\nA tabela '{nome_tabela}' está vazia.")
        print(f"\nNúmero total de linhas na tabela '{nome_tabela}': {num_linhas}")

    # Configurar o cliente MinIO
    client = Minio(
        endpoint_minio,  
        access_key=os.getenv('MINIO_ROOT_USER'),
        secret_key=os.getenv('MINIO_ROOT_PASSWORD'),
        secure=False
    )

    # Nome do Bucket e Nome do Diretorio
    bucket_name = 'azurecost'
    file_name = 'bronze/dados.parquet'

    # Verificar se o bucket já existe e caso não exista, criar o bucket
    try:
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' criado com sucesso!")
        else:
            print(f"Bucket '{bucket_name}' já existe.")
    except S3Error as err:
        print(f"Erro ao interagir com o bucket: {err}")

    #  Configurações do Minio 
    minio_endpoint = endpoint_minio
    minio_access_key = os.getenv('KEY_ACCESS')
    minio_secret_key = os.getenv('KEY_SECRETS')
    output_file_path = f"s3://{bucket_name}/{file_name}" # Caminho para salvar no Minio

    #  Instalar e carregar a extensão httpfs para acessar serviços HTTP(S) como S3 
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    print("Extensão httpfs instalada e carregada.")

    #  Configurar as credenciais do Minio 
    con.execute(f"""
        SET s3_endpoint='{minio_endpoint}';
        SET s3_access_key_id='{minio_access_key}';
        SET s3_secret_access_key='{minio_secret_key}';
        SET s3_use_ssl=False; -- Se o seu Minio não estiver usando SSL, ajuste para False
        SET s3_url_style='path'; -- Pode ser 'path' ou 'virtual' dependendo da configuração do seu Minio
    """)
    print("Credenciais do Minio configuradas.")

    # Salvar a tabela do DuckDB para o bucket do Minio em formato Parquet
    con.execute(f"COPY {nome_tabela} TO '{output_file_path}' (FORMAT PARQUET);")
    print(f"Tabela '{nome_tabela}' salva com sucesso em '{output_file_path}' no bucket '{bucket_name}'.")

