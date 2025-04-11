import duckdb
from minio import Minio
from minio.error import S3Error
import os
from dotenv import load_dotenv
from tabulate import tabulate

def carregar_dados():

    # Caminho relativo a variavel de ambiente
    env_path = os.path.join(os.getcwd(), '.env')
    load_dotenv(dotenv_path=env_path)

    # Conectar ao DuckDB diretamente a memoria RAM
    con = duckdb.connect(database=':memory:')

    # Nome da tabela processada ao extrair dados do MongoDB
    nome_tabela = "dados_mongo"

    # Consultar os dados da tabela e contar as linhas
    try:
        # Executar uma consulta SELECT simples para obter todos os dados
        query_data = f"SELECT * FROM {nome_tabela} LIMIT 10;"
        result = con.execute(query_data).fetchall()
        headers = [desc[0] for desc in con.execute(query_data).description]

        # Executar uma consulta COUNT para obter o número de linhas
        query_count = f"SELECT COUNT(*) FROM {nome_tabela};"
        count_result = con.execute(query_count).fetchone()
        num_linhas = count_result[0] if count_result else 0

        if result:
            print(f"\nDados da tabela '{nome_tabela}':")
            print(tabulate(result, headers=headers, tablefmt="grid"))
            print(f"\nNúmero total de linhas na tabela '{nome_tabela}': {num_linhas}")
        else:
            print(f"\nA tabela '{nome_tabela}' está vazia.")
            print(f"\nNúmero total de linhas na tabela '{nome_tabela}': {num_linhas}")

    except duckdb.Error as e:
        print(f"Ocorreu um erro ao consultar a tabela '{nome_tabela}': {e}")

    # Configurar o cliente MinIO
    client = Minio(
        'minio:9000',  
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
    minio_endpoint = 'minio:9000'
    minio_access_key = os.getenv('MINIO_ROOT_USER')
    minio_secret_key = os.getenv('MINIO_ROOT_PASSWORD')
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