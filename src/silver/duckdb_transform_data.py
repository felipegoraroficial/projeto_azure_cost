import boto3
import duckdb
import os
from dotenv import load_dotenv
from io import BytesIO
from tabulate import tabulate

def normalizar_dados():

    # Caminho relativo
    env_path = os.path.join(os.getcwd(), '.env')
    load_dotenv(dotenv_path=env_path)

    # Nome da tabela processada ao extrair dados do MongoDB
    nome_tabela = "silver_data"

    #  Configurações do Minio
    minio_endpoint = 'minio:9000'
    minio_access_key = os.getenv('MINIO_ROOT_USER')
    minio_secret_key = os.getenv('MINIO_ROOT_PASSWORD')
    bucket_name = 'azurecost'
    bronze_file = 'bronze/dados.parquet'
    bronze_file_path = f"s3://{bucket_name}/{bronze_file}"
    silver_file = 'silver/dados.parquet'
    silver_file_path = f"s3://{bucket_name}/{silver_file}"

    # Conectar ao DuckDB diretamente a memoria RAM
    con = duckdb.connect('azurecost.db')

    #  Instalar e carregar a extensão httpfs para acessar serviços HTTP(S) como S3
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    print("Extensão httpfs instalada e carregada.")

    #  Configurar as credenciais do Minio
    con.execute(f"""
        SET s3_endpoint='{minio_endpoint}';
        SET s3_access_key_id='{minio_access_key}';
        SET s3_secret_access_key='{minio_secret_key}';
        SET s3_use_ssl=False;
        SET s3_url_style='path';
    """)
    print("Credenciais do Minio configuradas.")

    # Criar uma nova tabela com os dados transformados em silver
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {nome_tabela} AS
    SELECT DISTINCT
        PreTaxCost,
        CAST(
            SUBSTR(CAST(UsageDate AS VARCHAR), 1, 4) || '-' ||
            SUBSTR(CAST(UsageDate AS VARCHAR), 5, 2) || '-' ||
            SUBSTR(CAST(UsageDate AS VARCHAR), 7, 2) AS DATE
        ) AS UsageDate,
        ResourceGroup,
        Currency,
        REGEXP_EXTRACT(ResourceId, '/subscriptions/([^/]+)/', 1) AS subscriptions,
        REGEXP_EXTRACT(ResourceId, '/providers/([^/]+)/', 1) AS providers,
        REGEXP_EXTRACT(ResourceId, '/providers/[^/]+/([^/]+)/', 1) AS recurso
    FROM '{bronze_file_path}';
    """)

    # **Exibir os dados transformados em formato tabular**
    print("Dados transformados:")
    query = f"SELECT * FROM {nome_tabela} ORDER BY UsageDate DESC LIMIT 10"
    result = con.execute(query).fetchall()
    columns = [desc[0] for desc in con.execute(f"DESCRIBE {nome_tabela}").fetchall()]
    print(tabulate(result, headers=columns, tablefmt="grid"))

    # **Exibir o DESCRIBE da tabela**
    print("\nEstrutura da tabela (DESCRIBE):")
    describe_query = f"DESCRIBE {nome_tabela}"
    describe_result = con.execute(describe_query).fetchall()
    print(tabulate(describe_result, headers=["Column Name", "Data Type", "Nullable"], tablefmt="grid"))

    # Salvar a tabela do DuckDB para o bucket do Minio em formato Parquet
    con.execute(f"COPY {nome_tabela} TO '{silver_file_path}' (FORMAT PARQUET);")
    print(f"Tabela '{nome_tabela}' salva com sucesso em '{silver_file_path}' no bucket '{bucket_name}'.")
