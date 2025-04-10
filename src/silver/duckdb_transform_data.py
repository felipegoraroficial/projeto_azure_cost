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

    # Configuração do cliente S3 (MinIO)
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id=os.getenv('KEY_ACCESS'),
        aws_secret_access_key=os.getenv('KEY_SECRETS')
    )

    # Nome do bucket e arquivo Parquet
    bucket_name = 'azurecost'
    file_name = 'bronze/dados.parquet'

    # Fazendo o download do arquivo Parquet
    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    parquet_data = BytesIO(response['Body'].read())  # Carrega os dados como um buffer em memória

    # Salvar o buffer em um arquivo temporário
    temp_file_path = "temp_dados.parquet"
    with open(temp_file_path, "wb") as temp_file:
        temp_file.write(parquet_data.getvalue())

    # Usar DuckDB para processar o arquivo Parquet diretamente
    con = duckdb.connect("azurecost.db")

    # Criar uma tabela temporária a partir do arquivo Parquet
    con.execute(f"CREATE TEMP TABLE IF NOT EXISTS temp_table AS SELECT * FROM parquet_scan('{temp_file_path}')")

    # Criar uma nova tabela com a coluna `ResourceId` excluída, colunas extras incluídas e sem duplicatas
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS updated_table AS
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
    FROM temp_table;
    """)

    # **Exibir os dados transformados em formato tabular**
    print("Dados transformados:")
    query = "SELECT * FROM updated_table LIMIT 10"  # Limitar a 10 registros para exibição
    result = con.execute(query).fetchall()
    columns = [desc[0] for desc in con.execute("DESCRIBE updated_table").fetchall()]
    print(tabulate(result, headers=columns, tablefmt="grid"))

    # **Exibir o DESCRIBE da tabela**
    print("\nEstrutura da tabela (DESCRIBE):")
    describe_query = "DESCRIBE updated_table"
    describe_result = con.execute(describe_query).fetchall()
    print(tabulate(describe_result, headers=["Column Name", "Data Type", "Nullable"], tablefmt="grid"))

    # **Salvar os dados transformados em Parquet**
    clean_query = """
    SELECT DISTINCT
        PreTaxCost,
        UsageDate,
        CAST(ResourceGroup AS VARCHAR) AS ResourceGroup,
        CAST(Currency AS VARCHAR) AS Currency,
        CAST(subscriptions AS VARCHAR) AS subscriptions,
        CAST(providers AS VARCHAR) AS providers,
        CAST(recurso AS VARCHAR) AS recurso
    FROM updated_table
    """
    cleaned_data_path = "cleaned_dados.parquet"
    con.execute(f"COPY ({clean_query}) TO '{cleaned_data_path}' (FORMAT 'parquet')")

    # Fazer o upload do arquivo Parquet para o MinIO
    with open(cleaned_data_path, "rb") as output_file:
        s3_client.put_object(
            Bucket=bucket_name,
            Key='silver/processed_dados.parquet',  # Caminho no MinIO
            Body=output_file,
            ContentType='application/octet-stream'
        )

    print(f"\nArquivo salvo com sucesso no MinIO: silver/processed_dados.parquet")

    # Limpar arquivos temporários locais
    os.remove(temp_file_path)
    os.remove(cleaned_data_path)
