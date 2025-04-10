import boto3
import duckdb
import os
from dotenv import load_dotenv
from io import BytesIO
from tabulate import tabulate
from datetime import datetime


def transformar_dados():

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
    file_name = 'silver/processed_dados.parquet'

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

    # Obter a data atual em formato "YYYY-MM-DD"
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Verificar se existe `UsageDate` igual à data atual
    missing_date_groups = con.execute(f"""
    SELECT DISTINCT ResourceGroup, recurso
    FROM temp_table
    EXCEPT
    SELECT ResourceGroup, recurso
    FROM temp_table
    WHERE UsageDate = '{current_date}';
    """).fetchall()

    # Inserir uma nova linha para cada grupo que está faltando a data atual
    for resource_group, resource in missing_date_groups:
        con.execute(f"""
        INSERT INTO temp_table (ResourceGroup, recurso, UsageDate, PreTaxCost)
        VALUES ('{resource_group}', '{resource}', '{current_date}', 0);
        """)

    # Calcular aumento ou diminuição por recurso e grupo de recursos
    con.execute("""
    CREATE TABLE IF NOT EXISTS insights_table AS
    SELECT
        ResourceGroup,
        recurso,
        UsageDate,
        PreTaxCost,
        PreTaxCost - LAG(PreTaxCost) OVER (PARTITION BY ResourceGroup, recurso ORDER BY UsageDate) AS change
    FROM temp_table
    ORDER BY ResourceGroup, recurso, UsageDate;
    """)

    # Consultar e exibir os resultados
    query = "SELECT * FROM insights_table"  # Limitar a 10 registros para exibição
    result = con.execute(query).fetchall()
    columns = [desc[0] for desc in con.execute("DESCRIBE insights_table").fetchall()]

    # Exibir os resultados no formato tabular
    print("Aumento/Diminuição de PreTaxCost por ResourceGroup e Recurso:")
    print(tabulate(result, headers=columns, tablefmt="grid"))

    # Salvar os dados transformados em Parquet
    insights_query = """
    SELECT
        CAST(ResourceGroup AS VARCHAR) AS ResourceGroup,
        CAST(recurso AS VARCHAR) AS recurso,
        UsageDate,
        PreTaxCost,
        change
    FROM insights_table
    """
    insights_data_path = "insights_data.parquet"
    con.execute(f"COPY ({insights_query}) TO '{insights_data_path}' (FORMAT 'parquet')")

    # Fazer o upload do arquivo Parquet para o MinIO
    with open(insights_data_path, "rb") as output_file:
        s3_client.put_object(
            Bucket=bucket_name,
            Key='gold/insights_dados.parquet',  # Caminho no MinIO
            Body=output_file,
            ContentType='application/octet-stream'
        )

    print(f"\nArquivo salvo com sucesso no MinIO: gold/insights_dados.parquet")

    # Remover arquivos temporários locais
    os.remove(temp_file_path)
    os.remove(insights_data_path)