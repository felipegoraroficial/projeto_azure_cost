import duckdb
from datetime import datetime
import os
from dotenv import load_dotenv
from tabulate import tabulate


def transformar_dados():

    # Caminho relativo
    env_path = os.path.join(os.getcwd(), '.env')
    load_dotenv(dotenv_path=env_path)

    # Nome da tabela processada ao extrair dados do MongoDB
    nome_tabela = "gold_data"

    #  Configurações do Minio
    minio_endpoint = 'minio:9000'
    minio_access_key = os.getenv('MINIO_ROOT_USER')
    minio_secret_key = os.getenv('MINIO_ROOT_PASSWORD')
    bucket_name = 'azurecost'
    silver_file = 'silver/dados.parquet'
    silver_file_path = f"s3://{bucket_name}/{silver_file}"
    gold_file = 'gold/dados.parquet'
    gold_file_path = f"s3://{bucket_name}/{gold_file}"

    # Conectar ao DuckDB diretamente a memoria RAM
    con = duckdb.connect(database=':memory:')

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

    # Obter a data atual em formato "YYYY-MM-DD"
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Criar uma tabela temporária a partir do arquivo Parquet
    con.execute(f"CREATE TEMP TABLE IF NOT EXISTS gold_temp_table AS SELECT * FROM read_parquet('{silver_file_path}')")

    # Verificar se existe `UsageDate` igual à data atual
    missing_date_groups = con.execute(f"""
    SELECT DISTINCT ResourceGroup, recurso
    FROM read_parquet('{silver_file_path}')
    EXCEPT
    SELECT ResourceGroup, recurso
    FROM read_parquet('{silver_file_path}')
    WHERE UsageDate = '{current_date}';
    """).fetchall()

    # Inserir uma nova linha para cada grupo que está faltando a data atual
    for resource_group, resource in missing_date_groups:
        con.execute(f"""
        INSERT INTO gold_temp_table (ResourceGroup, recurso, UsageDate, PreTaxCost, Currency)
        VALUES ('{resource_group}', '{resource}', '{current_date}', 0, '-');
        """)

    # Calcular aumento ou diminuição por recurso e grupo de recursos
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {nome_tabela} AS
    SELECT
        ResourceGroup,
        recurso,
        UsageDate,
        PreTaxCost,
        PreTaxCost - LAG(PreTaxCost) OVER (PARTITION BY ResourceGroup, recurso ORDER BY UsageDate) AS change
    FROM gold_temp_table
    ORDER BY ResourceGroup, recurso, UsageDate;
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
    con.execute(f"COPY {nome_tabela} TO '{gold_file_path}' (FORMAT PARQUET);")
    print(f"Tabela '{nome_tabela}' salva com sucesso em '{gold_file_path}' no bucket '{bucket_name}'.")