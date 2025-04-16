import psycopg2
import duckdb
import os
from dotenv import load_dotenv
import pandas as pd

def carregando_dados_na_tabela():

    # Carregar variáveis de ambiente do arquivo .env
    env_path = os.path.join(os.getcwd(), '.env')
    load_dotenv(dotenv_path=env_path)

    # --- Configurações do Minio ---
    minio_endpoint = 'minio:9000'
    minio_access_key = os.getenv('KEY_ACCESS')
    minio_secret_key = os.getenv('KEY_SECRETS')
    bucket_name = 'azurecost'
    gold_file = 'gold/dados.parquet'
    gold_file_path = f"s3://{bucket_name}/{gold_file}"

    # --- Conectar ao DuckDB e carregar dados ---
    con_duckdb = duckdb.connect(':memory:')
    con_duckdb.execute("INSTALL httpfs;")
    con_duckdb.execute("LOAD httpfs;")
    print("Extensão httpfs instalada e carregada.")

    con_duckdb.execute(f"""
        SET s3_endpoint='{minio_endpoint}';
        SET s3_access_key_id='{minio_access_key}';
        SET s3_secret_access_key='{minio_secret_key}';
        SET s3_use_ssl=False;
        SET s3_url_style='path';
    """)
    print("Credenciais do Minio configuradas.")

    query = f"SELECT * FROM read_parquet('{gold_file_path}');"
    df = con_duckdb.execute(query).fetchdf()
    print("Dados carregados do Parquet para DataFrame.")

    # --- Configurações de conexão com o PostgreSQL ---
    DB_HOST = "airflow-postgres"
    DB_PORT = "5432"
    DB_USER = os.getenv('POSTGRES_USER')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    DB_NAME = "azurecost"
    TABLE_NAME = "azure_cost_data"

    conn_pg = None
    try:
        # --- Conectar ao PostgreSQL ---
        conn_pg = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur_pg = conn_pg.cursor()

        # --- Overwrite da tabela (truncar e inserir) ---
        truncate_query = f"TRUNCATE TABLE {TABLE_NAME};"
        cur_pg.execute(truncate_query)
        print(f"Tabela '{TABLE_NAME}' truncada.")
        conn_pg.commit()

        # --- Inserir dados do DataFrame na tabela PostgreSQL ---
        for index, row in df.iterrows():
            insert_query = """
            INSERT INTO azure_cost_data (ResourceGroup, recurso, UsageDate, PreTaxCost, change)
            VALUES (%s, %s, %s, %s, %s);
            """
            data_to_insert = (
                row['ResourceGroup'],
                row['recurso'],
                row['UsageDate'].to_pydatetime().date(),
                row['PreTaxCost'],
                row['change']
            )
            cur_pg.execute(insert_query, data_to_insert)

        conn_pg.commit()
        print(f"{len(df)} linhas carregadas com sucesso (overwrite) na tabela '{TABLE_NAME}' do PostgreSQL.")

        # --- Fechar cursor do PostgreSQL ---
        cur_pg.close()

    except psycopg2.Error as e:
        print(f"Erro ao conectar ou carregar os dados no PostgreSQL: {e}")
    finally:
        # --- Fechar conexão com o PostgreSQL ---
        if conn_pg:
            conn_pg.close()

    # --- Fechar conexão com DuckDB ---
    con_duckdb.close()

    print("Processo completo.")