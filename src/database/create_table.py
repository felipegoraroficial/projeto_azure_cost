import psycopg2
import os

def criando_database_e_tabela():

    # Informações de conexão com o PostgreSQL
    DB_HOST = "airflow-postgres"
    DB_PORT = "5432"
    DB_USER = os.getenv('POSTGRES_USER')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    DB_NAME = "azurecost"

    conn = None
    try:
        # Conecta-se ao banco de dados 'postgres' para criar o banco 'azurecost'
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database="postgres",
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Verifica e cria o banco de dados 'azurecost' se não existir
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{DB_NAME}'")
        exists = cur.fetchone()
        if not exists:
            cur.execute(f"CREATE DATABASE {DB_NAME}")
            print(f"Banco de dados '{DB_NAME}' criado com sucesso.")
        else:
            print(f"Banco de dados '{DB_NAME}' já existe.")

        cur.close()
        conn.close()  # Fecha a conexão com 'postgres'

        # Conecta-se ao banco de dados 'azurecost' para criar a tabela
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Comando SQL para criar a tabela 'azure_cost_data'
        create_table_query = """
        CREATE TABLE IF NOT EXISTS azure_cost_data (
            ResourceGroup VARCHAR,
            recurso VARCHAR,
            UsageDate DATE,
            PreTaxCost DOUBLE PRECISION,
            change DOUBLE PRECISION
        );
        """

        cur.execute(create_table_query)
        conn.commit()
        print(f"Tabela 'azure_cost_data' criada com sucesso no banco de dados '{DB_NAME}'.")

        cur.close()

    except psycopg2.Error as e:
        print(f"Erro ao conectar ou criar o banco de dados/tabela: {e}")
    finally:
        if conn:
            conn.close()