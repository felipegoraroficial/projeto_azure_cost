import psycopg2
import os

def create_database():

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

    except psycopg2.Error as e:
        print(f"Erro ao conectar ou criar o banco de dados/tabela: {e}")
    finally:
        if conn:
            conn.close()