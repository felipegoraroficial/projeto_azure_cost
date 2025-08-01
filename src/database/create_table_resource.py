import psycopg2
import os

def criando_tabela_resource():

    # Informações de conexão com o PostgreSQL
    DB_HOST = "airflow-postgres"
    DB_PORT = "5432"
    DB_USER = os.getenv('POSTGRES_USER')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    DB_NAME = "azurecost"

    conn = None
    try:

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

        # Comando SQL para criar a tabela 'resources'
        create_table_query = """
        CREATE TABLE IF NOT EXISTS resources (
            Id SERIAL PRIMARY KEY,
            ResourceName VARCHAR,
            SubscriptionId VARCHAR,
            ResourceGroup VARCHAR,
            Provider VARCHAR,
            StatusRecourse VARCHAR,
            Currency VARCHAR,
            TendenciaCusto VARCHAR
        );
        """

        cur.execute(create_table_query)
        conn.commit()
        print(f"Tabela 'resources' criada com sucesso no banco de dados '{DB_NAME}'.")

        cur.close()

    except psycopg2.Error as e:
        print(f"Erro ao conectar ou criar o banco de dados/tabela: {e}")
    finally:
        if conn:
            conn.close()