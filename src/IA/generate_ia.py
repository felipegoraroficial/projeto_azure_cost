import duckdb
import os
from dotenv import load_dotenv
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain_openai import ChatOpenAI
import pandas as pd

def gerar_insights():

    # Caminho relativo
    env_path = os.path.join(os.getcwd(), '.env')
    load_dotenv(dotenv_path=env_path)

    ai_key = os.getenv('OPENAI_API_KEY')

    #  Configurações do Minio
    minio_endpoint = 'minio:9000'
    minio_access_key = os.getenv('MINIO_ROOT_USER')
    minio_secret_key = os.getenv('MINIO_ROOT_PASSWORD')
    bucket_name = 'azurecost'
    gold_file = 'gold/dados.parquet'
    gold_file_path = f"s3://{bucket_name}/{gold_file}"

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

    # Consultar e exibir os resultados
    query = f"SELECT * FROM read_parquet('{gold_file_path}');"
    df = con.execute(query).fetchdf()

    if len(df) > 0:

        print("A tabela tem dados! Continuando o fluxo...")

        # Criar um agente LangChain para interagir com o DataFrame
        llm = ChatOpenAI(temperature=0.7, model="gpt-4o-mini", openai_api_key=ai_key)
        agent = create_pandas_dataframe_agent(llm, df, verbose=True, allow_dangerous_code=True)

        # Fazer perguntas ao agente LangChain sobre os dados
        question = "Me informe dicas de como economizar com custos clouds referente aos dados do dataframe"
        try:
            answer = agent.invoke(question)
            print("Insights de IA:")
            print(answer)
        except Exception as e:
            if "RateLimitError" in str(e):
                print("Rate Limit Error, wait a few seconds and try again")
            else:
                print(f"An error occurred: {e}")

    else:
        print("Sem dados para gerar insights") 