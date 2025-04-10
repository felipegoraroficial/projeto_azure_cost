import boto3
import duckdb
import os
from dotenv import load_dotenv
from io import BytesIO
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain_openai import ChatOpenAI
import pandas as pd

def gerar_insights():

    # Caminho relativo
    env_path = os.path.join(os.getcwd(), '.env')
    load_dotenv(dotenv_path=env_path)

    ai_key = os.getenv('OPENAI_API_KEY')

    # Configuração do cliente S3 (MinIO)
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id=os.getenv('KEY_ACCESS'),
        aws_secret_access_key=os.getenv('KEY_SECRETS')
    )

    # Nome do bucket e arquivo Parquet
    bucket_name = 'azurecost'
    file_name = 'gold/insights_dados.parquet'

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

    # Consultar e exibir os resultados
    query = "SELECT * FROM temp_table" 
    table = con.execute(query).fetchdf()

    # Verifique se a tabela contém mais de 0 linhas
    if len(table) > 0:
    
        print("A tabela tem dados! Continuando o fluxo...")

        # Criar um agente LangChain para interagir com o DataFrame
        llm = ChatOpenAI(temperature=0.7, model="gpt-4o-mini", openai_api_key=ai_key)
        agent = create_pandas_dataframe_agent(llm, table, verbose=True, allow_dangerous_code=True)

        # Fazer perguntas ao agente LangChain sobre os dados
        question = "Você pode me ajudar a identificar quais são os recursos que tiveram mais custo com a data mais recente que não seja igual a zero?"
        try:
            answer = agent.invoke(question) # alterado agent.run para agent.invoke
            print("Insights de IA:")
            print(answer)
        except Exception as e:
            if "RateLimitError" in str(e):
                print("Rate Limit Error, wait a few seconds and try again")
            else:
                print(f"An error occurred: {e}")

    else:
        print("Sem dados para gerar insights")          

    # Remover arquivos temporários locais
    os.remove(temp_file_path)
