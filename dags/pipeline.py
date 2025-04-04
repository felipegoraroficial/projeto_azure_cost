from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess

# Função para executar o consumidor.py
def executar_consumidor():
    subprocess.run(["python3", "projeto_azure_cost/src/inbound/consumidor.py"], check=True)

# Função para executar o produtor.py
def executar_produtor():
    subprocess.run(["python3", "projeto_azure_cost/src/inbound/produtor.py"], check=True)

def processar_dados():
    subprocess.run([r"C:\Users\felip\OneDrive\Documentos\GitHub\projeto_azure_cost\venv\Scripts\python", "src/bronze/mongo-to-minio.py"])

def transformar_dados():
    subprocess.run(["python3", "projeto_azure_cost/src/silver/duckdb-transform-data.py"])

def gerar_insights_dados():
    subprocess.run(["python3", "projeto_azure_cost/src/gold/generate_insights.py"])
    

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 29),
    'retries': 1,
}

with DAG('azurecost_pipeline', default_args=default_args, schedule_interval='@daily') as dag:
    tarefa_1 = PythonOperator(
        task_id='mongo-to-minio',
        python_callable=processar_dados
    )

    tarefa_2 = PythonOperator(
        task_id='transformar-com-duckdb',
        python_callable=transformar_dados
    )

    tarefa_3 = PythonOperator(
        task_id='gerar-insights',
        python_callable=gerar_insights_dados
    )

    tarefa_1 >> tarefa_2 >> tarefa_3