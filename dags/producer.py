from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from src.inbound.produtor import processar_dados


default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
}

dag = DAG(
    'kafka-producer',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
)

# Define uma função Python para o primeiro passo
def executar_processar_dados():
    processar_dados()

# Task para processar dados do MongoDB
task1 = PythonOperator(
    task_id='producer',
    python_callable=executar_processar_dados,
    dag=dag,
)

# Define a dependência entre as tasks
task1