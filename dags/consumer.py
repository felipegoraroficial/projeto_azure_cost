from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from src.inbound.consumidor import carregar_dados


default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
}

dag = DAG(
    'kafka-consumer',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
)

# Task para processar dados do MongoDB
task1 = PythonOperator(
    task_id='producer',
    python_callable=carregar_dados,
    dag=dag,
)

# Define a dependência entre as tasks
task1