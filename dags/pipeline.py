from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from src.bronze.processar_dados_bruto import processar_dados
from src.silver.transform_data import normalizar_dados
from src.gold.generate_insights import calcular_dados_futuro
from src.database.create_database import create_database
from src.database.create_table_resource import criando_tabela_resource
from src.database.create_table_cost import criando_tabela_cost
from src.database.load_data_in_table_resource import load_data_in_resource
from src.database.load_data_in_table_cost import load_data_in_cost
from src.metrics.resourcegroups_total import creatview_resourcegroup_totals
from src.metrics.resourcename_total import creatview_resourcename_totals
from src.metrics.resource_information import creatview_resource_information
from src.metrics.cost_by_date import creatview_cost_by_date
from src.clean_bucket.clean_inbound import clean_inbound_bucket

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
}

dag = DAG(
    'azure-cost-pipeline',
    default_args=default_args,
    schedule_interval='*/60 * * * *',
    catchup=False,
)

# Verificar se existe arquivos no bucket do minio
wait_for_file = S3KeySensor(
    task_id='wait_for_new_file',
    bucket_key=f'inbound/*.json',
    bucket_name='azurecost',
    aws_conn_id='s3_minio',
    poke_interval=5,
    timeout=15,
    wildcard_match=True,
    dag=dag,
)



# Task para processar dados bruto do diretório inbound
task1 = PythonOperator(
    task_id='processar_dados_bruto',
    python_callable=processar_dados,
    dag=dag,
)

# Task para limpar e normalizar os dados
task2 = PythonOperator(
    task_id='normalizar_dados_silver',
    python_callable=normalizar_dados,
    dag=dag,
)

# Task para incluir metricas de machine learning
task3 = PythonOperator(
    task_id='transformar_dados_gold',
    python_callable=calcular_dados_futuro,
    dag=dag,
)

# Task para criar database
task4 = PythonOperator(
    task_id='criar_database',
    python_callable=create_database,
    dag=dag,
)

# Task para criar tabela resource
task5 = PythonOperator(
    task_id='criando_tabela_resource',
    python_callable=criando_tabela_resource,
    dag=dag,
)

# Task para criar tabela cost
task6 = PythonOperator(
    task_id='criando_tabela_cost',
    python_callable=criando_tabela_cost,
    dag=dag,
)

# Task para criar tabela cost
task7 = PythonOperator(
    task_id='carregar_dados_tabela_resource',
    python_callable=load_data_in_resource,
    dag=dag,
)

# Task para criar tabela cost
task8 = PythonOperator(
    task_id='carregar_dados_tabela_cost',
    python_callable=load_data_in_cost,
    dag=dag,
)

dummy = EmptyOperator(
    task_id='dummy',
    dag=dag,
)

# Task para criar view resourcegroup_totals
task9 = PythonOperator(
    task_id='view_resourcegroup_totals',
    python_callable=creatview_resourcegroup_totals,
    dag=dag,
)

# Task para criar view resource_name_totals
task10 = PythonOperator(
    task_id='view_resourcename_totals',
    python_callable=creatview_resourcename_totals,
    dag=dag,
)

# Task para criar view resource_information
task11 = PythonOperator(
    task_id='view_resource_information',
    python_callable=creatview_resource_information,
    dag=dag,
)

# Task para criar view cost_by_date
task12 = PythonOperator(
    task_id='view_cost_by_date',
    python_callable=creatview_cost_by_date,
    dag=dag,
)

# Task para criar limpar bucket
task13 = PythonOperator(
    task_id='clean_inbound_bucket',
    python_callable=clean_inbound_bucket,
    dag=dag,
)

# Define a dependência entre as tasks
wait_for_file >> task1 >> task2 >> task3 >> task4

task4 >> [task5, task6]

task5 >> task7
task6 >> task8

task7 >> dummy
task8 >> dummy

dummy >> [task9, task10, task11, task12]

[task9, task10, task11, task12] >> task13