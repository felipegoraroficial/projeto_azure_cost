from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from src.bronze.processar_dados_bruto import processar_dados
from src.silver.transform_data import normalizar_dados
from src.gold.generate_insights import calcular_dados_futuro
#from src.IA.generate_ia import gerar_insights
#from src.database.create_table import criando_database_e_tabela
#from src.database.load_data_on_table import carregando_dados_na_tabela



default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
}

dag = DAG(
    'azure-cost-pipeline',
    default_args=default_args,
    schedule_interval='*/30 * * * *',
    catchup=False,
)


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



# Task para processar dados do MongoDB
task1 = PythonOperator(
    task_id='processar_dados_bruto',
    python_callable=processar_dados,
    dag=dag,
)

# Task para normalizar dados com duckDB
task2 = PythonOperator(
    task_id='normalizar_dados_silver',
    python_callable=normalizar_dados,
    dag=dag,
)

# Task para transformar dados com duckDB
task3 = PythonOperator(
    task_id='transformar_dados_gold',
    python_callable=calcular_dados_futuro,
    dag=dag,
)

# Task para gerar insights com IA
#task4 = PythonOperator(
#    task_id='gerar_dados_IA',
#    python_callable=gerar_insights,
#    dag=dag,
#)

# Task para criar database e tabelas no postgres
#task5 = PythonOperator(
#    task_id='criando_tabela_postgres',
#    python_callable=criando_database_e_tabela,
#    dag=dag,
#)

# Task para carregar dados na tabela do postgres
#task6 = PythonOperator(
#    task_id='carregando_tabela_postgres',
#    python_callable=carregando_dados_na_tabela,
#    dag=dag,
#)

# Define a dependência entre as tasks
wait_for_file >> task1 >> task2 >> task3
#task1 >> task2 >> task3 >> [task4,task5] >> task6