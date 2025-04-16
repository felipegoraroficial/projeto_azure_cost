from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from src.bronze.processar_dados_do_mongo import processar_dados
from src.bronze.carregar_dados_minio import carregar_dados
from src.silver.duckdb_transform_data import normalizar_dados
from src.gold.generate_insights import transformar_dados
from src.IA.generate_ia import gerar_insights
from src.database.create_table import criando_database_e_tabela
from src.database.load_data_on_table import carregando_dados_na_tabela



default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
}

dag = DAG(
    'azure-cost-pipeline',
    default_args=default_args,
    schedule_interval='0 */4 * * *',
    catchup=False,
)

# Define uma função Python para o primeiro passo
def executar_processar_dados():
    processar_dados()

# Define uma função Python para o segundo passo
def executar_carregar_dados():
    carregar_dados()

# Define uma função Python para o terceiro passo
def executar_normalizar_dados():
    normalizar_dados()

# Define uma função Python para o quarto passo
def executar_transformar_dados():
    transformar_dados()

# Define uma função Python para o quarto passo
def executar_gerar_dados_IA():
    gerar_insights()

# Define uma função Python para o quinto passo
def criando_tabelas():
    criando_database_e_tabela()

# Define uma função Python para o quinto passo
def carregando_tabelas():
    carregando_dados_na_tabela()

# Task para processar dados do MongoDB
task1 = PythonOperator(
    task_id='processar_dados_mongodb',
    python_callable=executar_processar_dados,
    dag=dag,
)

# Task para carregar dados no MinIO
task2 = PythonOperator(
    task_id='carregar_dados_bronze',
    python_callable=executar_carregar_dados,
    dag=dag,
)

# Task para normalizar dados com duckDB
task3 = PythonOperator(
    task_id='normalizar_dados_silver',
    python_callable=executar_normalizar_dados,
    dag=dag,
)

# Task para transformar dados com duckDB
task4 = PythonOperator(
    task_id='transformar_dados_gold',
    python_callable=executar_transformar_dados,
    dag=dag,
)

# Task para gerar insights com IA
task5 = PythonOperator(
    task_id='gerar_dados_IA',
    python_callable=gerar_insights,
    dag=dag,
)

# Task para criar database e tabelas no postgres
task6 = PythonOperator(
    task_id='criando_tabela_postgres',
    python_callable=criando_database_e_tabela,
    dag=dag,
)

# Task para carregar dados na tabela do postgres
task7 = PythonOperator(
    task_id='carregando_tabela_postgres',
    python_callable=carregando_dados_na_tabela,
    dag=dag,
)

# Define a dependência entre as tasks
task1 >> task2 >> task3 >> task4 >> [task5, task6] >> task7