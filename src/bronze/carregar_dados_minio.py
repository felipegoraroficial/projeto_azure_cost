import duckdb
import boto3
import os
from io import BytesIO

def carregar_dados():

    # Conectar ao DuckDB
    con = duckdb.connect("azurecost.db")

    # Exportar os dados para Parquet
    parquet_buffer = BytesIO()
    con.execute("COPY todas_as_linhas TO 'dados.parquet' (FORMAT 'parquet')")

    # Ler o arquivo Parquet gerado
    with open('dados.parquet', 'rb') as parquet_file:
        parquet_buffer.write(parquet_file.read())

    parquet_buffer.seek(0)

    # Configurar o cliente S3 (MinIO)
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',  # URL do MinIO
        aws_access_key_id=os.getenv('KEY_ACCESS'),        # Substitua pela sua chave de acesso
        aws_secret_access_key=os.getenv('KEY_SECRETS')    # Substitua pela sua chave secreta
    )

    # Nome do bucket e arquivo no MinIO
    bucket_name = 'azurecost'
    file_name = 'bronze/dados.parquet'

    # Fazer o upload do arquivo Parquet para o bucket MinIO
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=parquet_buffer.getvalue(),
        ContentType='application/octet-stream'
    )

    os.remove('dados.parquet')

    return print(f'Arquivo {file_name} salvo com sucesso no bucket {bucket_name}!')