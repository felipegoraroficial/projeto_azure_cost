from minio import Minio
import os
from datetime import datetime, timezone, timedelta

def clean_inbound_bucket():

    # Configure o cliente MinIO
    minio_client = Minio(
        "minio:9000",
        access_key=os.getenv("KEY_ACCESS"),
        secret_key=os.getenv("KEY_SECRETS"),
        secure=False,
    )

    bucket_name = "azurecost"
    prefix = "inbound/"

    # Calcula a hora limite (1 hora atrás)
    hora_limite = datetime.now(timezone.utc) - timedelta(hours=1)

    # Lista todos os objetos no prefixo 'inbound/'
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)

    for obj in objects:
        # Filtra por arquivos .json
        if obj.object_name.endswith(".json"):
            # Verifica se o arquivo foi modificado há mais de uma hora
            if obj.last_modified < hora_limite:
                print(f"Deletando: {obj.object_name}")
                minio_client.remove_object(bucket_name, obj.object_name)