from kafka import KafkaConsumer
import json
import os
from minio import Minio
from io import BytesIO


def carregar_dados(batch_size=100):
    # Configuração do consumidor Kafka para ouvir o tópico 'api-topic'
    consumer = KafkaConsumer(
        'api-topic',
        bootstrap_servers=['kafka1:19091'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        consumer_timeout_ms=5000  # Timeout para encerrar a iteração se não houver mensagens
    )

    client = Minio(
        "minio:9000",
        access_key=os.getenv('MINIO_ROOT_USER'),
        secret_key=os.getenv('MINIO_ROOT_PASSWORD'),
        secure=False
    )

    bucket_name = "azurecost"

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    print("Consumindo mensagens do Kafka (batch)...")

    count = 0
    # O consumer_timeout_ms permite que o for termine se não há msgs novas após 5s
    try:
        for message in consumer:
            data = message.value
            print("Mensagem recebida:", data)

            usage_date = data['UsageDate'].replace("-", "_").replace("T", "_").replace(":", "_")
            resource = data['ResourceId'].rsplit('/', 1)[-1]

            json_bytes = json.dumps(data).encode('utf-8')
            json_stream = BytesIO(json_bytes)

            object_name = f"inbound/{resource}_{usage_date}.json"

            client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=json_stream,
                length=len(json_bytes),
                content_type="application/json"
            )

            print(f"{object_name} enviado com sucesso para o bucket {bucket_name}")

            count += 1
            if count >= batch_size:
                print(f"Processados {count} mensagens - finalizando batch.")
                break
    finally:
        consumer.close()

    print(f"Batch de consumo finalizado com {count} mensagens processadas.")
