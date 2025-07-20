from kafka import KafkaConsumer
import json
import os
from minio import Minio
from io import BytesIO


def carregar_dados():

    # Configuração do consumidor Kafka para ouvir o tópico 'api-topic'
    consumer = KafkaConsumer(
        'api-topic',  # Nome do tópico Kafka
        bootstrap_servers=['kafka1:19091'],  # Endereço do servidor Kafka
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),  # Deserializa as mensagens recebidas de JSON
        auto_offset_reset='latest',  # Começa a consumir a partir da última mensagem disponível
        enable_auto_commit=True  # Confirma automaticamente o offset das mensagens lidas
    )

    client = Minio(
        "minio:9000",
        access_key=os.getenv('MINIO_ROOT_USER'),
        secret_key=os.getenv('MINIO_ROOT_PASSWORD'),
        secure=False
    )


    bucket_name = "azurecost"

    # Crie o bucket se não existir
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    print("Aguardando mensagens do Kafka...")
    # Loop principal: consome mensagens do tópico Kafka indefinidamente
    for message in consumer:
        data = message.value # Obtém o conteúdo da mensagem
        print("Mensagem recebida:", data)
        usage_date = data['UsageDate']
        usage_date = usage_date.replace("-", "_").replace("T", "_").replace(":", "_")
        resource = data['ResourceId']
        resource = resource.rsplit('/', 1)[-1]

        # Converter JSON para bytes
        json_bytes = json.dumps(data).encode('utf-8')
        json_stream = BytesIO(json_bytes)

        object_name = f"inbound/{resource}_{usage_date}.json"

        # Enviar diretamente da memória
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=json_stream,
            length=len(json_bytes),
            content_type="application/json"
        )

        print(f"{object_name} enviado com sucesso para o bucket {bucket_name}")

