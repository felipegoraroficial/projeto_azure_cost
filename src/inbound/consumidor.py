from kafka import KafkaConsumer
from pymongo import MongoClient
import json
from datetime import datetime
import os

def carregar_dados():

    # Configuração do consumidor Kafka
    consumer = KafkaConsumer(
        'api-topic',
        bootstrap_servers=['kafka1:19091'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest', 
        enable_auto_commit=True
    )

    mongo_user = os.getenv('MONGO_INITDB_ROOT_USERNAME')
    mongo_password = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
    mongo_host = "mongo:27017"

    # Conexão ao MongoDB
    client = MongoClient(f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}/")

    # Garante que o database azurecost será criado se não existir
    db_name = "azurecost"
    if db_name not in client.list_database_names():
        print(f"Database {db_name} não encontrado. Será criado automaticamente ao inserir dados.")
    db = client[db_name]

    # Obtém a data atual
    data_atual = datetime.now().strftime('%Y-%m-%d')

    # Definindo a coleção no MongoDB
    collection = db[data_atual]

    print("Aguardando a última mensagem do Kafka...")
    for message in consumer:
        # Obtém apenas a mensagem mais recente que chega
        data = message.value
        print("Última mensagem recebida:", data)

        try:
            if isinstance(data, list):  # Se o JSON for uma lista de objetos
                collection.insert_many(data)
            else:  # Se o JSON for um único objeto
                collection.insert_one(data)
            print(f"Dados salvos na collection {collection} do MongoDB:", data)
        except Exception as e:
            print("Erro ao salvar no MongoDB:", e)