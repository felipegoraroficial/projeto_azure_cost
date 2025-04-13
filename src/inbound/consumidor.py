from kafka import KafkaConsumer
from pymongo import MongoClient
import json
from datetime import datetime

# Configuração do consumidor Kafka
consumer = KafkaConsumer(
    'api-topic',
    bootstrap_servers=['localhost:9091'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest', 
    enable_auto_commit=True
)

# Configuração do MongoDB
client = MongoClient("mongodb://fececa:fececa13@localhost:27017/")

# Garante que o database azurecost será criado se não existir
db_name = "azurecost"
if db_name not in client.list_database_names():
    print(f"Database {db_name} não encontrado. Será criado automaticamente ao inserir dados.")
db = client[db_name]

# Obtém a data atual
data_atual = datetime.now()
data_formatada = data_atual.strftime('%Y-%m-%d')

# Definindo a coleção no MongoDB
collection = db[data_formatada]

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
        print("Dados salvos no MongoDB:", data)
    except Exception as e:
        print("Erro ao salvar no MongoDB:", e)