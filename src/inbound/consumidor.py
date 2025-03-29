from kafka import KafkaConsumer
from pymongo import MongoClient
import json
from datetime import datetime

# Configuração do consumidor Kafka
consumer = KafkaConsumer(
    'api-topic',
    bootstrap_servers=['localhost:9091'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Configuração do MongoDB
client = MongoClient("mongodb://fececa:fececa13@localhost:27017/")
db = client["azurecost"]

# Obtém a data atual
data_atual = datetime.now()
data_formatada = data_atual.strftime('%Y-%m-%d')

# Consumindo dados do Kafka e salvando no MongoDB
collection = db[data_formatada]  # Nome da coleção com base na data
print("Iniciando consumo de mensagens do Kafka...")
for message in consumer:
    data = message.value
    if "mensagem" in data and data["mensagem"] == "Dado periódico do produtor":
        print("Mensagem genérica ignorada.")
        continue

    try:
        if isinstance(data, list):  # Se o JSON for uma lista de objetos
            collection.insert_many(data)
        else:  # Se o JSON for um único objeto
            collection.insert_one(data)
        print("Dados salvos no MongoDB:", data)
    except Exception as e:
        print("Erro ao salvar no MongoDB:", e)
