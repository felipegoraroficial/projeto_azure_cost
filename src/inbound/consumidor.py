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

    def listar_valores_collection():

        dados_colection = []

        documentos = collection.find()
        for documento in documentos:
            linha = {
                "_id": documento.get("_id"),
                "PreTaxCost": documento.get("PreTaxCost"),
                "UsageDate": documento.get("UsageDate"),
                "ResourceGroup": documento.get("ResourceGroup"),
                "ResourceId": documento.get("ResourceId"),
                "Currency": documento.get("Currency")
            }
            dados_colection.append(linha)

        return dados_colection
    valors_collection = listar_valores_collection()



    print("Aguardando a última mensagem do Kafka...")
    for message in consumer:
        # Obtém apenas a mensagem mais recente que chega
        data = message.value
        print("Última mensagem recebida:", data)

        # Verifica se os valores de data já existem em valors_collection
        def existe_nos_valores(dado, valores):
            for valor in valores:
                if (
                    dado.get("PreTaxCost") == valor.get("PreTaxCost") and
                    dado.get("UsageDate") == valor.get("UsageDate") and
                    dado.get("ResourceGroup") == valor.get("ResourceGroup") and
                    dado.get("ResourceId") == valor.get("ResourceId") and
                    dado.get("Currency") == valor.get("Currency")
                ):
                    return True
            return False

        if isinstance(data, list):
            novos_dados = [d for d in data if not existe_nos_valores(d, valors_collection)]
        else:
            novos_dados = [data] if not existe_nos_valores(data, valors_collection) else []

        if not novos_dados:
            print("Nenhum novo dado para inserir. Todos já existem na collection.")
            continue

        try:
            if isinstance(novos_dados, list):  # Se o JSON for uma lista de objetos
                collection.insert_many(novos_dados)
            else:  # Se o JSON for um único objeto
                collection.insert_one(novos_dados)
            print(f"Dados salvos na collection {collection} do MongoDB:", novos_dados)
        except Exception as e:
            print("Erro ao salvar no MongoDB:", e)