import time
import requests
import json
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

# Caminho relativo
env_path = os.path.join(os.getcwd(), '.env')
load_dotenv(dotenv_path=env_path)

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
tenant_id = os.getenv('TENANT_ID')
subscription_id = os.getenv('SUBSCRIPTION_ID')

# Configuração do Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9091'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# URL para obter o token de autenticação
auth_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
auth_data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "resource": "https://management.azure.com/"
}

# URL para consultar os custos
costs_url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.CostManagement/query?api-version=2021-10-01"
headers = {"Content-Type": "application/json"}

# Corpo da consulta para custos por Resource Group e Recurso
query_body = {
    "type": "Usage",
    "timeframe": "MonthToDate",
    "dataset": {
        "granularity": "Daily",
        "grouping": [
            {"type": "Dimension", "name": "ResourceGroup"},
            {"type": "Dimension", "name": "ResourceId"}
        ],
        "aggregation": {
            "totalCost": {
                "name": "PreTaxCost",
                "function": "Sum"
            }
        }
    }
}

# Obtendo o token de autenticação
response = requests.post(auth_url, data=auth_data)
response.raise_for_status()
token = response.json()["access_token"]
headers["Authorization"] = f"Bearer {token}"

# Loop para consulta e envio de dados periódicos
print("Iniciando envio periódico de mensagens para o Kafka...")
while True:
    try:
        # Consultar a API
        response = requests.post(costs_url, headers=headers, json=query_body)
        response.raise_for_status()
        cost_data = response.json()
        print("Dados recuperados da API:", cost_data)

        # Enviar dados para o Kafka
        producer.send('api-topic', value=cost_data)
        producer.flush()
        print("Dados da API enviados com sucesso!")
    except Exception as e:
        print("Erro ao consultar a API ou enviar os dados:", e)

    time.sleep(60)  # Envia a cada 60 segundos
