import time
import requests
import json
from dotenv import load_dotenv
import os
from kafka import KafkaProducer
from datetime import datetime

# Caminho relativo
env_path = os.path.join(os.getcwd(), '.env')
load_dotenv(dotenv_path=env_path)

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
tenant_id = os.getenv('TENANT_ID')
subscription_id = os.getenv('SUBSCRIPTION_ID')

while True:

    # Obtém a data atual no formato 'YYYY-MM-DD'
    data_atual = datetime.now().strftime('%Y-%m-%d')

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

    # Corpo da consulta para custos por Resource Group e Recurso PARA O DIA ATUAL
    query_body = {
        "type": "Usage",
        "timeframe": "Custom",
        "timePeriod": {
            "from": f"{data_atual}T00:00:00Z",
            "to": f"{data_atual}T23:59:59Z"
        },
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

    try:
        # Obtendo o token de autenticação
        response = requests.post(auth_url, data=auth_data)
        response.raise_for_status()
        token = response.json()["access_token"]
        headers["Authorization"] = f"Bearer {token}"

        response = requests.post(costs_url, headers=headers, json=query_body)
        response.raise_for_status()
        cost_data = response.json()

        print("Dados de custo extraídos com sucesso.")

        # Inicializa o produtor Kafka
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9091'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Formata os dados para enviar ao Kafka
        if cost_data and "properties" in cost_data and "rows" in cost_data["properties"] and "columns" in cost_data["properties"]:
            columns = [col["name"] for col in cost_data["properties"]["columns"]]
            documents_to_send = []
            for row in cost_data["properties"]["rows"]:
                document = dict(zip(columns, row))
                documents_to_send.append(document)

            # Envia cada documento para o tópico Kafka
            for document in documents_to_send:
                try:
                    producer.send('api-topic', value=document)
                    print(f"Mensagem enviada para o tópico Kafka: {document['ResourceGroup']} - {document['ResourceId']}")
                except Exception as e:
                    print(f"Erro ao enviar mensagem para o Kafka: {e}")
                producer.flush()

            print("-----------------")
            print(f"{len(documents_to_send)} documentos enviados para o tópico Kafka '{'api-topic'}'.")

        else:
            print("Nenhum dado de custo encontrado para enviar ao Kafka.")

    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição HTTP: {e}")
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON: {e}")
    except KeyError as e:
        print(f"Erro de chave no JSON de resposta: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
    finally:
        if 'producer' in locals():
            producer.close()

    time.sleep(60)  # Envia a cada 60 segundos

