import requests
import json
from dotenv import load_dotenv
import os
from kafka import KafkaProducer
from datetime import datetime, timezone, timedelta

def processar_dados():

    # Carrega as variáveis de ambiente do arquivo .env localizado no diretório atual
    env_path = os.path.join(os.getcwd(), '.env')
    load_dotenv(dotenv_path=env_path)

    # Recupera as credenciais e informações necessárias do ambiente
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    tenant_id = os.getenv('TENANT_ID')
    subscription_id = os.getenv('SUBSCRIPTION_ID')

    # Obtém o horário atual em UTC
    agora_utc = datetime.now(timezone.utc)

    # Formata a data/hora atual para o padrão ISO sem os segundos (ex: '2025-07-11T18:17')
    data_atual = agora_utc.strftime('%Y-%m-%dT%H:%M')

    # Calcula o horário de dois minutos atrás
    dois_minutos_atras = agora_utc - timedelta(minutes=2)

    # Formata a data/hora de dois minutos atrás
    data_dois_minutos_atras = dois_minutos_atras.strftime('%Y-%m-%dT%H:%M')

    # Monta a URL para obter o token de autenticação do Azure AD
    auth_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    auth_data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "resource": "https://management.azure.com/"
    }

    # Monta a URL para consultar os custos na API do Azure Cost Management
    costs_url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.CostManagement/query?api-version=2021-10-01"
    headers = {"Content-Type": "application/json"}

    # Define o corpo da consulta para buscar os custos agrupados por Resource Group e ResourceId, no intervalo dos últimos 2 minutos
    query_body = {
        "type": "Usage",
        "timeframe": "Custom",
        "timePeriod": {
            "from": f"{data_dois_minutos_atras}:00Z",  # início do intervalo
            "to": f"{data_atual}:59Z"                   # fim do intervalo
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
        # Solicita o token de autenticação ao Azure AD
        response = requests.post(auth_url, data=auth_data)
        response.raise_for_status()  # Lança exceção se houver erro HTTP
        token = response.json()["access_token"]
        headers["Authorization"] = f"Bearer {token}"  # Adiciona o token ao header

        # Faz a requisição para obter os dados de custo do Azure
        response = requests.post(costs_url, headers=headers, json=query_body)
        response.raise_for_status()
        cost_data = response.json()

        print("Dados de custo extraídos com sucesso.")

        # Inicializa o produtor Kafka para enviar os dados ao tópico desejado
        producer = KafkaProducer(
            bootstrap_servers=['kafka1:19091'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Verifica se a resposta possui os dados esperados
        if cost_data and "properties" in cost_data and "rows" in cost_data["properties"] and "columns" in cost_data["properties"]:
            columns = [col["name"] for col in cost_data["properties"]["columns"]]
            idx_data = columns.index('UsageDate')
            documents_to_send = []
            # Para cada linha de dados, cria um dicionário associando coluna e valor
            for row in cost_data["properties"]["rows"]:
                row[idx_data] = data_atual
                document = dict(zip(columns, row))
                documents_to_send.append(document)

            # Envia cada documento individualmente para o tópico Kafka
            for document in documents_to_send:
                try:
                    producer.send('api-topic', value=document)
                    print(f"Mensagem enviada para o tópico Kafka: {document['ResourceGroup']} - {document['ResourceId']}")
                except Exception as e:
                    print(f"Erro ao enviar mensagem para o Kafka: {e}")
                producer.flush()  # Garante que a mensagem foi enviada

            print("-----------------")
            print(f"{len(documents_to_send)} documentos enviados para o tópico Kafka '{'api-topic'}'.")

        else:
            print("Nenhum dado de custo encontrado para enviar ao Kafka.")

    except requests.exceptions.RequestException as e:
        # Captura erros relacionados a requisições HTTP
        print(f"Erro na requisição HTTP: {e}")
    except json.JSONDecodeError as e:
        # Captura erros na decodificação do JSON de resposta
        print(f"Erro ao decodificar JSON: {e}")
    except KeyError as e:
        # Captura erros de chave ausente no JSON de resposta
        print(f"Erro de chave no JSON de resposta: {e}")
    except Exception as e:
        # Captura qualquer outro erro inesperado
        print(f"Ocorreu um erro inesperado: {e}")
    finally:
        # Fecha o produtor Kafka, caso tenha sido criado
        if 'producer' in locals():
            producer.close()
