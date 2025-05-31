from processar_dados_bruto import processar_dados


endpoint_mongo = "localhost:27017"
endpoint_minio = 'localhost:9000'

processar_dados(endpoint_mongo,endpoint_minio)