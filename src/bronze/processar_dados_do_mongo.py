from pymongo import MongoClient
import duckdb
from tabulate import tabulate
import os

def processar_dados():

    mongo_user = os.getenv('MONGO_INITDB_ROOT_USERNAME')
    mongo_password = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
    mongo_host = "mongo:27017"

    # Conexão ao MongoDB
    client = MongoClient(f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}/")
    db = client["azurecost"]

    # Variável para armazenar todas as linhas
    dados_mongo = []

    # Listando todas as coleções no banco de dados
    colecoes = db.list_collection_names()

    # Iterar sobre as coleções e processar os dados
    for colecao_nome in colecoes:
        colecao = db[colecao_nome]
        documentos = colecao.find()
        for documento in documentos:
            linha = {
                "_id": documento.get("_id"),
                "PreTaxCost": documento.get("PreTaxCost"),
                "UsageDate": documento.get("UsageDate"),
                "ResourceGroup": documento.get("ResourceGroup"),
                "ResourceId": documento.get("ResourceId"),
                "Currency": documento.get("Currency")
            }
            dados_mongo.append(linha)

    # Conectar ao DuckDB diretamente a memoria RAM
    con = duckdb.connect('azurecost.db')

    # Criar uma tabela temporária e inserir os dados manualmente
    con.execute("""
    CREATE TABLE IF NOT EXISTS dados_mongo (
        PreTaxCost DOUBLE,
        UsageDate INT,
        ResourceGroup VARCHAR,
        ResourceId VARCHAR,
        Currency VARCHAR
    )
    """)

    # Inserir os dados da lista dados_mongo na tabela
    for linha in dados_mongo:
        con.execute(
            "INSERT INTO dados_mongo VALUES (?, ?, ?, ?, ?)",
            (linha["PreTaxCost"], linha["UsageDate"], linha["ResourceGroup"], linha["ResourceId"], linha["Currency"])
        )

    # Executar consultas diretamente na tabela
    result = con.execute("SELECT * FROM dados_mongo LIMIT 10").fetchall()

    # Obter os nomes das colunas
    columns = [desc[0] for desc in con.execute("DESCRIBE dados_mongo").fetchall()]

    # Exibir os resultados em formato tabular usando tabulate
    print(tabulate(result, headers=columns, tablefmt="psql"))

    return result
