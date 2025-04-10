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
    todas_as_linhas = []

    # Listando todas as coleções no banco de dados
    colecoes = db.list_collection_names()

    # Iterar sobre as coleções e processar os dados
    for colecao_nome in colecoes:
        colecao = db[colecao_nome]
        documentos = colecao.find()
        for documento in documentos:
            # Extrair colunas e linhas da propriedade 'properties'
            colunas = [col["name"] for col in documento.get("properties", {}).get("columns", [])]
            linhas = documento.get("properties", {}).get("rows", [])
            for linha in linhas:
                # Criar um dicionário para cada linha com base nas colunas
                todas_as_linhas.append(dict(zip(colunas, linha)))

    # Conectar ao DuckDB
    con = duckdb.connect("azurecost.db")

    # Criar uma tabela temporária e inserir os dados manualmente
    con.execute("""
    CREATE TABLE IF NOT EXISTS todas_as_linhas (
        PreTaxCost DOUBLE,
        UsageDate INT,
        ResourceGroup VARCHAR,
        ResourceId VARCHAR,
        Currency VARCHAR
    )
    """)

    # Inserir os dados da lista todas_as_linhas na tabela
    for linha in todas_as_linhas:
        con.execute(
            "INSERT INTO todas_as_linhas VALUES (?, ?, ?, ?, ?)",
            (linha["PreTaxCost"], linha["UsageDate"], linha["ResourceGroup"], linha["ResourceId"], linha["Currency"])
        )

    # Executar consultas diretamente na tabela
    result = con.execute("SELECT * FROM todas_as_linhas LIMIT 10").fetchall()

    # Obter os nomes das colunas
    columns = [desc[0] for desc in con.execute("DESCRIBE todas_as_linhas").fetchall()]

    # Exibir os resultados em formato tabular usando tabulate
    print(tabulate(result, headers=columns, tablefmt="psql"))

    return result
