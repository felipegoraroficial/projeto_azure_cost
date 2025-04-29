import streamlit as st
from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd
import plotly.express as px

st.markdown("<h1 style='text-align: center;'>Azure Cost</h1>", unsafe_allow_html=True)

def load_data():

    env_path = os.path.join(os.getcwd(), '.env')
    load_dotenv(dotenv_path=env_path)

    # --- Configurações de conexão com o PostgreSQL ---
    DB_HOST = "airflow-postgres"
    DB_PORT = "5432"
    DB_USER = os.getenv('POSTGRES_USER')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    DB_NAME = "azurecost"
    TABLE_NAME = "azure_cost_data"

    conn_pg = None
    try:
        # --- Conectar ao PostgreSQL ---
        conn_pg = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Conexão com o PostgreSQL estabelecida com sucesso.")

        # --- Ler dados da tabela PostgreSQL para um DataFrame do Pandas ---
        query = f"SELECT * FROM {TABLE_NAME};"
        df = pd.read_sql(query, conn_pg)
        print(f"Dados da tabela '{TABLE_NAME}' carregados para um DataFrame do Pandas.")

        return df

    except Exception as e:
        return st.error(f"Erro ao conectar ao banco de dados com os dados: {e}")
df = load_data()

def load_ia_answer():

    mongo_user = os.getenv('MONGO_INITDB_ROOT_USERNAME')
    mongo_password = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
    mongo_host = "mongo:27017"

    # Conexão ao MongoDB
    client = MongoClient(f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}/")

    # Nome do banco de dados
    db_name = "IAcost"
    db = client[db_name]

    # Listar as coleções no banco de dados
    collection_names = db.list_collection_names()

    # Filtrar apenas as coleções que parecem ser datas no formato YYYY-MM-DD
    date_collections = [name for name in collection_names if len(name) == 10 and all(c.isdigit() or c == '-' for c in name)]

    if not date_collections:
        print(f"Não foram encontradas coleções com formato de data (YYYY-MM-DD) no banco de dados '{db_name}'.")
    else:
        # Converter os nomes das coleções para objetos datetime para comparação
        date_objects = []
        valid_date_collections = {}
        for name in date_collections:
            try:
                date_obj = datetime.strptime(name, "%Y-%m-%d").date()
                date_objects.append(date_obj)
                valid_date_collections[date_obj] = name
            except ValueError:
                return print(f"A coleção '{name}' não está no formato de data esperado e será ignorada.")

        if date_objects:
            # Encontrar a data mais recente
            latest_date = max(date_objects)
            latest_collection_name = valid_date_collections[latest_date]

            # Obter a coleção mais recente
            latest_collection = db[latest_collection_name]

            # Buscar o último documento inserido na coleção mais recente
            latest_document = latest_collection.find().sort([('_id', -1)]).limit(1).next()

            # Obter o valor do campo 'output'
            output_value = latest_document.get('output')

            if output_value:
                return output_value ,latest_date

            else:
                return print(f"O campo 'output' não foi encontrado no último documento da coleção '{latest_collection_name}'.")
        else:
            return print(f"Não foi possível identificar nenhuma coleção válida com formato de data no banco de dados '{db_name}'.")

    # Fechar a conexão com o MongoDB
    client.close()
ia_answer = load_ia_answer()

row_count = len(df)
st.markdown("<h2 style='text-align: center; color: green;'>Conectado ao banco de dados com sucesso.</h2>", unsafe_allow_html=True)
st.markdown(f"<h2 style='text-align: center; color: green;'>Número de linhas na tabela: {row_count}</h2>", unsafe_allow_html=True)

st.markdown(f"<p style='text-align: center; font-size:20px; font-weight: bold; color:blue;'>Última mensagem da IA foi: {ia_answer[1]}</p>", unsafe_allow_html=True)

st.markdown(f"<p style='text-align: center; font-size:20px; font-weight: bold; color:blue;'>{ia_answer[0]}</p>", unsafe_allow_html=True)

# Convertendo a coluna usagedate para o formato de data e extraindo o mês
df["usagedate"] = pd.to_datetime(df["usagedate"])
df["month"] = df["usagedate"].dt.strftime("%Y-%m")  # Formato YYYY-MM

# Agrupando e somando os valores
df_grouped = df.groupby("month")["pretaxcost"].sum().reset_index()

# Criando o gráfico de barras
barra = px.bar(df_grouped, x="month", y="pretaxcost", title="Custo Cloud por Mês")

# Exibindo no Streamlit
st.plotly_chart(barra)

# Criando o gráfico de pizza
fig_pizza = px.pie(df, values="pretaxcost", names="resourcegroup", title="Distribuição de Custo por Resource Group")

# Exibindo no Streamlit
st.plotly_chart(fig_pizza)

# Ordenando os dados do maior para o menor
barra_lateral = df.sort_values(by="pretaxcost", ascending=False)

# Criando o gráfico de barras horizontais
fig_barra_lateral = px.bar(barra_lateral, x="pretaxcost", y="recurso", orientation='h', title="Custo dos Recursos")

# Exibindo no Streamlit
st.plotly_chart(fig_barra_lateral)

st.markdown("<h3 style='text-align: center;'>Dados para Download</h3>", unsafe_allow_html=True)
st.markdown("<div style='display: flex; justify-content: center;'>", unsafe_allow_html=True)
st.write(df)
st.markdown("</div>", unsafe_allow_html=True)


