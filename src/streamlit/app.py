import streamlit as st
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain_openai import ChatOpenAI
import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd
import plotly.express as px

env_path = os.path.join(os.getcwd(), '.env')
load_dotenv(dotenv_path=env_path)

ai_key = os.getenv('OPENAI_API_KEY')

st.markdown("<h1 style='text-align: center;'>FinOps Dashboard</h1>", unsafe_allow_html=True)

def load_data(table):

    # --- Configurações de conexão com o PostgreSQL ---
    DB_HOST = "airflow-postgres"
    DB_PORT = "5432"
    DB_USER = os.getenv('POSTGRES_USER')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    DB_NAME = "azurecost"
    TABLE_NAME = table

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
dfresources = load_data('resources')
dfcost = load_data('costresources')

df = pd.merge(dfresources, dfcost, on='id', how='left')

llm = ChatOpenAI(temperature=0.7, model="gpt-4o-mini", openai_api_key=ai_key)
agent = create_pandas_dataframe_agent(llm, df, verbose=True, allow_dangerous_code=True)

row_count = len(df)
st.markdown("<h2 style='text-align: center; color: green;'>Conectado ao banco de dados com sucesso.</h2>", unsafe_allow_html=True)

st.title("Pergunte para a IA")

# Campo para o usuário digitar a pergunta
pergunta = st.text_input("Digite sua pergunta:")

if pergunta:
    answer = agent.invoke(pergunta)
    st.markdown(f"**Resposta:** `{answer['output']}`")

cost_by_date = load_data('vw_cost_by_date')

# Criando o gráfico de barras
barra = px.bar(cost_by_date, x="usagedate", y="pretaxcost_sum", title="Custo Cloud por Mês")

# Exibindo no Streamlit
st.plotly_chart(barra)

resourcegroup = load_data('vw_resourcegroup_totais')

# Criando o gráfico de pizza
fig_pizza = px.pie(resourcegroup, values="total_pretaxcost", names="resourcegroup", title="Distribuição de Custo por Resource Group")

# Exibindo no Streamlit
st.plotly_chart(fig_pizza)

resourcename = load_data('vw_resourcename_totais')

# Ordenando os dados do maior para o menor
barra_lateral = resourcename.sort_values(by="total_pretaxcost", ascending=False)

# Criando o gráfico de barras horizontais
fig_barra_lateral = px.bar(barra_lateral, x="total_pretaxcost", y="resourcename", orientation='h', title="Custo dos Recursos")

# Exibindo no Streamlit
st.plotly_chart(fig_barra_lateral)

st.markdown("<h3 style='text-align: center;'>Dados para Download</h3>", unsafe_allow_html=True)
st.markdown("<div style='display: flex; justify-content: center;'>", unsafe_allow_html=True)
st.write(df)
st.markdown("</div>", unsafe_allow_html=True)


