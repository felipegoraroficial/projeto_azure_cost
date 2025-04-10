# Gerando Insights com IA em Dados de Custo Cloud da Azure 

## Objetivo:

O objetivo deste projeto é extrair dados do MS Graph onde podemos coletar informações de uma subscription especiifca referente a custo cloud da Azure. Após a captura, os dados serão tratrados utilizando ferramenta de alta performace para bigdata e carregados em um banco de dados para armazenamento da informação e então, será gerado insgts para redução ou apontamento prévio referente aos custos em recursos que sejam relevante ao budget da conta em questão.

<div align="center">
  <img src="https://github.com/user-attachments/assets/0d005317-c273-465b-8b84-ade1a2e8426d" alt="Desenho Funcional">
  <p><b>Desenho Funcional</b></p>
</div>

<p align="left">
</p>

## Introdução:

O projeto foi desenvolvido utilizando a linguagem de programação Python com as seguintes bibliotecas:
- Kafka-python para envio de dados em tempo real;
- PyMongo para armazenamento de mensgens do kafka em collections do MongoDB;
- Boto3 para integração com o Bucket Minio;
- DuckDB para processamento de dados bigdata;
- LangChain para gerar insghts com IA ao dataframe final.
Para utilização dessas bibliotecas foi criado um ambiente virtual com Python

A arquitetura do projeto foi construida utilizando Docker onde utilizamos container para obter recursos do MongoDB, Apache Kafka, Minio, Apache Airflow e Postgres.
Foi criado variáveis de ambientes para armazemento de Keys e Senhas aos recursos.

O projeto está versionado em duas branches no GitHub, sendo elas: Dev e Prd

<div align="center">
  <img src="https://github.com/user-attachments/assets/126c4479-ac6a-4fa5-abbc-a100dfabf92e" alt="Desenho Técnico">
  <p><b>Desenho Técnico</b></p>
</div>

<p align="left">
</p>

## Meta:

1. **Dados em Tempo Real**:
    - **Objetivo**: Capturar dados brutos em tempo real utilziando Kafka.
    - **Benefício**: Permite trabalhar com dados atualziados na etapa de processamento com duckdb schedulado pelo Apache Airflow.

2. **Processamento Big Data - DuckDB**:
    - **Objetivo**: Processar volume de dados bigdata com alternativa ao SPARK.
    - **Benefício**: Facilidade na instalação e uso com syntax SQL.

3. **Armazenamento Estrutura Medalhão**:
    - **Objetivo**: Aplicar metologia de armazenamento de dados em estrutura medalhão utilizando MinIO.
    - **Benefício**: Facilidade na instalação com Docker e conexão com python.

4. **Schedulagem e Monitoramento**:
    - **Objetivo**: Schedular o processo para tratativa de dados no procesos medalhão com monitoramento do pipeline.
    - **Benefício**: Instalação facilitada com Docker e monitoramento do fluxo com alerta enviados por e-mail.

5. **Integração com IA**:
    - **Objetivo**: Gerar insight do dataframe final utilizando IA com a biblioteca LangChain.
    - **Benefício**: Instalação  e utilização simplificada da biblioteca python integrada ao OpenIA.

6. **Arquitetura Sem Custo**:
    - **Objetivo**: Construção dos recursos utilizando Docker.
    - **Benefício**: Arquitetura robusta on primese com custo zero.
