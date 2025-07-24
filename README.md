# Pipeline de Dados com Docker V1

## Objetivo:

O objetivo deste projeto é extrair dados do MS Graph onde podemos coletar informações de uma subscription especiifca referente a custo cloud da Azure. Após a captura, os dados serão tratrados utilizando ferramenta de alta performace para bigdata e carregados em um banco de dados para armazenamento da informação e então, será gerado insgts para redução ou apontamento prévio referente aos custos em recursos que sejam relevante ao budget da conta em questão.

<div align="center">
  <img src="https://github.com/user-attachments/assets/0d005317-c273-465b-8b84-ade1a2e8426d" alt="Desenho Funcional">
  <p><b>Desenho Funcional</b></p>
</div>

<p align="left">
</p>

## Introdução:

O projeto foi desenvolvido utilizando a linguagem de programação Python/Spark com as seguintes bibliotecas:
- Kafka-python para envio de dados em tempo real;
- Minio para integração com o Bucket S3 Minio;
- PySpark para processamento de dados bigdata;
- Delta-Spark para armazenamento no formato delta no Bucket do MIinio;
- Scipy e Numpy para analise e exploração estatistica;
- Apache-Airflow para orquestração do pipeline;
- Psycopg2 para conexões ao banco de dados Postgres;
- LangChain para gerar insghts com IA ao dataframe final.
- Pandas e Streamlit para gerar insights e gráficos.

A arquitetura do projeto foi construida utilizando Docker onde utilizamos container para execução de recursos do Apache Kafka, Apache Spark, Minio, Apache Airflow, Postgres e Streamlit.

Foi criado variáveis de ambientes para armazemento de Keys e Senhas aos recursos.

<div align="center">
  <img src="https://github.com/user-attachments/assets/ed76af20-f6a2-4727-81d3-8be1c91a0a1e" alt="Desenho Técnico">
  <p><b>Desenho Técnico</b></p>
</div>

<p align="left">
</p>

## Meta:

1. **Dados em Tempo Real**:
    - **Objetivo**: Capturar dados brutos em tempo real utilziando Kafka.
    - **Benefício**: Permite trabalhar com dados atualziados na etapa de processamento.

2. **Processamento Big Data**:
    - **Objetivo**: Processar volume de dados bigdata com SPARK.
    - **Benefício**: Facilidade na instalação e uso com Docker e integração entre recursos.

3. **Armazenamento Estrutura Medalhão**:
    - **Objetivo**: Aplicar metodologia de armazenamento de dados em estrutura medalhão utilizando MinIO.
    - **Benefício**: Facilidade na instalação com Docker e conexão com Spark.

4. **Schedulagem e Monitoramento**:
    - **Objetivo**: Schedular o processo para tratativa de dados no procesos medalhão com monitoramento do pipeline.
    - **Benefício**: Instalação facilitada com Docker e monitoramento do fluxo.

5. **Integração com IA**:
    - **Objetivo**: Gerar insight do dataframe final utilizando IA com a biblioteca LangChain.
    - **Benefício**: Instalação  e utilização simplificada da biblioteca python integrada ao OpenIA.

6. **Arquitetura de Dados**:
    - **Objetivo**: Construção dos recursos utilizando Docker.
    - **Benefício**: Arquitetura robusta on primese com custo zero.


## Iniciando Projeto:

Requisito: Ter insatalo em sua maquina o Docker Desktop

1- Realizar o clone do repositório em sua maquina local:

`git clone https://github.com/felipegoraroficial/projeto_azure_cost.git`

2- Suba o container Docker com o seguinte comando:

`docker compose build`
Para construir as imagens que estão no Dockerfile

`docker-compose up -d`
Para subir os recursos do container em segundo plano

3- Criar uma Secret no Minio

<div align="center">
  <img src="https://github.com/user-attachments/assets/5658a102-f5e3-4c70-9b50-4b2b3044d1e6" alt="painel minio">
  <p><b>Clique em "Create access key"</b></p>
</div>

<div align="center">
  <img src="https://github.com/user-attachments/assets/793368ef-df1e-456e-9b43-bdb638829f86" alt="criando secret">
  <p><b>Preencha os campos para criação da sua secret e clique em "Create"</b></p>
</div>

<div align="center">
  <img src="https://github.com/user-attachments/assets/e108c3f5-9f93-4d3a-9f2c-d587899ef138" alt="Secret criada">
  <p><b>Copie a Key e a Secret e cole em seu ambiente de sistema</b></p>
</div>

4- Atualizar Imagem Docker com a Secret do Minio:

OBS: Necessário derrubar os recursos Docker e subir novamente para a atualização

`docker compose down`
Para derrubar os recursos docker em execução

`docker-compose up -d`
Para subir os recursos do container em segundo plano

5- Criar uma connections do Minio ao Airflow

Necessário para que o airflow só start um fluxo se houver files na camada do Kafka.

<div align="center">
  <img src="https://github.com/user-attachments/assets/0fa56458-7cc1-497e-b56a-ba981a127264" alt="Secret criada">
  <p><b>Preencha as informações em seu respsctivo campo</b></p>
</div>

```
{
  "host": "http://<'seu host'>>",
  "enpoint_url": "http://<'seu endpoint'>>",
  "aws_access_key_id": "<'sua key'>",
  "aws_secret_access_key": "<'seu secret'>",
  "verify": false
}
```
