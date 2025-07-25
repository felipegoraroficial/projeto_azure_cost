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

<div align="center">
  <img src="https://github.com/user-attachments/assets/5835c3ac-fc38-4809-bcd7-6af2a91497cd" alt="Desenho Técnico">
  <p><b>Desenho Técnico</b></p>
</div>

<p align="left">
</p>

Para ambiente de desenvolvimento foi utilizado a imagem Docker Jupyter com Pyspark

<div align="center">
  <img src="https://github.com/user-attachments/assets/bf65b926-1bef-4d2e-95cf-5f1783811b06" alt="Ambientes" width="250"/>
</div>


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


## Incluíndo Requisitos

- MS Graph

Para conseguir acessar a API do MS Graph e obter dados referente aos custos cloud de sua assiantura na Azure, siga esse passo a passo:

1- Crie um registro de aplciativo e armazene em sua variável de ambiente as seguintes informações:

- CLIENT_ID
- CLIENT_SECRET
- TENANT_ID
- SUBSCRIPTION_ID

2 - Conceder permissão ao Regristro de aplicativo ao MS Graph:

<div align="center">
  <img src="https://github.com/user-attachments/assets/a4ec1570-e97c-49fe-8018-1aa50ac917e9" alt="registro aplciativo">
  <p><b>Em seu registro de aplicativo , vá em Permissões de APIs"</b></p>
</div>

<div align="center">
  <img src="https://github.com/user-attachments/assets/2eb1be96-ab92-4414-99e3-f9696a2db116" alt="clicar em adicionar permissão">
  <p><b>Clique em Adiconar uma permissão"</b></p>
</div>

<div align="center">
  <img src="https://github.com/user-attachments/assets/08be0755-64bd-4f00-adf3-916460b9fb04" alt="permissão api">
  <p><b>Selecione MS Graph e busque por APIConnectors"</b></p>
</div>

<div align="center">
  <img src="https://github.com/user-attachments/assets/834d3673-a440-4d7e-9408-d66e662c4223" alt="api não concedida">
  <p><b>Após incluir a permissão, note que a permissão ainda não está concedida"</b></p>
</div>

<div align="center">
  <img src="https://github.com/user-attachments/assets/2d74aed6-685a-40e2-ae9d-906041dabc6d" alt="api concedida">
  <p><b>Após conceder a permissão ao aplicativo, agora estaremos apto a conectar ao MS Graph"</b></p>
</div>

## Iniciando Projeto:

Requisito: 
- Ter um Registro de Aplicativo em sua assinatura Azure com permissão para conectar a API MS Graph
- Ter insatalo em sua maquina o Docker Desktop

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
  <img src="https://github.com/user-attachments/assets/7a7f4515-dedc-4efc-b8d0-f2bc0e6916bc" alt="Secret criada">
  <p><b>Preencha as informações em seu respsctivo campo</b></p>
</div>

```
{
  "host": "http://minio:9000",
  "endpoint_url": "http://minio:9000",
  "aws_access_key_id": "QN71RDa4HFrTQJYGnWe2",
  "aws_secret_access_key": "H5MOUH77RV11EIQcJEVgPUpXOSooc7AQ4WckKCJO",
  "verify": false
}
```

## Como usar Jupyter Notebook:

Use o comando :

`docker ps` 
para lsitar todas as isntancias em execção do docker

Procure pela IMAGE projeto_azure_cost-jupyter-pyspark e execute o comando:

`docker logs jupyter-pyspark` 

Isso irá listar o logs da imagem projeto_azure_cost-jupyter-pyspark

Agora basta procurar pelo link de acesso ao Server do Jupyter-Pyspark

<div align="center">
  <img src="https://github.com/user-attachments/assets/f1022ec9-aaea-4444-89f3-4487f66ca13b" alt="Jupyter Server">
</div>

Ao clicar nesse link em marcação, você será direiconado ao Jupyter em sua localhost:

<div align="center">
  <img src="https://github.com/user-attachments/assets/b9f0f84f-661a-4dae-8be8-6fd2fba4625c" alt="Jupyter local">
</div>

Para rodar o Jupyter em seu VSCode, é necessário ter instalado a extensão:

<div align="center">
  <img src="https://github.com/user-attachments/assets/f39ffadb-f340-43e9-83f0-b3a537872935" alt="Jupyter extensão">
</div>

Agora basta seguir esses passos:

1 - Em um arquivo ipynb, selecione o Kernel

<div align="center">
  <img src="https://github.com/user-attachments/assets/6fe9a93e-0710-4fdb-a3e0-6955c542fb85" alt="Select Kernel">
</div>

2 - Selecione Existing Jupyter Server...

<div align="center">
  <img src="https://github.com/user-attachments/assets/2543cd86-4634-4c52-a4af-5ac93f5a183b" alt="Existente Jupyter">
</div>

3 - Insira o localhost (o mesmo do logs)

<div align="center">
  <img src="https://github.com/user-attachments/assets/bc40aa33-11a9-47ad-944e-93454e2dca53" alt="Localhost Jupyter">
</div>

4 - Insira o token (o mesmo do logs)

<div align="center">
  <img src="https://github.com/user-attachments/assets/e5438abd-74f8-4f19-bfe7-cb90c1be3809" alt="token Jupyter">
</div>

5 - Insira um nome para sua conexão

<div align="center">
  <img src="https://github.com/user-attachments/assets/d0cf435a-9943-4e8b-91b7-d987b055a800" alt="Name Jupyter">
</div>

5 - Selecione o Kernel da conexão

<div align="center">
  <img src="https://github.com/user-attachments/assets/36b88a79-ef9c-4fb8-8881-321f72e364b9" alt="Kernel Jupyter">
</div>

5 - Agora está tudo certo para realizar um teste com pyspark

<div align="center">
  <img src="https://github.com/user-attachments/assets/a2b9caac-dfbb-4687-9e5d-791d75d2d61f" alt="Testando Jupyter">
</div>