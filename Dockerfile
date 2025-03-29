# Usar a imagem oficial do Mongo como base
FROM mongo:latest

# Atualizar pacotes e instalar dependências necessárias
RUN apt-get update && apt-get install -y \
    wget gnupg curl openjdk-11-jdk python3-pip && \
    wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | apt-key add - && \
    echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/6.0 multiverse" > /etc/apt/sources.list.d/mongodb-org-6.0.list && \
    apt-get update && \
    apt-get install -y mongodb-org-shell

# Definir o comando padrão do MongoDB
CMD ["mongod"]
