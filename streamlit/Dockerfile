# Imagem oficial Python 3.12 slim — leve e sem dependências do Airflow
FROM python:3.12-slim

# Diretório de trabalho dentro do container
WORKDIR /app

# Dependências de sistema: curl (healthcheck) + libs para psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copia e instala as dependências Python antes do código (aproveita cache do Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# O código é montado via volume em dev, mas copiado aqui para produção
COPY . .

# Expõe a porta padrão do Streamlit
EXPOSE 8501

# Garante que o Streamlit escuta em todas as interfaces (necessário para Docker)
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_PORT=8501

CMD ["streamlit", "run", "main.py"]
