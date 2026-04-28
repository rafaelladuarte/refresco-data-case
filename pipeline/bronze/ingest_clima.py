import requests
import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configurações de Caminhos
# Define o diretório base do projeto a partir da localização deste script
BASE_DIR = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze" / "clima"

# Configurações da API
API_URL = "https://archive-api.open-meteo.com/v1/archive?latitude=-21.78&longitude=-43.35&start_date=2024-01-01&end_date=2024-12-31&daily=temperature_2m_max,precipitation_sum&timezone=America%2FSao_Paulo"
API_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

def fetch_weather_data():
    """
    Consome dados históricos de clima da API Open-Meteo.
    Retorna um DataFrame do Pandas com os dados diários.
    """
    try:
        logger.info(f"Iniciando consumo da API: {API_BASE_URL}")
        response = requests.get(API_URL)
        
        # Tratamento de erro básico para verificar status 200
        if response.status_code == 200:
            data = response.json()
            
            # Extração do nó 'daily' e conversão para DataFrame
            daily_data = data.get('daily', {})
            if not daily_data:
                logger.warning("Nenhum dado diário encontrado na resposta da API.")
                return None
                
            df = pd.DataFrame(daily_data)
            logger.info(f"Dados consumidos com sucesso. Total de registros: {len(df)}")
            return df
        else:
            logger.error(f"Falha na requisição. Status Code: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de conexão ao buscar dados da API: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao processar dados da API: {str(e)}")
        return None

def save_to_bronze(df):
    """
    Adiciona metadados, cria a estrutura de diretórios e salva os dados na camada Bronze.
    """
    if df is None or df.empty:
        logger.warning("O DataFrame está vazio. A gravação na camada Bronze foi abortada.")
        return

    try:
        # Adição de Metadados
        df['_ingested_at'] = datetime.now()
        df['_api_source'] = API_BASE_URL
        
        # Definição do particionamento: ingestion_date=YYYY-MM-DD
        ingestion_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        partition_path = BRONZE_DIR / f"ingestion_date={ingestion_date}"
        
        # Criação dos diretórios caso não existam (uso do pathlib)
        partition_path.mkdir(parents=True, exist_ok=True)
        
        target_path = partition_path / "clima_history.csv"
        
        # Armazenamento em CSV
        df.to_csv(target_path, index=False)
        logger.info(f"Dados armazenados com sucesso em: {target_path}")
        
    except Exception as e:
        logger.error(f"Erro ao salvar dados na camada Bronze: {str(e)}")
        raise

def main():
    """
    Função principal para coordenar a ingestão dos dados de clima.
    """
    logger.info("Iniciando Processo de Ingestão: API -> Bronze (Clima)")
    
    df_clima = fetch_weather_data()
    save_to_bronze(df_clima)
    
    logger.info("Processo de Ingestão de Clima finalizado.")

if __name__ == "__main__":
    main()
