import pandas as pd
import os
from datetime import datetime
import logging
from pathlib import Path

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configurações de Caminhos
BASE_DIR = Path(__file__).parent.parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
BRONZE_DIR = BASE_DIR / "data" / "bronze"

def get_ingestion_date():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def get_timestamp():
    """Retorna o timestamp atual."""
    return datetime.now()

def process_table(table_name: str):
    """
    Lê um arquivo CSV da camada Raw, adiciona metadados e salva na camada Bronze
    seguindo a estrutura de particionamento por data de ingestão.
    """
    source_file = f"{table_name}.csv"
    source_path = RAW_DIR / source_file
    
    if not source_path.exists():
        logger.warning(f"Arquivo fonte não encontrado: {source_path}. Pulando...")
        return

    try:
        logger.info(f"Iniciando ingestão da tabela: {table_name}")
        
        # 1. Leitura do arquivo original (Garante que a raw permaneça intacta)
        df = pd.read_csv(source_path, encoding="latin1")
        
        # 2. Enriquecimento com Metadados
        df['_ingested_at'] = get_timestamp()
        df['_source_file'] = source_file
        
        # 3. Definição do particionamento
        ingestion_date = get_ingestion_date()
        partition_path = BRONZE_DIR / table_name / f"ingestion_date={ingestion_date}"
        
        # 4. Idempotência: Cria diretório (sobrescreve logicamente ao salvar o arquivo)
        partition_path.mkdir(parents=True, exist_ok=True)
        
        target_path = partition_path / f"{table_name}.csv"
        
        # 5. Escrita na camada Bronze (CSV)
        # index=False para não salvar o índice do pandas como coluna
        df.to_csv(target_path, index=False)
        
        logger.info(f"Sucesso: {table_name} ingerido em {target_path}")
        
    except Exception as e:
        logger.error(f"Erro ao processar a tabela {table_name}: {str(e)}")
        raise

def main():
    """Função principal para coordenar a ingestão das entidades."""
    tables = ['clientes', 'financeiro', 'produtos', 'vendas']
    
    logger.info("Iniciando Pipeline de Ingestão: Raw -> Bronze")
    
    for table in tables:
        process_table(table)
        
    logger.info("Pipeline de Ingestão finalizado com sucesso.")

if __name__ == "__main__":
    main()
