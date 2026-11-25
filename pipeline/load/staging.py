from sqlalchemy import create_engine, text

from pipeline.common.config import postgres_url
from pipeline.common.logging import get_logger
from pipeline.common.storage import read_silver

logger = get_logger(__name__)


def load_to_staging():
    logger.info("Iniciando carga de dados da camada Silver para Staging (Postgres-Gold)")

    engine = create_engine(postgres_url())

    schema_name = "load_stage"

    try:
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))
            logger.info(f"[OK] Schema '{schema_name}' verificado/criado com sucesso.")
    except Exception as e:
        logger.error(f"[ERRO] Erro ao criar o schema {schema_name}: {e}")
        raise

    tabelas = ["vendas", "produtos", "clientes", "clima", "financeiro"]

    for tabela in tabelas:
        logger.info(f"Iniciando carga da tabela: {tabela}")
        s3_path = f"s3://silver/{tabela}/"

        try:
            df = read_silver(s3_path)
            logger.info(f"Lidos {len(df)} registros da tabela {tabela} do S3.")

            kwargs = {"name": tabela, "schema": schema_name, "if_exists": "replace", "index": False}

            if tabela == "vendas":
                kwargs["chunksize"] = 10000
                kwargs["method"] = "multi"

            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{tabela} CASCADE;"))
                logger.info(f"DROP TABLE CASCADE executado com sucesso para {schema_name}.{tabela}.")

                kwargs["con"] = conn
                df.to_sql(**kwargs)

            logger.info(f"[OK] Tabela {tabela} carregada com sucesso no Postgres (Schema: {schema_name}).")

        except Exception as e:
            logger.error(f"[ERRO] Erro ao carregar a tabela {tabela}: {e}")
            raise

    logger.info("Carga de todos os arquivos para staging finalizada com sucesso!")


if __name__ == "__main__":
    load_to_staging()
