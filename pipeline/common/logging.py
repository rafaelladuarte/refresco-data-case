"""Configuração única de logging do pipeline."""
import logging

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Retorna o logger do módulo, configurando o root logger se ninguém o configurou.

    `basicConfig` não faz nada se o root já tem handlers (caso do Airflow, que
    captura os logs da task); fora do Airflow garante o formato padrão em INFO.
    """
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    return logging.getLogger(name)
