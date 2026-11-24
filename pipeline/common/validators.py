"""Padrões de ID e normalização de colunas usados por mais de uma entidade da Silver."""
import re

import pandas as pd

# Formato válido de id_cliente: C seguido de 4 dígitos (ex: C0001 – C9999)
PATTERN_ID_CLIENTE = re.compile(r'^C\d{4}$', re.IGNORECASE)
# Formato válido de id_produto: P seguido de 3 dígitos (ex: P001 – P999)
PATTERN_ID_PRODUTO = re.compile(r'^P\d{3}$', re.IGNORECASE)


def normalize_id(series: pd.Series) -> pd.Series:
    """Remove espaços, padroniza em maiúsculas e converte None/NaN para string vazia.

    IDs ficam vazios ("") em vez de nulos porque as regras de validação comparam
    com string vazia e os joins/lookups não lidam bem com nulo.
    """
    return series.fillna("").astype(str).str.strip().str.upper()


def clean_text(series: pd.Series) -> pd.Series:
    """Converte para o dtype de string nullable do pandas, remove espaços e trata vazio como nulo.

    Substitui o padrão `astype(str)`, que transforma NaN na string literal "nan"
    e faz nulos chegarem ao warehouse como se fossem valores de negócio.
    """
    return series.astype("string").str.strip().replace("", pd.NA)
