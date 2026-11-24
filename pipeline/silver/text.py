"""Normalização de colunas de texto da camada Silver."""
import pandas as pd


def clean_text(series: pd.Series) -> pd.Series:
    """Converte para o dtype de string nullable do pandas, remove espaços e trata vazio como nulo.

    Substitui o padrão `astype(str)`, que transforma NaN na string literal "nan"
    e faz nulos chegarem ao warehouse como se fossem valores de negócio.
    """
    return series.astype("string").str.strip().replace("", pd.NA)
