from __future__ import annotations
import pandas as pd

# Função de cast para inteiros
def to_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")

# Função de cast para strings e remove aspas desnecessárias
def to_str(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.replace('^"|"$', '', regex=True)

# Converte números com vírgula decimal para float.
def to_float_pt(series: pd.Series) -> pd.Series:
    # Se já vier como número, só garante float
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce").astype(float)
    # Caso venha como string com vírgula
    return (
        series.astype(str)
              .str.replace(".", "", regex=False)  # remove separador de milhar, se houver
              .str.replace(",", ".", regex=False) # vírgula -> ponto
              .pipe(pd.to_numeric, errors="coerce")
              .astype(float)
    )

# Aplica os casts conforme listas de colunas (uso típico no CSV)
def apply_casts(df: pd.DataFrame, integer_fields: list[str], string_fields: list[str], float_fields: list[str]) -> pd.DataFrame:
    for col in integer_fields:
        if col in df.columns:
            df[col] = to_int(df[col])
    for col in string_fields:
        if col in df.columns:
            df[col] = to_str(df[col])
    for col in float_fields:
        if col in df.columns:
            df[col] = to_float_pt(df[col])
    return df

# Converte colunas inteiras para Int64 (nullable) e normaliza strings (strip + remoção de aspas)
def clean_dataframe(
    df: pd.DataFrame,
    int_fields: list[str] | None = None,
    str_fields: list[str] | None = None,
) -> pd.DataFrame:
    int_fields = int_fields or []
    str_fields = str_fields or []
    for col in int_fields:
        if col in df.columns:
            df[col] = to_int(df[col])
    for col in str_fields:
        if col in df.columns:
            df[col] = to_str(df[col])
    return df
