from __future__ import annotations
import pandas as pd

class SchemaError(Exception):
    pass

def ensure_required_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SchemaError(f"Colunas ausentes no DataFrame: {missing}")

def check_dtypes(df: pd.DataFrame, integer_fields: list[str], string_fields: list[str], float_fields: list[str]) -> None:
    # Verificações simples de tipo após cast
    wrong = []

    for col in integer_fields:
        if col in df.columns and str(df[col].dtype) != "Int64":
            wrong.append((col, "Int64", str(df[col].dtype)))

    for col in string_fields:
        if col in df.columns and df[col].dtype.kind != "O":  # object = string
            wrong.append((col, "string(object)", str(df[col].dtype)))

    for col in float_fields:
        if col in df.columns and df[col].dtype.kind != "f":
            wrong.append((col, "float", str(df[col].dtype)))

    if wrong:
        msg = "; ".join([f"{c} esperado {e}, encontrado {f}" for c, e, f in wrong])
        raise SchemaError(f"Tipos incorretos após cast: {msg}")
