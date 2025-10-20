from __future__ import annotations
import json
import os
import csv
import pandas as pd

from utils.casting import apply_casts
from utils.date import today_yyyymmdd
from utils.validate import ensure_required_columns, check_dtypes, SchemaError
from utils.metadata import write_metadata_from_df

# Carrega configuração JSON do caminho especificado
def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# Normaliza nomes de colunas conforme mapeamento fornecido
def normalize_columns(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    return df.rename(columns=mapping)


# Função principal de ingestão do CSV
def main():
    cfg = load_config(os.path.join(os.path.dirname(__file__), "config", "indicadores_municipios.json"))

    # Extrai configurações específicas
    csv_cfg = cfg["csv"]
    schema_cfg = cfg["schema"]
    output_cfg = cfg["output"]
    col_norm = cfg["columns_normalization"]

    csv_path   = csv_cfg["path"]
    in_sep     = csv_cfg.get("delimiter", ";")
    in_encoding= csv_cfg.get("encoding", "utf-8")
    has_header = csv_cfg.get("has_header", True)

    # Monta configurações para leitura do CSV
    read_config = dict(sep=in_sep, encoding=in_encoding, dtype=str, engine="python", quoting=0)
    if not has_header:
        read_config["header"] = None

    # Lê CSV bruto usando as configurações definidas
    df_raw = pd.read_csv(csv_path, **read_config)

    # Normaliza nomes de colunas (acentos, espaços, pontos) conforme mapeamento do config
    df = normalize_columns(df_raw, col_norm)

    # Valida colunas obrigatórias
    ensure_required_columns(df, schema_cfg["required_columns"])

    # Validação de colunas extras (não previstas no schema)
    expected_cols = (
    schema_cfg["required_columns"]
    + schema_cfg.get("integer_fields", [])
    + schema_cfg.get("string_fields", [])
    + schema_cfg.get("float_fields", [])
   )
    extras = [c for c in df.columns if c not in expected_cols]
    if extras:
       print(f"[AVISO] Colunas adicionais encontradas (serão mantidas): {extras}")

    # Aplica casts conforme schema
    df = apply_casts(
        df,
        integer_fields=schema_cfg.get("integer_fields", []),
        string_fields=schema_cfg.get("string_fields", []),
        float_fields=schema_cfg.get("float_fields", []),
    )

    # Revalida tipos após casts
    check_dtypes(
        df,
        integer_fields=schema_cfg.get("integer_fields", []),
        string_fields=schema_cfg.get("string_fields", []),
        float_fields=schema_cfg.get("float_fields", []),
    )

    # Mostra as 10 primeiras linhas (apenas Município, UF e Densidade SMP)
    preview_cols = ["municipio", "uf", "densidade_smp"]

    # Validação das colunas esperadas no preview
    missing = [c for c in preview_cols if c not in df.columns]
    if missing:
       raise SchemaError(f"Colunas de preview ausentes: {missing}")

    print("\n>>> Prévia (10 linhas) — colunas: Município, UF e Densidade SMP")
    print(df[preview_cols].head(10).to_string(index=False))

    # Salva em TXT (CSV) em bronze
    anomesdia     = today_yyyymmdd()
    base_dir      = output_cfg["base_dir"]
    table         = output_cfg["table"]
    partition_key = output_cfg["partition_key"]
    filename      = output_cfg["filename"]

    out_delimiter = output_cfg.get("csv_delimiter", ";")
    out_encoding  = output_cfg.get("encoding", "utf-8")
    out_index     = output_cfg.get("index", False)

    out_dir  = os.path.join(base_dir, table, f"{partition_key}={anomesdia}")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, filename)

    # salva como TXT com separador ';' e encoding UTF-8
    df.to_csv(
    out_path,
    sep=out_delimiter,                
    encoding=out_encoding,       
    index=out_index,
    lineterminator="\n"    
)
    print(f"\n Salvo com sucesso em: {out_path}")
 
    # Metadados
    write_metadata_from_df(
        df=df,
        path=out_path,
        dataset="ibc_municipios.indicadores_normalizados",
        origem="csv",
        endpoint=csv_path,                 
        delimitador=out_delimiter,
        encoding=out_encoding,
        partition_key=partition_key,
        producer="ingestao_csv.py",
    )
    print("Metadados gerados com sucesso.")


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        print(f"[ERRO] Arquivo não encontrado: {e}")
    except SchemaError as e:
        print(f"[ERRO] Falha de schema: {e}")
    except Exception as e:
        print(f"[ERRO] {type(e).__name__}: {e}")
