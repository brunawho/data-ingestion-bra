# utils/manifest.py
from __future__ import annotations
import os, json, hashlib, datetime
from typing import Optional, Dict, Any, Iterable
from dataclasses import dataclass, asdict

try:
    import pandas as pd
except Exception:  # opcional, para não forçar pandas em todos os usos
    pd = None


# ===== Helpers =====

def _md5(path: str, chunk_size: int = 1 << 20) -> str:
    md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            md5.update(chunk)
    return md5.hexdigest()


def _now_iso() -> str:
    return datetime.datetime.now().isoformat(timespec="seconds")


def _pandas_dtypes(df) -> Dict[str, str]:
    # Mapeia dtypes pandas para strings simples
    return {c: str(df[c].dtype) for c in df.columns}


def _null_counts(df) -> Dict[str, int]:
    return {c: int(df[c].isna().sum()) for c in df.columns}


def _head_preview(df, n: int = 3) -> list[dict[str, Any]]:
    # pequena amostra só para depuração/inspeção
    return df.head(n).to_dict(orient="records")


def _count_file_lines(path: str) -> int:
    # conta linhas sem carregar tudo em memória (rápido para CSVs grandes)
    count = 0
    with open(path, "rb") as f:
        for _ in f:
            count += 1
    return count


# ===== Modelo do Manifest (flexível, mas com campos "core") =====

@dataclass
class CoreInfo:
    arquivo: str
    diretorio: str
    tamanho_bytes: int
    hash_md5: str
    gerado_em: str


@dataclass
class DatasetInfo:
    dataset: Optional[str] = None           # ex.: "jsonplaceholder.users"
    origem: Optional[str] = None            # ex.: "api", "csv"
    endpoint: Optional[str] = None          # ex.: URL da API (se houver)
    delimitador: Optional[str] = None       # ex.: ";"
    encoding: Optional[str] = None          # ex.: "utf-8" / "utf-8-sig"
    partition_key: Optional[str] = None     # ex.: "anomesdia"
    partition_value: Optional[str] = None   # ex.: "20251020"
    run_id: Optional[str] = None            # ex.: id único de execução (se tiver)
    producer: Optional[str] = None          # ex.: "ingestao_api.py"


@dataclass
class SchemaStats:
    colunas: list[str]
    dtypes: Optional[Dict[str, str]] = None
    linhas: Optional[int] = None
    nulos: Optional[Dict[str, int]] = None
    preview: Optional[list[dict]] = None    # pequena amostra opcional


# ===== API pública =====

def write_metadata_from_df(
    df, path: str, *,
    dataset: Optional[str] = None,
    origem: Optional[str] = None,
    endpoint: Optional[str] = None,
    delimitador: Optional[str] = None,
    encoding: Optional[str] = None,
    partition_key: Optional[str] = None,
    partition_value: Optional[str] = None,
    run_id: Optional[str] = None,
    producer: Optional[str] = None,
    include_dtypes: bool = True,
    include_nulls: bool = True,
    include_preview: bool = False,
    extra: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Gera <path>.manifest.json usando informações do DataFrame (mais rico).
    """
    if pd is None:
        raise RuntimeError("pandas é necessário para write_manifest_from_df")

    # garanta que o arquivo existe (para hash/bytes)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado para manifest: {path}")

    core = CoreInfo(
        arquivo=os.path.basename(path),
        diretorio=os.path.dirname(path),
        tamanho_bytes=os.path.getsize(path),
        hash_md5=_md5(path),
        gerado_em=_now_iso(),
    )

    stats = SchemaStats(
        colunas=[str(c) for c in df.columns],
        dtypes=_pandas_dtypes(df) if include_dtypes else None,
        linhas=int(len(df)),
        nulos=_null_counts(df) if include_nulls else None,
        preview=_head_preview(df, 3) if include_preview else None,
    )

    meta = DatasetInfo(
        dataset=dataset,
        origem=origem,
        endpoint=endpoint,
        delimitador=delimitador,
        encoding=encoding,
        partition_key=partition_key,
        partition_value=partition_value,
        run_id=run_id,
        producer=producer,
    )

    manifest: Dict[str, Any] = {
        "core": asdict(core),
        "dataset": asdict(meta),
        "schema_stats": asdict(stats),
    }
    if extra:
        manifest["extra"] = extra

    out = path + ".manifest.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    return out


def write_manifest_from_file(
    path: str, *,
    dataset: Optional[str] = None,
    origem: Optional[str] = None,
    endpoint: Optional[str] = None,
    delimitador: Optional[str] = None,
    encoding: Optional[str] = None,
    partition_key: Optional[str] = None,
    partition_value: Optional[str] = None,
    run_id: Optional[str] = None,
    producer: Optional[str] = None,
    header: bool = True,
    infer_columns_from_header: bool = True,
    line_count: bool = True,
    extra: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Gera <path>.manifest.json sem precisar de DataFrame (rápido e genérico).
    Para CSV: pode inferir colunas pela primeira linha (header).
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado para manifest: {path}")

    core = CoreInfo(
        arquivo=os.path.basename(path),
        diretorio=os.path.dirname(path),
        tamanho_bytes=os.path.getsize(path),
        hash_md5=_md5(path),
        gerado_em=_now_iso(),
    )

    cols: list[str] = []
    rows: Optional[int] = None

    if infer_columns_from_header and header and delimitador:
        try:
            with open(path, "r", encoding=encoding or "utf-8") as f:
                first = f.readline().strip("\n\r")
                cols = [c.strip() for c in first.split(delimitador)]
        except Exception:
            cols = []

    if line_count:
        rows = _count_file_lines(path)
        # se tem header real, linhas de dados = linhas - 1
        if header and rows and rows > 0:
            rows = rows - 1

    stats = SchemaStats(
        colunas=cols,
        dtypes=None,     #
        linhas=rows,
        nulos=None,
        preview=None,
    )

    meta = DatasetInfo(
        dataset=dataset,
        origem=origem,
        endpoint=endpoint,
        delimitador=delimitador,
        encoding=encoding,
        partition_key=partition_key,
        partition_value=partition_value,
        run_id=run_id,
        producer=producer,
    )

    manifest: Dict[str, Any] = {
        "core": asdict(core),
        "dataset": asdict(meta),
        "schema_stats": asdict(stats),
    }
    if extra:
        manifest["extra"] = extra

    out = path + ".manifest.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    return out
