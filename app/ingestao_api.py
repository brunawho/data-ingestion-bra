from __future__ import annotations
import json
import time
import os
import sys
from typing import Optional, Dict, Any, Union, Tuple
import requests
import pandas as pd

from utils.date import today_yyyymmdd
from utils.casting import clean_dataframe
from utils.validate import ensure_required_columns, check_dtypes, SchemaError
from utils.metadata import write_metadata_from_df


# Erros de chamada/uso da API
class ApiError(Exception):
    pass

# Carrega o simulacao_api.json como dicionário.
def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

_SESSION: Optional[requests.Session] = None

# Singleton de sessão requests com persistência de conexões
def get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
    return _SESSION


# Realiza GET seguro com retries e timeout
def safe_get(url: str, *, timeout: Union[int, Tuple[int,int]]=(5,30), retries: int=2, params: Optional[Dict[str,Any]]=None) -> requests.Response:
    sess = get_session()
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            resp = sess.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp
            # 429/503 → tenta novamente com Retry-After
            if resp.status_code in (429, 503):
                ra = resp.headers.get("Retry-After")
                sleep_s = int(ra) if (ra and ra.isdigit()) else min(2 ** attempt, 16)
            else:
                sleep_s = min(2 ** attempt, 8)
            last_err = ApiError(f"HTTP {resp.status_code} — {resp.text[:200]}")
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            sleep_s = min(2 ** attempt, 8)
        if attempt < retries:
            time.sleep(sleep_s + (0.1 * attempt))
    raise ApiError(f"Falha ao chamar {url}: {last_err}")

# Busca todos os usuários da API e retorna DataFrame limpo
def find_users(base_url: str, ep_users: str, timeout: int, retries: int) -> pd.DataFrame:
    url = f"{base_url}{ep_users}"
    resp = safe_get(url, timeout=timeout, retries=retries)
   
   # Valida Json
    try:
        data = resp.json()
    except ValueError as e:
        raise ApiError(f"Resposta não é JSON válido de {resp.url}: {e}")

    # Monta DataFrame e renomeia colunas
    users_df = pd.DataFrame(data, columns=["id", "name", "username", "email"]).rename(
        columns={
            "id": "user_id",
            "name": "nome",
            "username": "usuario",
            "email": "email",
        }
    )
    # Aplica limpeza/casts
    users_df = clean_dataframe(
        users_df,
        int_fields=["user_id"],
        str_fields=["nome", "usuario", "email"],
    )
    return users_df

# Busca todos os posts de um user_id e retorna DataFrame limpo
def find_posts_by_user_id(base_url: str, ep_posts: str, user_id: int, timeout: int, retries: int) -> pd.DataFrame:
    resp = safe_get(f"{base_url}{ep_posts}", timeout=timeout, retries=retries, params={"userId": user_id})
    
    # Valida Json
    try:
        data = resp.json()
    except ValueError as e:
        raise ApiError(f"Resposta não é JSON válido de {resp.url}: {e}")

    # Monta DataFrame e renomeia colunas
    posts_df = pd.DataFrame(data, columns=["userId", "id", "title", "body"]).rename(
        columns={
            "userId": "user_id",
            "id": "post_id",
            "title": "titulo",
            "body": "conteudo",
        }
    )

    posts_df = clean_dataframe(
        posts_df,
        int_fields=["user_id", "post_id"],
        str_fields=["titulo", "conteudo"],
    )
    return posts_df

# Salva DataFrame em arquivo TXT (CSV) com partição dinâmica anomesdia=AAAAMMDD
def save_txt(
    df: pd.DataFrame,
    base_dir: str,
    table: str,
    partition_key: str,
    filename: str,
    delimiter: str = ";",
    encoding: str = "utf-8",
    index: bool = False,
) -> str:
    anomesdia = today_yyyymmdd()
    out_dir = os.path.join(base_dir, table, f"{partition_key}={anomesdia}")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, filename)

    df.to_csv(out_path, sep=delimiter, encoding=encoding, index=index, lineterminator="\n")
    return out_path


def main():
    # Carrega configurações
    cfg_path = os.path.join(os.path.dirname(__file__), "config", "simulacao_api.json")
    cfg = load_config(cfg_path)

    api_cfg = cfg["api"]
    out_cfg = cfg["output"]
    schema_cfg = cfg["schema"]

    base_url = api_cfg["base_url"].rstrip("/")
    ep_users = api_cfg["endpoints"]["users"]
    ep_posts = api_cfg["endpoints"]["posts"]
    timeout = int(api_cfg.get("timeout_seconds", 20))
    retries = int(api_cfg.get("retries", 0))

    # Busca todos os usuários
    users_df = find_users(base_url, ep_users, timeout, retries)

    # Valida schema USERS
    ensure_required_columns(users_df, schema_cfg["users"]["required_columns"])
    check_dtypes(
        users_df,
        integer_fields=schema_cfg["users"]["integer_fields"],
        string_fields=schema_cfg["users"]["string_fields"],
        float_fields=schema_cfg["users"]["float_fields"],
    )

    # Captura user_id alvo para buscar posts
    alvo_nome = cfg["logic"]["user_target"]
    row = users_df.loc[users_df["nome"] == alvo_nome]
    if row.empty:
       print(f"[ERRO] Usuário '{alvo_nome}' não encontrado em /users", file=sys.stderr)
       sys.exit(2)
    user_id = int(row.iloc[0]["user_id"])


    # Busca todos os posts do usuário alvo
    posts_df = find_posts_by_user_id(base_url, ep_posts, user_id, timeout, retries)

     #Valida schema POSTS
    ensure_required_columns(posts_df, schema_cfg["posts"]["required_columns"])
    check_dtypes(
        posts_df,
        integer_fields=schema_cfg["posts"]["integer_fields"],
        string_fields=schema_cfg["posts"]["string_fields"],
        float_fields=schema_cfg["posts"]["float_fields"],
    )

    # Apresenta no stdout  
    print("\n=== Usuários (nome; usuário; email) ===")
    print(users_df.sort_values(["nome", "usuario"]).loc[:, ["nome", "usuario", "email"]].to_string(index=False))

    print(f"\n=== Posts do usuário '{alvo_nome}' (user_id={user_id}) ===")
    show = posts_df.sort_values("post_id").loc[:, ["post_id", "titulo"]]
    print("(nenhum post encontrado)" if show.empty else show.to_string(index=False))

    # Salva em TXT nas duas tabelas bronze 
    delimiter = out_cfg.get("csv_delimiter", ";")
    encoding = out_cfg.get("encoding", "utf-8")
    index = out_cfg.get("index", False)

    users_path = save_txt(
        users_df,
        base_dir=out_cfg["base_dir"],
        table=out_cfg["users_table"],
        partition_key=out_cfg["partition_key"],
        filename=out_cfg["users_filename"],
        delimiter=delimiter,
        encoding=encoding,
        index=index,
    )

    write_metadata_from_df(
        users_df,
        users_path,
        dataset="jsonplaceholder.users",
        origem="api",
        endpoint=f"{base_url}{ep_users}",
        delimitador=delimiter,
        encoding=encoding,
        partition_key=out_cfg["partition_key"],
        producer="ingestao_api.py",
    )

    posts_path = save_txt(
        posts_df,
        base_dir=out_cfg["base_dir"],
        table=out_cfg["posts_table"],
        partition_key=out_cfg["partition_key"],
        filename=out_cfg["posts_filename"],
        delimiter=delimiter,
        encoding=encoding,
        index=index,
    )

    write_metadata_from_df(
        posts_df,
        posts_path,
        dataset="jsonplaceholder.posts",
        origem="api",
        endpoint=f"{base_url}{ep_posts}?userId={user_id}",
        delimitador=delimiter,
        encoding=encoding,
        partition_key=out_cfg["partition_key"],
        producer="ingestao_api.py",
        extra={"user_id": int(user_id)},
    )

    print("\n Arquivos salvos com sucesso:")
    print(f" - {users_path}")
    print(f" - {posts_path}")
    print("Metadados gerados com sucesso.")

if __name__ == "__main__":
    try:
        main()
    # Trata erros comuns   
    except SchemaError as e:
        print(f"[ERRO DE SCHEMA] {e}", file=sys.stderr)
        sys.exit(3)
    except Exception as e:
        print(f"[ERRO] {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
