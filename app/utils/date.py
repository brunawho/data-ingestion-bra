from __future__ import annotations
from datetime import datetime

# Usa a data atual do sistema no formato YYYYMMDD
def today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")
