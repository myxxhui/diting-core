#!/usr/bin/env python3
# 从 config/industry_fallback.csv 直接回填 L2 industry_revenue_summary，不调用外部 API。
# 使用方式：cd diting-core && PYTHONPATH=. python3 scripts/backfill_industry_from_fallback.py

import os
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))
# 确保从 diting-core 根加载 .env
env_file = root / ".env"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and os.environ.get(k) is None:
                    os.environ[k] = v

from diting.ingestion.config import get_pg_l2_dsn
from diting.ingestion.industry_revenue import _upsert_industry_revenue_summary

CSV = root / "config" / "industry_fallback.csv"


def main():
    if not CSV.exists():
        print("missing", CSV)
        return 1
    rows = []
    with open(CSV, encoding="utf-8") as f:
        for line in f:
            line = line.strip().split("#")[0].strip()
            if not line or line.lower().startswith("symbol"):
                continue
            parts = line.split(",", 1)
            if len(parts) >= 2:
                symbol = parts[0].strip().upper()
                industry_name = parts[1].strip()
                if symbol:
                    rows.append((symbol, industry_name))
    if not rows:
        print("no rows in", CSV)
        return 1
    import psycopg2
    dsn = get_pg_l2_dsn()
    conn = psycopg2.connect(dsn)
    try:
        for symbol, industry_name in rows:
            _upsert_industry_revenue_summary(conn, symbol, industry_name, 0.0, 0.0, 0.0)
        print("backfill ok:", len(rows), "rows")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
