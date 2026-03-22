#!/usr/bin/env python3
# [Ref: 02_量化扫描引擎_实践] 将 config/symbol_names.csv 中的简称写入 L2 表 symbol_names（UPSERT）
# 前置：make init-l2-symbol-names-table；需 PG_L2_DSN
# 用法：make sync-symbol-names-csv 或 PYTHONPATH=. python3 scripts/sync_symbol_names_csv_to_l2.py

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

_env = ROOT / ".env"
if _env.exists():
    with open(_env, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and os.environ.get(k) is None:
                    os.environ[k] = v


def main() -> int:
    from diting.ingestion.config import get_pg_l2_dsn
    from diting.scanner.symbol_names import _save_to_db, load_symbol_names_csv_only

    dsn = (get_pg_l2_dsn() or "").strip()
    if not dsn:
        print("错误: 未配置 PG_L2_DSN", file=sys.stderr)
        return 1

    names = load_symbol_names_csv_only(root=ROOT, names_csv="config/symbol_names.csv")
    if not names:
        print("错误: symbol_names.csv 无有效行或文件不存在", file=sys.stderr)
        return 1

    _save_to_db(dsn, names, source="static_csv")
    print("已同步 L2 symbol_names:", len(names), "条（source=static_csv）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
