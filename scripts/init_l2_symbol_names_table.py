#!/usr/bin/env python3
# [Ref: 02_量化扫描引擎_实践] 在 L2 库中创建 symbol_names 表（若不存在）
# 标的中文名持久化：优先从该表读取，缺失时从静态文件/东方财富(akshare)拉取并写入本表
# 用法：make init-l2-symbol-names-table 或 PYTHONPATH=. python3 scripts/init_l2_symbol_names_table.py

import os
import sys
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)

_env = Path(ROOT) / ".env"
if _env.exists():
    with open(_env, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and os.environ.get(k) is None:
                    os.environ[k] = v

DDL = """
CREATE TABLE IF NOT EXISTS symbol_names (
    symbol    VARCHAR(32) PRIMARY KEY,
    name_cn   VARCHAR(128) NOT NULL DEFAULT '',
    source    VARCHAR(32)  NOT NULL DEFAULT 'akshare',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_symbol_names_updated ON symbol_names(updated_at DESC);
"""


def main():
    dsn = os.environ.get("PG_L2_DSN", "").strip()
    if not dsn:
        print("未配置 PG_L2_DSN。请在 .env 中配置 PG_L2_DSN。", file=sys.stderr)
        sys.exit(1)
    try:
        import psycopg2
    except ImportError:
        print("未安装 psycopg2。请执行: pip install psycopg2-binary", file=sys.stderr)
        sys.exit(1)
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        for stmt in DDL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        cur.close()
        conn.close()
        print("L2 表 symbol_names 已就绪（已存在或已创建）。")
    except Exception as e:
        print("创建 symbol_names 表失败: %s" % e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
