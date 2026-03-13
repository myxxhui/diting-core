#!/usr/bin/env python3
# [Ref: 01_语义分类器_实践] 在 L2 库中创建 classifier_output_snapshot 表（若不存在）
# 用法：在 diting-core 根目录 make init-l2-classifier-table 或 PYTHONPATH=. python3 scripts/init_l2_classifier_table.py
# 需 .env 中 PG_L2_DSN 可达；与 diting-infra schemas/sql/06_l2_classifier_output_snapshot.sql 一致

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
CREATE TABLE IF NOT EXISTS classifier_output_snapshot (
    id                BIGSERIAL PRIMARY KEY,
    batch_id           VARCHAR(64)  NOT NULL,
    symbol             VARCHAR(32)  NOT NULL,
    primary_tag        VARCHAR(16)  NOT NULL DEFAULT 'UNKNOWN',
    primary_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
    tags_json          JSONB,
    correlation_id     VARCHAR(64)  NOT NULL DEFAULT '',
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_classifier_output_snapshot_batch ON classifier_output_snapshot(batch_id);
CREATE INDEX IF NOT EXISTS idx_classifier_output_snapshot_symbol ON classifier_output_snapshot(symbol);
CREATE INDEX IF NOT EXISTS idx_classifier_output_snapshot_created ON classifier_output_snapshot(created_at DESC);
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
        print("L2 表 classifier_output_snapshot 已就绪（已存在或已创建）。")
    except Exception as e:
        err = str(e).strip()
        print("创建表失败: %s" % err)
        if "Connection refused" in err or "could not connect" in err.lower():
            print()
            print("说明: 当前 .env 中 PG_L2_DSN 指向的地址不可达。")
            print("  - 若需连**远程**库：请将 .env 中 PG_L2_DSN 的 host 改为远程地址（如从 diting-infra 的 prod.conn 复制 PG_L2_DSN，其为 公网IP:NodePort，例如 43.x.x.x:30002）。")
            print("  - 若 L2 在本机：请先启动 L2（如 diting-infra 的 make local-deps-up），保证 127.0.0.1:30002 可连。")
        sys.exit(1)


if __name__ == "__main__":
    main()
