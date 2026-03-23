#!/usr/bin/env python3
# [Ref: 04_A轨_MoE议会_实践] L2 表 moe_expert_opinion_snapshot（Module C 输出）
# 用法：make init-l2-moe-opinion-table 或 PYTHONPATH=. python3 scripts/init_l2_moe_expert_opinion_table.py

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
CREATE TABLE IF NOT EXISTS moe_expert_opinion_snapshot (
    id                BIGSERIAL PRIMARY KEY,
    batch_id           VARCHAR(64)  NOT NULL,
    symbol             VARCHAR(32)  NOT NULL,
    opinions_json      JSONB        NOT NULL DEFAULT '[]',
    correlation_id     VARCHAR(64)  NOT NULL DEFAULT '',
    moe_run_metadata   JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_moe_expert_opinion_batch ON moe_expert_opinion_snapshot(batch_id);
CREATE INDEX IF NOT EXISTS idx_moe_expert_opinion_symbol ON moe_expert_opinion_snapshot(symbol);
CREATE INDEX IF NOT EXISTS idx_moe_expert_opinion_created ON moe_expert_opinion_snapshot(created_at DESC);
"""

MIGRATION_ALTER = """
ALTER TABLE moe_expert_opinion_snapshot
  ADD COLUMN IF NOT EXISTS moe_run_metadata JSONB NOT NULL DEFAULT '{}'::jsonb;
"""


def main():
    dsn = os.environ.get("PG_L2_DSN", "").strip()
    if not dsn:
        print("未配置 PG_L2_DSN。请在 .env 中配置 PG_L2_DSN。", file=sys.stderr)
        sys.exit(1)
    try:
        import psycopg2
    except ImportError:
        print("未安装 psycopg2。", file=sys.stderr)
        sys.exit(1)
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        for stmt in DDL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        cur.execute(MIGRATION_ALTER.strip())
        cur.close()
        conn.close()
        print("L2 表 moe_expert_opinion_snapshot 已就绪（含 moe_run_metadata）。")
    except Exception as e:
        print("创建表失败: %s" % e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
