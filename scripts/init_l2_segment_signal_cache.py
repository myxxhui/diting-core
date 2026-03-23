#!/usr/bin/env python3
# [Ref: 12_右脑数据支撑与Segment规约] L2 建表：segment_signal_cache（细分信号缓存）
# 用法：make init-l2-segment-signal-cache 或 PYTHONPATH=. python3 scripts/init_l2_segment_signal_cache.py
# DDL 与 12_规约 §2.3 一致；信号层 refresh_segment_signals_for_symbols 写入，Module C 消费

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
CREATE TABLE IF NOT EXISTS segment_signal_cache (
    segment_id     VARCHAR(64) PRIMARY KEY,
    signal_summary TEXT NOT NULL,
    signal_at      TIMESTAMPTZ,
    fetched_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ttl_sec        INT NOT NULL DEFAULT 3600
);
CREATE INDEX IF NOT EXISTS idx_segment_signal_cache_fetched ON segment_signal_cache(fetched_at);

CREATE TABLE IF NOT EXISTS segment_signal_audit (
    id                   SERIAL PRIMARY KEY,
    segment_id           VARCHAR(64) NOT NULL,
    source_type          VARCHAR(32) NOT NULL DEFAULT 'rule',
    raw_snippet          TEXT,
    model_conclusion_json TEXT,
    error_message        TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_segment_signal_audit_segment_created ON segment_signal_audit(segment_id, created_at);
"""

COMMENT = """
COMMENT ON TABLE segment_signal_cache IS '细分垂直信号缓存；信号层 refresh_segment_signals_for_symbols 写入，Module C 消费';
COMMENT ON TABLE segment_signal_audit IS '细分信号理解审计；audit_enabled 时每次理解写一条，支持 audit_reuse_same_day';
"""


def main() -> int:
    dsn = os.environ.get("PG_L2_DSN", "").strip()
    if not dsn:
        print("未配置 PG_L2_DSN。", file=sys.stderr)
        return 1
    try:
        import psycopg2
    except ImportError:
        print("未安装 psycopg2。", file=sys.stderr)
        return 1
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        for stmt in (DDL + COMMENT).strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                try:
                    cur.execute(stmt)
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        raise
        cur.close()
        conn.close()
        print("L2 表 segment_signal_cache、segment_signal_audit 已就绪。")
    except Exception as e:
        print("建表失败: %s" % e, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
