#!/usr/bin/env python3
# [Ref: 06_A轨_信号层 A 轨双路信号] L2：a_track_signal_cache（标的级 + 申万行业级 打标结果）
# 用法: make init-l2-a-track-signal-cache 或 PYTHONPATH=. python3 scripts/init_l2_a_track_signal_cache.py

import os
import sys
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

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
CREATE TABLE IF NOT EXISTS a_track_signal_cache (
    cache_key      VARCHAR(160) PRIMARY KEY,
    track_scope    VARCHAR(16)  NOT NULL,
    track_id       VARCHAR(128) NOT NULL,
    signal_summary TEXT         NOT NULL DEFAULT '{}',
    signal_at      TIMESTAMPTZ,
    fetched_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    ttl_sec        INT          NOT NULL DEFAULT 3600
);
CREATE INDEX IF NOT EXISTS idx_a_track_signal_scope ON a_track_signal_cache(track_scope, track_id);
CREATE INDEX IF NOT EXISTS idx_a_track_signal_fetched ON a_track_signal_cache(fetched_at DESC);
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
        for stmt in DDL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        cur.close()
        conn.close()
        print("L2 表 a_track_signal_cache 已就绪。")
    except Exception as e:
        print("建表失败: %s" % e, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
