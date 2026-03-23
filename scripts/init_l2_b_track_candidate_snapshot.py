#!/usr/bin/env python3
# [Ref: 03_/B轨/02_B轨数据与存储规约] L2 建表：b_track_candidate_snapshot
# 用法：make init-l2-b-track-candidate 或 PYTHONPATH=. python3 scripts/init_l2_b_track_candidate_snapshot.py
# DITING_TRACK=b 时 run_module_c 与 run_refresh_segment_signals 从此表取 symbols

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
CREATE TABLE IF NOT EXISTS b_track_candidate_snapshot (
    id              BIGSERIAL PRIMARY KEY,
    batch_id        VARCHAR(64) NOT NULL,
    symbol          VARCHAR(32) NOT NULL,
    symbol_name     VARCHAR(64) NOT NULL DEFAULT '',
    phase_score     NUMERIC(10,4) DEFAULT NULL,
    trend_confirm   BOOLEAN DEFAULT NULL,
    sector_strength NUMERIC(10,4) DEFAULT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_b_track_candidate_batch ON b_track_candidate_snapshot(batch_id);
CREATE INDEX IF NOT EXISTS idx_b_track_candidate_created ON b_track_candidate_snapshot(created_at);
"""

COMMENT = """
COMMENT ON TABLE b_track_candidate_snapshot IS 'B 轨中线候选；DITING_TRACK=b 时 C 与 refresh 从此表取 symbols';
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
        print("L2 表 b_track_candidate_snapshot 已就绪。")
    except Exception as e:
        print("建表失败: %s" % e, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
