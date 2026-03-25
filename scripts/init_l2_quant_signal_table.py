#!/usr/bin/env python3
# [Ref: 02_量化扫描引擎_实践] 在 L2 库中创建 quant_signal_snapshot 表（若不存在）
# 用法：在 diting-core 根目录 make init-l2-quant-signal-table 或 PYTHONPATH=. python3 scripts/init_l2_quant_signal_table.py
# 需 .env 中 PG_L2_DSN 可达；与 diting-infra schemas/sql/07_l2_quant_signal_snapshot.sql 一致

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

# 与 diting-infra/schemas/sql/07_l2_quant_signal_snapshot.sql 一致；老库仍靠 ALTER_EXTRA 补列
DDL = """
CREATE TABLE IF NOT EXISTS quant_signal_snapshot (
    id                BIGSERIAL PRIMARY KEY,
    batch_id           VARCHAR(64)  NOT NULL,
    symbol             VARCHAR(32)  NOT NULL,
    symbol_name        VARCHAR(128) NOT NULL DEFAULT '',
    technical_score    DOUBLE PRECISION NOT NULL DEFAULT 0,
    strategy_source    VARCHAR(16)  NOT NULL DEFAULT 'UNSPECIFIED',
    sector_strength    DOUBLE PRECISION NOT NULL DEFAULT 0,
    trend_score        DOUBLE PRECISION NOT NULL DEFAULT 0,
    reversion_score    DOUBLE PRECISION NOT NULL DEFAULT 0,
    breakout_score     DOUBLE PRECISION NOT NULL DEFAULT 0,
    momentum_score     DOUBLE PRECISION NOT NULL DEFAULT 0,
    technical_score_percentile DOUBLE PRECISION,
    long_term_score    DOUBLE PRECISION,
    long_term_candidate BOOLEAN NOT NULL DEFAULT FALSE,
    correlation_id     VARCHAR(64)  NOT NULL DEFAULT '',
    signal_tier        VARCHAR(16) NOT NULL DEFAULT '',
    alert_passed       BOOLEAN NOT NULL DEFAULT FALSE,
    confirmed_passed   BOOLEAN NOT NULL DEFAULT FALSE,
    entry_reference_price DOUBLE PRECISION,
    stop_loss_price    DOUBLE PRECISION,
    take_profit_json   TEXT,
    risk_rules_json    TEXT,
    scanner_rules_fingerprint VARCHAR(32) NOT NULL DEFAULT '',
    evaluation_source  VARCHAR(16)  NOT NULL DEFAULT 'FRESH',
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_quant_signal_snapshot_batch ON quant_signal_snapshot(batch_id);
CREATE INDEX IF NOT EXISTS idx_quant_signal_snapshot_symbol ON quant_signal_snapshot(symbol);
CREATE INDEX IF NOT EXISTS idx_quant_signal_snapshot_created ON quant_signal_snapshot(created_at DESC);

CREATE TABLE IF NOT EXISTS quant_signal_scan_all (
    id                BIGSERIAL PRIMARY KEY,
    batch_id           VARCHAR(64)  NOT NULL,
    symbol             VARCHAR(32)  NOT NULL,
    symbol_name        VARCHAR(128) NOT NULL DEFAULT '',
    technical_score    DOUBLE PRECISION NOT NULL DEFAULT 0,
    strategy_source    VARCHAR(16)  NOT NULL DEFAULT 'UNSPECIFIED',
    sector_strength    DOUBLE PRECISION NOT NULL DEFAULT 0,
    trend_score        DOUBLE PRECISION NOT NULL DEFAULT 0,
    reversion_score    DOUBLE PRECISION NOT NULL DEFAULT 0,
    breakout_score     DOUBLE PRECISION NOT NULL DEFAULT 0,
    momentum_score     DOUBLE PRECISION NOT NULL DEFAULT 0,
    technical_score_percentile DOUBLE PRECISION,
    passed             BOOLEAN       NOT NULL DEFAULT FALSE,
    long_term_score    DOUBLE PRECISION,
    long_term_candidate BOOLEAN NOT NULL DEFAULT FALSE,
    correlation_id     VARCHAR(64)  NOT NULL DEFAULT '',
    signal_tier        VARCHAR(16) NOT NULL DEFAULT '',
    alert_passed       BOOLEAN NOT NULL DEFAULT FALSE,
    confirmed_passed   BOOLEAN NOT NULL DEFAULT FALSE,
    entry_reference_price DOUBLE PRECISION,
    stop_loss_price    DOUBLE PRECISION,
    take_profit_json   TEXT,
    risk_rules_json    TEXT,
    scanner_rules_fingerprint VARCHAR(32) NOT NULL DEFAULT '',
    evaluation_source  VARCHAR(16)  NOT NULL DEFAULT 'FRESH',
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_quant_signal_scan_all_batch ON quant_signal_scan_all(batch_id);
CREATE INDEX IF NOT EXISTS idx_quant_signal_scan_all_symbol ON quant_signal_scan_all(symbol);
CREATE INDEX IF NOT EXISTS idx_quant_signal_scan_all_passed ON quant_signal_scan_all(passed);
CREATE INDEX IF NOT EXISTS idx_quant_signal_scan_all_created ON quant_signal_scan_all(created_at DESC);
"""

# 已有表补列 symbol_name、各池得分（PostgreSQL 9.5+）
ALTER_EXTRA = """
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS symbol_name VARCHAR(128) NOT NULL DEFAULT '';
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS symbol_name VARCHAR(128) NOT NULL DEFAULT '';
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS trend_score DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS reversion_score DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS breakout_score DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS momentum_score DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS trend_score DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS reversion_score DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS breakout_score DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS momentum_score DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS technical_score_percentile DOUBLE PRECISION;
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS technical_score_percentile DOUBLE PRECISION;
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS long_term_score DOUBLE PRECISION;
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS long_term_candidate BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS long_term_score DOUBLE PRECISION;
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS long_term_candidate BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS signal_tier VARCHAR(16) NOT NULL DEFAULT '';
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS alert_passed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS confirmed_passed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS entry_reference_price DOUBLE PRECISION;
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS stop_loss_price DOUBLE PRECISION;
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS take_profit_json TEXT;
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS risk_rules_json TEXT;
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS signal_tier VARCHAR(16) NOT NULL DEFAULT '';
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS alert_passed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS confirmed_passed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS entry_reference_price DOUBLE PRECISION;
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS stop_loss_price DOUBLE PRECISION;
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS take_profit_json TEXT;
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS risk_rules_json TEXT;
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
UPDATE quant_signal_scan_all SET updated_at = created_at WHERE updated_at IS NULL;
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS scanner_rules_fingerprint VARCHAR(32) NOT NULL DEFAULT '';
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS evaluation_source VARCHAR(16) NOT NULL DEFAULT 'FRESH';
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS scanner_rules_fingerprint VARCHAR(32) NOT NULL DEFAULT '';
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS evaluation_source VARCHAR(16) NOT NULL DEFAULT 'FRESH';
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS scan_input_ohlcv_max_ts TIMESTAMPTZ;
ALTER TABLE quant_signal_snapshot ADD COLUMN IF NOT EXISTS scan_input_news_max_ts TIMESTAMPTZ;
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS scan_input_ohlcv_max_ts TIMESTAMPTZ;
ALTER TABLE quant_signal_scan_all ADD COLUMN IF NOT EXISTS scan_input_news_max_ts TIMESTAMPTZ;
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
        for stmt in ALTER_EXTRA.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                try:
                    cur.execute(stmt)
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        raise
        cur.close()
        conn.close()
        print("L2 表 quant_signal_snapshot 与 quant_signal_scan_all 已就绪（已存在或已创建）。")
    except Exception as e:
        err = str(e).strip()
        print("创建表失败: %s" % err)
        if "Connection refused" in err or "could not connect" in err.lower():
            print()
            print("说明: 当前 .env 中 PG_L2_DSN 指向的地址不可达。")
        sys.exit(1)


if __name__ == "__main__":
    main()
