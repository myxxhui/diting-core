#!/usr/bin/env python3
# [Ref: 12_右脑数据支撑与Segment规约] L2 建表：segment_registry、symbol_business_profile
# 用法：make init-l2-business-profile-tables 或 PYTHONPATH=. python3 scripts/init_l2_business_profile_tables.py
# DDL 与 diting-infra/schemas/sql/09_*.sql、10_*.sql 一致；含对旧库 segment_registry 列的幂等 ALTER

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
CREATE TABLE IF NOT EXISTS segment_registry (
    segment_id   VARCHAR(64) PRIMARY KEY,
    domain       VARCHAR(32)  NOT NULL DEFAULT '宏观',
    sub_domain   VARCHAR(64)  DEFAULT NULL,
    segment_tier SMALLINT      DEFAULT NULL,
    name_cn      VARCHAR(256) NOT NULL DEFAULT '',
    signal_adapter_id VARCHAR(64) DEFAULT NULL,
    signal_refresh_ttl_sec INT DEFAULT NULL,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_segment_registry_domain ON segment_registry(domain);

CREATE TABLE IF NOT EXISTS symbol_business_profile (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(32)  NOT NULL,
    segment_id      VARCHAR(64)  NOT NULL,
    segment_label_cn VARCHAR(256) NOT NULL DEFAULT '',
    revenue_share   DOUBLE PRECISION NOT NULL DEFAULT 0,
    is_primary      BOOLEAN NOT NULL DEFAULT FALSE,
    report_date     VARCHAR(32)  DEFAULT NULL,
    source          VARCHAR(32)  NOT NULL DEFAULT 'akshare_zygc',
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE(symbol, segment_id)
);
CREATE INDEX IF NOT EXISTS idx_symbol_business_profile_symbol ON symbol_business_profile(symbol);
CREATE INDEX IF NOT EXISTS idx_symbol_business_profile_updated ON symbol_business_profile(updated_at DESC);
"""

# 旧库仅有 segment_id/domain/name_cn/updated_at 时补齐列（PostgreSQL 11+ IF NOT EXISTS）。
# idx_segment_registry_tier 必须放在 ALTER 之后：旧库上 CREATE TABLE IF NOT EXISTS 不会加列，先建索引会报 column 不存在。
MIGRATE_SEGMENT_REGISTRY_ALTER = """
ALTER TABLE segment_registry ADD COLUMN IF NOT EXISTS sub_domain VARCHAR(64) DEFAULT NULL;
ALTER TABLE segment_registry ADD COLUMN IF NOT EXISTS segment_tier SMALLINT DEFAULT NULL;
ALTER TABLE segment_registry ADD COLUMN IF NOT EXISTS signal_adapter_id VARCHAR(64) DEFAULT NULL;
ALTER TABLE segment_registry ADD COLUMN IF NOT EXISTS signal_refresh_ttl_sec INT DEFAULT NULL;
CREATE INDEX IF NOT EXISTS idx_segment_registry_tier ON segment_registry(segment_tier) WHERE segment_tier IS NOT NULL;
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
        for stmt in MIGRATE_SEGMENT_REGISTRY_ALTER.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        # 主营披露哈希 id 缺省补 L3
        cur.execute(
            """
            UPDATE segment_registry SET segment_tier = 3
            WHERE segment_tier IS NULL AND segment_id LIKE 'seg_bp_%%'
            """
        )
        cur.close()
        conn.close()
        print("L2 表 segment_registry / symbol_business_profile 已就绪。")
    except Exception as e:
        print("建表失败: %s" % e, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
