#!/usr/bin/env python3
# [Ref: 11_数据采集与输入层规约] 在 L2 库中创建 industry_revenue_summary（若不存在）
# 用法：make init-l2-industry-revenue-table 或 PYTHONPATH=. python3 scripts/init_l2_industry_revenue_summary.py
# DDL 与 diting-infra/schemas/sql/03_l2_industry_revenue_summary.sql 一致；采集任务写入数据前须先有表

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

# 与 diting-infra/schemas/sql/03_l2_industry_revenue_summary.sql 一致
DDL = """
CREATE TABLE IF NOT EXISTS industry_revenue_summary (
    symbol          VARCHAR(32) PRIMARY KEY,
    industry_name   VARCHAR(128) NOT NULL DEFAULT '',
    revenue_ratio   DOUBLE PRECISION NOT NULL DEFAULT 0,
    rnd_ratio       DOUBLE PRECISION NOT NULL DEFAULT 0,
    commodity_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_industry_revenue_summary_updated ON industry_revenue_summary(updated_at);
COMMENT ON TABLE industry_revenue_summary IS 'Module A 输入：每标的行业名与营收/研发/大宗占比，由采集写入';
"""


def main() -> int:
    dsn = os.environ.get("PG_L2_DSN", "").strip()
    if not dsn:
        print("未配置 PG_L2_DSN。请在 .env 中配置 PG_L2_DSN。", file=sys.stderr)
        return 1
    try:
        import psycopg2
    except ImportError:
        print("未安装 psycopg2。请执行: pip install psycopg2-binary", file=sys.stderr)
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
        print("L2 表 industry_revenue_summary 已就绪（已存在或已创建）。")
        return 0
    except Exception as e:
        err = str(e).strip()
        print("创建表失败: %s" % err)
        if "Connection refused" in err or "could not connect" in err.lower():
            print()
            print("说明: 当前 .env 中 PG_L2_DSN 指向的地址不可达。")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
