#!/usr/bin/env python3
# 诊断：采集写入的 L1 与验证/Module A 使用的 DSN 与表是否一致；并列出 ohlcv 表中实际存在的 symbol 样本。
# 工作目录: diting-core；与 run_ingest_production、verify_production_data_evidence 一致从 .env 读 DSN。

import os
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
env_file = root / ".env"
if env_file.exists():
    with open(env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and os.environ.get(k) is None:
                    os.environ[k] = v

# 与 ingest 完全一致：通过 config 取 DSN（确保与写入端同源）
sys.path.insert(0, str(root))
from diting.ingestion.config import get_timescale_dsn, get_pg_l2_dsn

def main():
    import psycopg2
    dsn_l1 = get_timescale_dsn()
    dsn_l2 = get_pg_l2_dsn()
    # 脱敏打印
    def _mask(s):
        if not s or "@" not in s:
            return "(未设置)" if not s else "***"
        pre, _, rest = s.rpartition("@")
        return pre.split(":")[0] + ":****@" + rest.split("/")[0] if "/" in rest else pre + "@***"
    print("=== DSN 来源（与 ingest 一致：config.get_*_dsn）===")
    print("L1 TIMESCALE_DSN:", _mask(dsn_l1))
    print("L2 PG_L2_DSN:    ", _mask(dsn_l2))
    print()

    print("=== L1 表 ohlcv：全表概况（不按 27 标过滤）===")
    try:
        conn = psycopg2.connect(dsn_l1)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ohlcv")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT symbol) FROM ohlcv")
        ndistinct = cur.fetchone()[0]
        print("总行数:", total, "| 不同 symbol 数:", ndistinct)
        cur.execute("SELECT symbol, COUNT(*) AS cnt FROM ohlcv GROUP BY symbol ORDER BY symbol LIMIT 35")
        rows = cur.fetchall()
        print("前 35 个 symbol 及行数（检查写入格式）:")
        for sym, cnt in rows:
            print(" ", repr(sym), "->", cnt)
        # 明确查 27 标中的几个
        for test in ["002371.SZ", "002371.sz", "600879.SH"]:
            cur.execute("SELECT COUNT(*) FROM ohlcv WHERE symbol = %s", (test,))
            n = cur.fetchone()[0]
            print("  [样本] symbol = %s 行数: %s" % (repr(test), n))
        conn.close()
    except Exception as e:
        print("L1 查询异常:", e)
        return 1
    print()

    print("=== L2 表 industry_revenue_summary：与 Module A 读取表一致 ===")
    try:
        conn = psycopg2.connect(dsn_l2)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM industry_revenue_summary")
        n = cur.fetchone()[0]
        print("总行数:", n)
        cur.execute("SELECT symbol, industry_name FROM industry_revenue_summary WHERE symbol IN ('002371.SZ','600879.SH') ORDER BY symbol")
        for row in cur.fetchall():
            print(" ", row[0], "industry_name=", repr(row[1]))
        conn.close()
    except Exception as e:
        print("L2 查询异常:", e)
    return 0

if __name__ == "__main__":
    sys.exit(main())
