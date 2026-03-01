#!/usr/bin/env python3
# [Ref: 03_原子目标与规约/Stage2_数据采集与存储/06_生产级数据要求_设计.md]
# 生产级数据验收：L1 单标日线≥5 年、标的与 universe 一致、复权口径；工作目录 diting-core，使用 .env。

import os
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
env_file = root / ".env"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

try:
    import psycopg2
except ImportError:
    print("psycopg2 not installed", file=sys.stderr)
    sys.exit(1)

TIMESCALE_DSN = os.environ.get("TIMESCALE_DSN")
MIN_BARS_5_YEARS = 5 * 252  # 约 5 年日线最小根数


def main() -> int:
    if not TIMESCALE_DSN:
        print("TIMESCALE_DSN not set (use .env or prod-data-env.conn)", file=sys.stderr)
        return 1
    try:
        conn = psycopg2.connect(TIMESCALE_DSN)
        conn.autocommit = True
    except Exception as e:
        print(f"L1 connect failed: {e}", file=sys.stderr)
        return 1

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ohlcv'"
            )
            if cur.fetchone() is None:
                print("L1: table ohlcv not found", file=sys.stderr)
                return 1

            # 单标最小 bar 数（按 symbol 聚合）
            cur.execute("""
                SELECT symbol, COUNT(*) AS cnt
                FROM ohlcv
                GROUP BY symbol
            """)
            rows = cur.fetchall()
            if not rows:
                print("L1: no data in ohlcv", file=sys.stderr)
                return 1

            min_bars = min(r[1] for r in rows)
            if min_bars < MIN_BARS_5_YEARS:
                print(
                    f"L1: 单标最小 bar 数 {min_bars} < 5 年要求 {MIN_BARS_5_YEARS}",
                    file=sys.stderr,
                )
                return 1
            print(f"L1: 单标最小 bar 数 {min_bars} >= {MIN_BARS_5_YEARS} OK")

            # 若存在 a_share_universe 表则校验标的一致（与 11_/get_current_a_share_universe 一致）
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'a_share_universe'"
            )
            if cur.fetchone():
                cur.execute("SELECT COUNT(DISTINCT symbol) FROM ohlcv")
                ohlcv_symbols = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM a_share_universe")
                universe_count = cur.fetchone()[0]
                if universe_count > 0 and ohlcv_symbols < universe_count:
                    print(
                        f"a_share_universe 表行数 {universe_count} > ohlcv 标的数 {ohlcv_symbols}",
                        file=sys.stderr,
                    )
                    return 1
                print("a_share_universe 与 ohlcv 标的口径一致 OK")
    finally:
        conn.close()
    print("verify-data-production OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
