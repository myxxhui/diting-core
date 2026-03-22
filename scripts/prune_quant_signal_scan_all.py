#!/usr/bin/env python3
# [Ref: 02_量化扫描引擎_实践] 按保留天数裁剪 L2 quant_signal_scan_all，控制表膨胀（运维/定时任务）

from __future__ import annotations

import argparse
import os
import sys


def main() -> int:
    p = argparse.ArgumentParser(description="删除 quant_signal_scan_all 中早于 N 天的行")
    p.add_argument("--days", type=int, default=90, help="保留最近 N 天（按 created_at）")
    p.add_argument("--dry-run", action="store_true", help="仅打印将删除行数，不执行 DELETE")
    args = p.parse_args()
    dsn = (os.environ.get("PG_L2_DSN") or "").strip()
    if not dsn:
        print("未配置 PG_L2_DSN", file=sys.stderr)
        return 1
    if args.days < 1:
        print("--days 须 >= 1", file=sys.stderr)
        return 1
    try:
        import psycopg2
    except ImportError:
        print("需要 psycopg2", file=sys.stderr)
        return 1
    try:
        conn = psycopg2.connect(dsn, connect_timeout=20)
        cur = conn.cursor()
        cur.execute(
            """SELECT COUNT(*) FROM quant_signal_scan_all
               WHERE created_at < NOW() - make_interval(days => %s)""",
            (args.days,),
        )
        n = cur.fetchone()[0]
        print("quant_signal_scan_all: 早于 %s 天的行数 = %s" % (args.days, n))
        if args.dry_run:
            conn.rollback()
            cur.close()
            conn.close()
            print("dry-run：未删除")
            return 0
        cur.execute(
            """DELETE FROM quant_signal_scan_all
               WHERE created_at < NOW() - make_interval(days => %s)""",
            (args.days,),
        )
        conn.commit()
        print("已删除 %s 行" % cur.rowcount)
        cur.close()
        conn.close()
        return 0
    except Exception as e:
        print("失败: %s" % e, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
