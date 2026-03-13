#!/usr/bin/env python3
# [Ref: 01_语义分类器_实践] 一键查询 A 模块写入的数据：L2 表 classifier_output_snapshot
# 用法：在 diting-core 根目录 make query-module-a-output 或 PYTHONPATH=. python3 scripts/query_classifier_output.py
# 时间输出为 UTF-8 字符串（北京时间 UTC+8）

import os
import sys
from pathlib import Path

# 保证 stdout 使用 UTF-8
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

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


def _format_time_utf8(created):
    """将 created_at 格式化为 UTF-8 友好字符串（北京时间 UTC+8）。"""
    if created is None:
        return ""
    try:
        if getattr(created, "tzinfo", None):
            from datetime import timezone, timedelta
            utc8 = timezone(timedelta(hours=8))
            local = created.astimezone(utc8)
        else:
            local = created
        return local.strftime("%Y-%m-%d %H:%M:%S") + " (UTC+8)"
    except Exception:
        return str(created)


def main():
    dsn = os.environ.get("PG_L2_DSN", "").strip()
    if not dsn:
        print("未配置 PG_L2_DSN，无法查询 L2。请在 .env 中配置 PG_L2_DSN。", file=sys.stderr)
        sys.exit(1)
    try:
        import psycopg2
    except ImportError:
        print("未安装 psycopg2，无法查询。pip install psycopg2-binary", file=sys.stderr)
        sys.exit(1)

    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        # 按批次统计 + 最新一批明细
        cur.execute("""
            SELECT batch_id, COUNT(*), MAX(created_at)
            FROM classifier_output_snapshot
            GROUP BY batch_id
            ORDER BY MAX(created_at) DESC
            LIMIT 5
        """)
        rows = cur.fetchall()
        print()
        print("======== A 模块写入数据（L2 表 classifier_output_snapshot）========  ")
        if not rows:
            print("  表为空或不存在（请先执行 make run-module-a 并确保 PG_L2_DSN 可达）。")
            cur.close()
            conn.close()
            sys.exit(0)
        print("  最近批次汇总:")
        for batch_id, cnt, created in rows:
            print("    batch_id=%s, 行数=%s, 最新写入=%s" % (batch_id, cnt, _format_time_utf8(created)))
        # 最新一批的明细（最多 30 条）
        cur.execute("""
            SELECT id, batch_id, symbol, primary_tag, primary_confidence, created_at
            FROM classifier_output_snapshot
            ORDER BY created_at DESC
            LIMIT 30
        """)
        detail = cur.fetchall()
        print()
        print("  最新写入明细（最多 30 条）:")
        print("    %-6s %-36s %-12s %-8s %-6s %s" % ("id", "batch_id", "symbol", "primary_tag", "conf", "created_at"))
        print("    " + "-" * 90)
        for r in detail:
            rid, bid, sym, tag, conf, created = r
            bid_short = (bid or "")[:32] + ".." if len(bid or "") > 34 else (bid or "")
            print("    %-6s %-36s %-12s %-8s %.2f   %s" % (rid, bid_short, sym or "", tag or "", conf or 0, _format_time_utf8(created)))
        cur.close()
        conn.close()
        print()
    except Exception as e:
        print()
        print("======== A 模块写入数据（L2 表 classifier_output_snapshot）========  ")
        print("  查询失败（PG_L2_DSN 不可达或表不存在）: %s" % e)
        print("  请确保 L2 已启动且 .env 中 PG_L2_DSN 正确，先执行 make run-module-a 写入后再查询。")
        print()
        sys.exit(0)


if __name__ == "__main__":
    main()
