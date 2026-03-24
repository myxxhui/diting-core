#!/usr/bin/env python3
# [Ref: 06_B轨_信号层_1对1对1] 全链路运行结果查询：从 L2 汇总 A/B/信号层/C 最近批次与行数，便于排查。
#
# 用法：make query-full-pipeline-result 或 PYTHONPATH=. python3 scripts/query_full_pipeline_result.py

import json
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


def _safe_count(cur, sql: str, params=None) -> int:
    try:
        cur.execute(sql, params or ())
        r = cur.fetchone()
        return int(r[0] or 0) if r else 0
    except Exception:
        return -1


def main() -> int:
    dsn = (os.environ.get("PG_L2_DSN") or "").strip()
    if not dsn:
        print("未配置 PG_L2_DSN。", file=sys.stderr)
        return 1
    try:
        import psycopg2
    except ImportError:
        print("未安装 psycopg2。", file=sys.stderr)
        return 1

    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    def exists(table: str) -> bool:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            )
            """,
            (table,),
        )
        return bool(cur.fetchone()[0])

    print()
    print("======== 全链路运行结果查询（L2）========  ")
    print("  说明: A→B→信号层 refresh→C 各表最近写入摘要；与 make run-full-pipeline 对照排查。")
    print()

    # --- Module A ---
    if exists("classifier_output_snapshot"):
        cur.execute(
            """
            SELECT batch_id, COUNT(*), MAX(created_at)
            FROM classifier_output_snapshot
            GROUP BY batch_id
            ORDER BY MAX(created_at) DESC NULLS LAST
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row and row[0]:
            print("【Module A】classifier_output_snapshot")
            print("  最近 batch_id: %s" % row[0])
            print("  行数: %s  最新时间: %s" % (row[1], row[2]))
        else:
            print("【Module A】classifier_output_snapshot 表空")
    else:
        print("【Module A】无表 classifier_output_snapshot")
    print()

    # --- Module B ---
    _b_tables = (
        ("quant_signal_snapshot", "quant_signal_snapshot（确认∪预警，供 C）"),
        ("quant_signal_scan_all", "quant_signal_scan_all（全量打分）"),
    )
    for tname, label in _b_tables:
        if exists(tname):
            cur.execute(
                """
                SELECT batch_id, COUNT(*), MAX(created_at)
                FROM """
                + tname
                + """
                GROUP BY batch_id
                ORDER BY MAX(created_at) DESC NULLS LAST
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if row and row[0]:
                print("【Module B】%s" % label)
                print("  最近 batch_id: %s" % row[0])
                print("  行数: %s  最新时间: %s" % (row[1], row[2]))
            else:
                print("【Module B】%s 表空" % label)
        else:
            print("【Module B】无表 %s" % tname)
        print()

    # --- 信号层 ---
    if exists("segment_signal_cache"):
        n = _safe_count(cur, "SELECT COUNT(*) FROM segment_signal_cache")
        cur.execute("SELECT MAX(fetched_at) FROM segment_signal_cache")
        mx = cur.fetchone()[0]
        print("【信号层】segment_signal_cache")
        print("  总行数: %s  最近 fetched_at: %s" % (n, mx))
    else:
        print("【信号层】无表 segment_signal_cache")
    print()

    # --- Module C ---
    if exists("moe_expert_opinion_snapshot"):
        cur.execute(
            """
            SELECT batch_id, COUNT(*), MAX(created_at)
            FROM moe_expert_opinion_snapshot
            GROUP BY batch_id
            ORDER BY MAX(created_at) DESC NULLS LAST
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row and row[0]:
            bid = row[0]
            print("【Module C】moe_expert_opinion_snapshot")
            print("  最近 batch_id: %s" % bid)
            print("  行数: %s  最新时间: %s" % (row[1], row[2]))
            try:
                cur.execute(
                    "SELECT moe_run_metadata FROM moe_expert_opinion_snapshot WHERE batch_id = %s LIMIT 1",
                    (bid,),
                )
                mr = cur.fetchone()
                if mr and mr[0]:
                    meta = mr[0] if isinstance(mr[0], dict) else json.loads(mr[0]) if mr[0] else {}
                    if isinstance(meta, dict) and meta:
                        print("  moe_run_metadata（摘要）:")
                        for k in (
                            "stub_segment_signals",
                            "classifier_batch_id",
                            "quant_batch_id",
                            "processed_symbols",
                            "universe_symbols",
                            "alignment_warnings",
                        ):
                            if k in meta:
                                v = meta[k]
                                if k == "alignment_warnings" and isinstance(v, list):
                                    print("    %s: %s" % (k, v[:5]))
                                else:
                                    print("    %s: %s" % (k, v))
            except Exception as e:
                print("  （读取 moe_run_metadata 失败: %s）" % e)
        else:
            print("【Module C】moe_expert_opinion_snapshot 表空")
    else:
        print("【Module C】无表 moe_expert_opinion_snapshot（可先 make init-l2-moe-opinion-table）")

    print()
    print("======== 结束 ========")
    print()

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
