#!/usr/bin/env python3
# [Ref: 04_A轨_MoE议会_实践] 一键查询 Module C 写入：L2 表 moe_expert_opinion_snapshot
# 用法：make query-module-c-output 或 PYTHONPATH=. python3 scripts/query_module_c_output.py

import json
import os
import sys
from pathlib import Path

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


def main():
    dsn = os.environ.get("PG_L2_DSN", "").strip()
    if not dsn:
        print("未配置 PG_L2_DSN。", file=sys.stderr)
        sys.exit(1)
    try:
        import psycopg2
    except ImportError:
        print("未安装 psycopg2。", file=sys.stderr)
        sys.exit(1)

    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'moe_expert_opinion_snapshot'
            )
            """
        )
        if not cur.fetchone()[0]:
            print()
            print("======== Module C（moe_expert_opinion_snapshot）========  ")
            print("  表不存在。请先: make init-l2-moe-opinion-table")
            print()
            cur.close()
            conn.close()
            return
        cur.execute(
            """
            SELECT batch_id, COUNT(*), MAX(created_at)
            FROM moe_expert_opinion_snapshot
            GROUP BY batch_id
            ORDER BY MAX(created_at) DESC
            LIMIT 8
            """
        )
        batches = cur.fetchall()
        print()
        print("======== Module C 写入数据（L2 moe_expert_opinion_snapshot）========  ")
        if not batches:
            print("  表为空。请先 make run-module-c")
            print()
            cur.close()
            conn.close()
            return
        print("  最近批次:")
        for bid, cnt, created in batches:
            stub_h = ""
            try:
                cur.execute(
                    "SELECT moe_run_metadata FROM moe_expert_opinion_snapshot WHERE batch_id = %s LIMIT 1",
                    (bid,),
                )
                mr = cur.fetchone()
                if mr and mr[0]:
                    meta = mr[0] if isinstance(mr[0], dict) else {}
                    if not isinstance(meta, dict):
                        meta = json.loads(meta) if meta else {}
                    if meta.get("stub_segment_signals"):
                        stub_h = " stub=联调"
                    elif "stub_segment_signals" in meta:
                        stub_h = " stub=关"
            except Exception:
                pass
            print(
                "    batch_id=%s 行数=%s 最新=%s%s"
                % (bid[:40] + ("..." if len(bid) > 40 else ""), cnt, created, stub_h)
            )
        cur.execute(
            """
            SELECT symbol, opinions_json, batch_id, created_at
            FROM moe_expert_opinion_snapshot
            ORDER BY created_at DESC
            LIMIT 25
            """
        )
        rows = cur.fetchall()
        print()
        print("  最新 25 条明细（symbol | is_supported | horizon | confidence | 摘要前 40 字）:")
        for sym, oj, bid, created in rows:
            try:
                arr = oj if isinstance(oj, list) else json.loads(oj) if oj else []
            except Exception:
                arr = []
            for i, op in enumerate(arr):
                summ = (op.get("reasoning_summary") or "")[:40]
                print(
                    "    %s [%s] supported=%s horizon=%s conf=%.2f | %s"
                    % (
                        sym,
                        i,
                        op.get("is_supported"),
                        op.get("horizon"),
                        float(op.get("confidence") or 0),
                        summ,
                    )
                )
        cur.close()
        conn.close()
        print()
    except Exception as e:
        print("查询失败: %s" % e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
