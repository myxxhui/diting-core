#!/usr/bin/env python3
# [Ref: 02_量化扫描引擎_实践] 一键查询 B 模块写入的数据：L2 表 quant_signal_snapshot
# 用法：在 diting-core 根目录 make query-module-b-output 或 PYTHONPATH=. python3 scripts/query_scanner_output.py
# 时间输出为 UTF-8 字符串（北京时间 UTC+8）

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


def _format_time_utf8(created):
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
        # 通过表（供 Module C）
        cur.execute("""
            SELECT batch_id, COUNT(*), MAX(created_at)
            FROM quant_signal_snapshot
            GROUP BY batch_id
            ORDER BY MAX(created_at) DESC
            LIMIT 5
        """)
        rows = cur.fetchall()
        print()
        print("======== B 模块写入数据 ========  ")
        print("  【通过表 quant_signal_snapshot】供 Module C 使用")
        if not rows:
            print("  表为空或不存在（请先执行 make run-module-b 并确保 PG_L2_DSN 可达；表未创建时先执行 make init-l2-quant-signal-table）。")
        else:
            print("  最近批次汇总:")
            for batch_id, cnt, created in rows:
                bid_short = (batch_id or "")[:32] + ".." if len(batch_id or "") > 34 else (batch_id or "")
                print("    batch_id=%s, 行数=%s, 最新写入=%s" % (bid_short, cnt, _format_time_utf8(created)))
            try:
                cur.execute("""
                    SELECT id, batch_id, symbol, COALESCE(symbol_name,''), technical_score, strategy_source, sector_strength,
                           COALESCE(trend_score,0), COALESCE(reversion_score,0), COALESCE(breakout_score,0), COALESCE(momentum_score,0),
                           COALESCE(technical_score_percentile, 0), created_at
                    FROM quant_signal_snapshot
                    ORDER BY created_at DESC
                    LIMIT 30
                """)
                detail = cur.fetchall()
                has_pool_scores = True
                has_name = True
                has_percentile = True
            except Exception:
                has_percentile = False
                try:
                    cur.execute("""
                        SELECT id, batch_id, symbol, COALESCE(symbol_name,''), technical_score, strategy_source, sector_strength, created_at
                        FROM quant_signal_snapshot
                        ORDER BY created_at DESC
                        LIMIT 30
                    """)
                    detail = cur.fetchall()
                    has_pool_scores = False
                    has_name = True
                except Exception:
                    cur.execute("""
                        SELECT id, batch_id, symbol, technical_score, strategy_source, sector_strength, created_at
                        FROM quant_signal_snapshot
                        ORDER BY created_at DESC
                        LIMIT 30
                    """)
                    detail = cur.fetchall()
                    has_pool_scores = False
                    has_name = False
                has_percentile = False
            print()
            print("  通过表最新明细（最多 30 条）：标的 | 中文名 | 总得分 | 策略 | 趋势 | 反转 | 突破 | 动量 | 分位")
            if has_pool_scores:
                print("    %-6s %-36s %-12s %-10s %8s %-10s %8s %8s %8s %8s %8s %6s %s" % ("id", "batch_id", "symbol", "symbol_name", "score", "strategy", "sector", "趋势", "反转", "突破", "动量", "分位", "created_at"))
                print("    " + "-" * 150)
                for r in detail:
                    if has_percentile and len(r) >= 13:
                        rid, bid, sym, sym_name, score, src, sector, t, rv, b, m, pct, created = r
                    else:
                        rid, bid, sym, sym_name, score, src, sector, t, rv, b, m, created = r[:12]
                        pct = 0.0
                    bid_short = (bid or "")[:32] + ".." if len(bid or "") > 34 else (bid or "")
                    print("    %-6s %-36s %-12s %-10s %8.2f %-10s %8.2f %8.2f %8.2f %8.2f %8.2f %6.2f %s" % (rid, bid_short, sym or "", (sym_name or "")[:8], score or 0, (src or "")[:8], sector or 0, t or 0, rv or 0, b or 0, m or 0, pct, _format_time_utf8(created)))
            else:
                print("    %-6s %-36s %-12s %-14s %8s %-16s %8s %s" % ("id", "batch_id", "symbol", "symbol_name", "score", "strategy_source", "sector", "created_at"))
                print("    " + "-" * 120)
                for r in detail:
                    if has_name:
                        rid, bid, sym, sym_name, score, src, sector, created = r
                    else:
                        rid, bid, sym, score, src, sector, created = r
                        sym_name = ""
                    bid_short = (bid or "")[:32] + ".." if len(bid or "") > 34 else (bid or "")
                    print("    %-6s %-36s %-12s %-14s %8.2f %-16s %8.2f %s" % (rid, bid_short, sym or "", (sym_name or "")[:12], score or 0, (src or "")[:14], sector or 0, _format_time_utf8(created)))

        # 全量表（通过/未通过分开可查，当前分数）
        try:
            cur.execute("""
                SELECT batch_id, COUNT(*), SUM(CASE WHEN passed THEN 1 ELSE 0 END), MAX(created_at)
                FROM quant_signal_scan_all
                GROUP BY batch_id
                ORDER BY MAX(created_at) DESC
                LIMIT 5
            """)
            all_rows = cur.fetchall()
            if all_rows:
                print()
                print("  【全量表 quant_signal_scan_all】通过/未通过分开存放，可查当前分数")
                print("  最近批次: 全量条数 | 通过条数 | 最新写入")
                for batch_id, total, passed_cnt, created in all_rows:
                    bid_short = (batch_id or "")[:32] + ".." if len(batch_id or "") > 34 else (batch_id or "")
                    print("    batch_id=%s, 全量=%s, 通过=%s, %s" % (bid_short, total, passed_cnt, _format_time_utf8(created)))
                try:
                    cur.execute("""
                        SELECT symbol, COALESCE(symbol_name,''), technical_score, strategy_source,
                               COALESCE(trend_score,0), COALESCE(reversion_score,0), COALESCE(breakout_score,0), COALESCE(momentum_score,0),
                               COALESCE(technical_score_percentile, 0), passed, created_at
                        FROM quant_signal_scan_all
                        ORDER BY created_at DESC, technical_score DESC
                        LIMIT 30
                    """)
                    scan_detail = cur.fetchall()
                    scan_has_pool = True
                    scan_has_name = True
                    scan_has_percentile = True
                except Exception:
                    scan_has_percentile = False
                    try:
                        cur.execute("""
                            SELECT symbol, COALESCE(symbol_name,''), technical_score, strategy_source, passed, created_at
                            FROM quant_signal_scan_all
                            ORDER BY created_at DESC, technical_score DESC
                            LIMIT 30
                        """)
                        scan_detail = cur.fetchall()
                        scan_has_pool = False
                        scan_has_name = True
                    except Exception:
                        cur.execute("""
                            SELECT symbol, technical_score, strategy_source, passed, created_at
                            FROM quant_signal_scan_all
                            ORDER BY created_at DESC, technical_score DESC
                            LIMIT 30
                        """)
                        scan_detail = cur.fetchall()
                        scan_has_pool = False
                        scan_has_name = False
                    scan_has_percentile = False
                print("  全量表最新 30 条（标的 | 中文名 | 总得分 | 策略 | 趋势 | 反转 | 突破 | 动量 | 分位 | 通过）:")
                if scan_has_pool:
                    print("    %-12s %-10s %8s %-10s %8s %8s %8s %8s %6s %6s %s" % ("symbol", "symbol_name", "score", "strategy", "趋势", "反转", "突破", "动量", "分位", "passed", "created_at"))
                    print("    " + "-" * 120)
                    for row in scan_detail:
                        if scan_has_percentile and len(row) >= 11:
                            sym, sym_name, score, src, t, rv, b, m, pct, passed, created = row
                        else:
                            sym, sym_name, score, src, t, rv, b, m, passed, created = row[:10]
                            pct = 0.0
                        print("    %-12s %-10s %8.2f %-10s %8.2f %8.2f %8.2f %8.2f %6.2f %6s %s" % (sym or "", (sym_name or "")[:8], score or 0, (src or "")[:8], t or 0, rv or 0, b or 0, m or 0, pct, passed, _format_time_utf8(created)))
                else:
                    print("    %-12s %-14s %8s %-16s %6s %s" % ("symbol", "symbol_name", "score", "strategy_source", "passed", "created_at"))
                    print("    " + "-" * 90)
                    for row in scan_detail:
                        if scan_has_name:
                            sym, sym_name, score, src, passed, created = row
                        else:
                            sym, score, src, passed, created = row
                            sym_name = ""
                        print("    %-12s %-14s %8.2f %-16s %6s %s" % (sym or "", (sym_name or "")[:12], score or 0, (src or "")[:14], passed, _format_time_utf8(created)))
        except Exception as e:
            if "quant_signal_scan_all" in str(e) or "does not exist" in str(e).lower():
                print()
                print("  全量表 quant_signal_scan_all 未创建，请先执行 make init-l2-quant-signal-table 后重新 run-module-b。")
            else:
                raise
        cur.close()
        conn.close()
        print()
    except Exception as e:
        print()
        print("======== B 模块写入数据（L2 表 quant_signal_snapshot）========  ")
        print("  查询失败（PG_L2_DSN 不可达或表不存在）: %s" % e)
        print("  请确保 L2 已启动、.env 中 PG_L2_DSN 正确，先执行 make init-l2-quant-signal-table 与 make run-module-b 后再查询。")
        print()
        sys.exit(0)


if __name__ == "__main__":
    main()
