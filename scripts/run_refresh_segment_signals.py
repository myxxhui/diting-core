#!/usr/bin/env python3
# [Ref: 06_B轨_信号层生产级数据采集_实践] 信号层 refresh 入口
# 用法：make refresh-segment-signals 或 python scripts/run_refresh_segment_signals.py [symbols...]
# 从 L2 本批快照读 symbols，或从命令行传入；执行 refresh_segment_signals_for_symbols

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

_env = ROOT / ".env"
if _env.exists():
    with open(_env, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and os.environ.get(k) is None:
                    os.environ[k] = v


def _symbols_from_b_track(dsn: str) -> list:
    """从 b_track_candidate_snapshot 取最近一批 symbols（DITING_TRACK=b 时）。"""
    try:
        import psycopg2
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT symbol FROM b_track_candidate_snapshot
            WHERE batch_id = (SELECT batch_id FROM b_track_candidate_snapshot ORDER BY created_at DESC LIMIT 1)
            ORDER BY symbol
            """
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [str(r[0] or "").strip() for r in rows if r and (r[0] or "").strip()]
    except Exception:
        return []


def _symbols_from_quant_snapshot(dsn: str) -> list:
    """从 quant_signal_snapshot 取最近一批 symbols（A 轨默认）。"""
    try:
        import psycopg2
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT symbol FROM quant_signal_snapshot
            WHERE batch_id = (SELECT batch_id FROM quant_signal_snapshot ORDER BY created_at DESC LIMIT 1)
            ORDER BY symbol
            """
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [str(r[0] or "").strip() for r in rows if r and (r[0] or "").strip()]
    except Exception:
        return []


def main() -> int:
    dsn = (os.environ.get("PG_L2_DSN") or "").strip()
    if not dsn:
        print("未配置 PG_L2_DSN", file=sys.stderr)
        return 1
    track = (os.environ.get("DITING_TRACK") or "a").strip().lower()
    symbols = [s.strip().upper() for s in sys.argv[1:] if (s or "").strip()]
    if not symbols:
        if track == "b":
            symbols = _symbols_from_b_track(dsn)
            if symbols:
                print("DITING_TRACK=b: 从 b_track_candidate_snapshot 取 symbols=%d" % len(symbols))
        if not symbols:
            symbols = _symbols_from_quant_snapshot(dsn)
    if not symbols:
        print("无标的可 refresh；请传入 symbols 或先 make run-module-b（A 轨）或确保 b_track_candidate_snapshot 有数据（B 轨）", file=sys.stderr)
        return 1
    from diting.signal_layer import refresh_segment_signals_for_symbols, RefreshSegmentSignalsResult
    result = refresh_segment_signals_for_symbols(symbols, dsn, options={"track": track})
    assert isinstance(result, RefreshSegmentSignalsResult)
    print("refresh_segment_signals 完成")
    print("  symbols_without_segments: %s" % len(result.symbols_without_segments))
    print("  segments_without_adapter: %s" % len(result.segments_without_adapter))
    print("  segments_skipped_ttl: %s" % len(result.segments_skipped_ttl))
    print("  segments_written: %s" % len(result.segments_written))
    print("  segments_failed: %s" % len(result.segments_failed))
    print("  summary: %s" % result.summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
