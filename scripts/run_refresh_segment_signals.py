#!/usr/bin/env python3
# [Ref: 06_B轨_信号层生产级数据采集_实践] 信号层 refresh 入口
#
# · DITING_TRACK=b：细分全量 refresh——解析主营细分 → L2 拉取各细分新闻等 → 打标 → 写入 segment_signal_cache（Module C 右脑）。
# · DITING_TRACK=a（默认）：不写 segment_signal_cache；对「标的新闻 + 申万行业新闻」双路打标 → a_track_signal_cache；
#   并终端展示观测表（news_content 条数与领域/申万）；行业正文依赖 scope=industry 入库（见 07_ / migrate_l2_news_content_scope）。
#
# 用法：make refresh-segment-signals 或 python scripts/run_refresh_segment_signals.py [symbols...]
# REFRESH_SEGMENT_SCOPE：snapshot（默认）= 仅 B 模块通过/预警档；full = 全量 a_share_universe

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
    """从 quant_signal_snapshot 取最近一批 symbols（确认档∪预警档）。"""
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


def _symbols_from_universe() -> list:
    """全量标的（从 a_share_universe）。"""
    try:
        from diting.universe import get_current_a_share_universe
        return get_current_a_share_universe(force_refresh=False)
    except Exception:
        return []


def _load_symbol_names(symbols: list, dsn: str) -> dict:
    """标的中文名：优先 L2 symbol_names，再静态 CSV，不调远程。"""
    try:
        from diting.scanner.symbol_names import get_symbol_names
        return get_symbol_names(symbols, dsn=dsn, skip_akshare=True)
    except Exception:
        return {}


def _calibration_list_max() -> int:
    raw = (os.environ.get("PIPELINE_CALIBRATION_LIST_MAX") or "32").strip()
    try:
        n = int(raw, 10)
        return max(8, min(500, n))
    except ValueError:
        return 32


def _latest_quant_snapshot_meta(dsn: str):
    """L2 quant_signal_snapshot 最近一批：batch_id、行数、时间（与人眼核对 refresh 输入是否同批）。"""
    try:
        import psycopg2

        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT batch_id, COUNT(*), MAX(created_at)
            FROM quant_signal_snapshot
            GROUP BY batch_id
            ORDER BY MAX(created_at) DESC NULLS LAST
            LIMIT 1
            """
        )
        r = cur.fetchone()
        cur.close()
        conn.close()
        if r and r[0]:
            return str(r[0]), int(r[1] or 0), r[2]
    except Exception:
        pass
    return None, 0, None


def _run_a_track_branch(dsn: str, symbols: list, symbol_source: str) -> int:
    """A 轨：双路 refresh → a_track_signal_cache + 终端观测表。"""
    sym_set = sorted({str(s).strip().upper() for s in symbols if (s or "").strip()})
    name_map = _load_symbol_names(list(sym_set), dsn)

    def _name(sym: str) -> str:
        n = (name_map.get(sym) or "").strip()
        return n if n else "(未录名)"

    _pq = (os.environ.get("PIPELINE_QUIET") or "").strip().lower() in ("1", "true", "yes")

    try:
        _db = int((os.environ.get("REFRESH_SEGMENT_DAYS_BACK") or "7").strip() or "7")
    except ValueError:
        _db = 7
    try:
        _tmax = int((os.environ.get("PIPELINE_SEGMENT_TABLE_MAX") or "80").strip() or "80")
    except ValueError:
        _tmax = 80

    from diting.signal_layer.a_track_refresh import refresh_a_track_signals_for_symbols
    from diting.signal_layer.pipeline_report import print_a_track_symbol_news_summary

    print()
    if not _pq:
        print("=" * 60)
        print("A 轨 · 信号层（标的新闻+行业新闻 打标 → a_track_signal_cache）")
        print("=" * 60)
    else:
        from diting.pipeline_io import pipeline_frame_quiet

        pipeline_frame_quiet()
        print("======== 信号层（A 轨：双路打标 + 观测表）========")
    print("  [说明] DITING_TRACK=a：不写 segment_signal_cache；拉取标的新闻与申万行业新闻（L2 scope），"
          "仅已配置大模型时打标写入 a_track_signal_cache；未配置则跳过打标（见 .env SIGNAL_LAYER_*）。")
    print()

    cfg = None
    try:
        from pathlib import Path
        import yaml
        _p = Path(ROOT) / "config" / "signal_layer.yaml"
        if _p.exists():
            with open(_p, encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
    except Exception:
        cfg = None
    try:
        from diting.signal_layer.refresh import _build_understanding_config
        from diting.signal_layer.understanding.engine import is_llm_configured

        _bu = _build_understanding_config(cfg or {}, "a")
        print(
            "  大模型: %s"
            % (
                "已配置（可产生 LLM 打标/摘要）"
                if is_llm_configured(_bu)
                else "未配置（不会调用 API；各标的【AI打标】见下方观测表）"
            )
        )
        print()
    except Exception:
        print("  大模型: （无法检测配置）")
        print()
    try:
        at_res = refresh_a_track_signals_for_symbols(
            sym_set,
            dsn,
            config=cfg,
            options={
                "days_back": _db,
                "ttl_sec": int(((cfg or {}).get("signal_layer") or {}).get("ttl_sec") or 3600),
            },
        )
        print(
            "  A 轨打标: 标的写入 %s 只 | 行业写入 %s 个 | 标的跳过TTL %s | 行业跳过TTL %s | 标的失败 %s | 行业失败 %s"
            % (
                len(at_res.symbols_written),
                len(at_res.industries_written),
                len(at_res.symbols_skipped_ttl),
                len(at_res.industries_skipped_ttl),
                len(at_res.symbols_failed),
                len(at_res.industries_failed),
            )
        )
    except Exception as ex:
        print("  [警告] A 轨 refresh 异常: %s" % ex)

    print()
    print_a_track_symbol_news_summary(dsn, sym_set, days_back=_db, max_rows=_tmax)

    if _pq:
        print("  ── 人眼校准 · A 轨 ──")
        print("  · 本 run 标的来源: %s | 输入标的数=%d" % (symbol_source or "（未分类）", len(sym_set)))
        qb, qn, _qt = _latest_quant_snapshot_meta(dsn)
        if qb:
            print(
                "  · L2 quant_signal_snapshot 最近批: %s 行 | batch_id=%s（应与刚跑完的 B 一致）"
                % (qn, qb)
            )
        cap = _calibration_list_max()
        print("  · 本批标的（前 %s）---" % cap)
        for sym in sym_set[:cap]:
            print("    %s %s" % (sym, _name(sym)))
        if len(sym_set) > cap:
            print("    ... 共 %d 只，余下略" % len(sym_set))
        print("  ┌─ 信号层准出（A 轨）────────────────────────────────────────")
        print("  │ ① 本步写入 a_track_signal_cache（标的级 sym:* + 行业级 ind:*）；不写 segment_signal_cache。")
        print("  │ ② 细分垂直叙事：DITING_TRACK=b → segment_signal_cache。")
        print("  └──────────────────────────────────────────────────────────────")
        print("  ── 下一模块 Module C 依赖 ──")
        print("  · A 轨：C 合并 a_track_signal_cache 与 segment_signal_cache（若有）。")
    print()
    return 0


def main() -> int:
    dsn = (os.environ.get("PG_L2_DSN") or "").strip()
    if not dsn:
        print("未配置 PG_L2_DSN", file=sys.stderr)
        return 1

    scope = (os.environ.get("REFRESH_SEGMENT_SCOPE") or "snapshot").strip().lower()
    track = (os.environ.get("DITING_TRACK") or "a").strip().lower()
    symbols = [s.strip().upper() for s in sys.argv[1:] if (s or "").strip()]
    symbol_source = "命令行参数" if symbols else ""

    if not symbols:
        if scope == "full":
            symbols = _symbols_from_universe()
            if symbols:
                symbol_source = "a_share_universe（REFRESH_SEGMENT_SCOPE=full）"
                print("REFRESH_SEGMENT_SCOPE=full: 全量标的 %d 只" % len(symbols))
        if not symbols:
            if track == "b":
                symbols = _symbols_from_b_track(dsn)
                if symbols:
                    symbol_source = "b_track_candidate_snapshot（最近批）"
                    print("DITING_TRACK=b: 从 b_track_candidate_snapshot 取 B 档标的 %d 只" % len(symbols))
            if not symbols:
                symbols = _symbols_from_quant_snapshot(dsn)
                if symbols:
                    symbol_source = "quant_signal_snapshot（L2 最近 batch 的确认∪预警）"
                    print("从 quant_signal_snapshot 取 B 档（确认∪预警）标的 %d 只" % len(symbols))

    if not symbols:
        print(
            "无标的可 refresh；请传入 symbols、或先 make run-module-b、或设置 REFRESH_SEGMENT_SCOPE=full 使用全量",
            file=sys.stderr,
        )
        return 1

    if track == "a":
        return _run_a_track_branch(dsn, symbols, symbol_source)

    from diting.signal_layer import refresh_segment_signals_for_symbols, RefreshSegmentSignalsResult

    result = refresh_segment_signals_for_symbols(symbols, dsn, options={"track": track})
    assert isinstance(result, RefreshSegmentSignalsResult)

    passed = result.symbols_with_signal
    failed = [sym for sym in symbols if sym not in set(passed)]

    # 标的中文名
    all_syms = set(symbols) | set(result.symbols_with_signal) | set(result.symbols_without_segments)
    name_map = _load_symbol_names(list(all_syms), dsn)

    def _name(sym: str) -> str:
        n = (name_map.get(sym) or "").strip()
        return n if n else "(未录名)"

    _pq = (os.environ.get("PIPELINE_QUIET") or "").strip().lower() in ("1", "true", "yes")

    # 输出
    table_name = "segment_signal_cache"
    print()
    if not _pq:
        print("=" * 60)
        print("细分信号刷新完成 → 写入 L2 表 %s" % table_name)
        print("=" * 60)
    else:
        from diting.pipeline_io import pipeline_frame_quiet

        pipeline_frame_quiet()
        print("======== 信号层 refresh（管道精简）→ %s ========" % table_name)
    print("  无主营构成的标的数: %d" % len(result.symbols_without_segments))
    print("  无适配器细分数量: %d" % len(result.segments_without_adapter))
    print("  本批新写入 segment 行数: %d（UPSERT segment_signal_cache）" % len(result.segments_written))
    print("  TTL 内跳过(缓存仍有效未重写): %d" % len(result.segments_skipped_ttl))
    print("  失败细分数量: %d" % len(result.segments_failed))
    s = result.summary
    print("  汇总: 轨=%s | 总标的=%d | 有细分=%d | 解析细分=%d | 跳过=%d | 写入=%d | 失败=%d" % (
        s.get("track", "-"),
        s.get("total_symbols", 0),
        s.get("symbols_with_segments", 0),
        s.get("segments_resolved", 0),
        s.get("segments_skipped_ttl", 0),
        s.get("segments_written", 0),
        s.get("segments_failed", 0),
    ))
    print(
        "  [说明] 「有细分信号标的」= 至少 1 个关联 segment 在本批已写入或 TTL 仍有效；"
        "故本批写入=0 但仍有信号时，多为 TTL 命中。"
    )
    try:
        _db = int((os.environ.get("REFRESH_SEGMENT_DAYS_BACK") or "7").strip() or "7")
    except ValueError:
        _db = 7
    try:
        _rf = float((os.environ.get("PIPELINE_SEGMENT_REVENUE_FLOOR") or "0.3").strip() or "0.3")
    except ValueError:
        _rf = 0.3
    try:
        _tmax = int((os.environ.get("PIPELINE_SEGMENT_TABLE_MAX") or "80").strip() or "80")
    except ValueError:
        _tmax = 80
    from diting.signal_layer.pipeline_report import print_segment_refresh_work_table

    print_segment_refresh_work_table(
        dsn, symbols, result, days_back=_db, revenue_floor=_rf, max_rows=_tmax
    )
    if _pq:
        n_sig = len(passed)
        n_no = len(failed)
        print("  有细分信号标的=%d 只、无=%d 只" % (n_sig, n_no))
        print("  ── 人眼校准 · 与 Module C 右脑输入是否一致 ──")
        print("  · 本 run 标的来源: %s | 输入标的数=%d" % (symbol_source or "（未分类）", len(symbols)))
        qb, qn, _qt = _latest_quant_snapshot_meta(dsn)
        if qb:
            print(
                "  · L2 quant_signal_snapshot 最近批: %s 行 | batch_id=%s（应与刚跑完的 B 一致）"
                % (qn, qb)
            )
        cap = _calibration_list_max()
        print("  · 有细分信号（前 %s，右脑可读 segment_signal_cache）---" % cap)
        if passed:
            for sym in sorted(passed)[:cap]:
                print("    %s %s" % (sym, _name(sym)))
            if len(passed) > cap:
                print("    ... 共 %d 只，余下略" % len(passed))
        else:
            print("    （无）")
        print("  · 无细分信号（前 %s，C 可能显示「上游无数据」）---" % cap)
        if failed:
            for sym in sorted(failed)[:cap]:
                print("    %s %s" % (sym, _name(sym)))
            if len(failed) > cap:
                print("    ... 共 %d 只，余下略" % len(failed))
        else:
            print("    （无）")
        sws = list(result.symbols_without_segments or [])
        if sws:
            cap2 = min(cap, len(sws))
            print("  · 无主营构成标的（前 %s）---" % cap2)
            for sym in sorted(sws)[:cap2]:
                print("    %s %s" % (sym, _name(sym)))
            if len(sws) > cap2:
                print("    ... 共 %d 只" % len(sws))
        print("  ┌─ 信号层准出（设计对照 · 判断能否进入 Module C）────────────────")
        print(
            "  │ ① 输入: %s 只 | 来源=%s | L2 quant 最近批应与 B 一致（见上行）"
            % (len(symbols), symbol_source or "—")
        )
        print(
            "  │ ② 解析: 有细分信号=%s | 无细分信号=%s | 无主营构成标的=%s"
            % (len(passed), len(failed), len(result.symbols_without_segments or []))
        )
        print(
            "  │ ③ segment 落库: 本批新写入=%s | TTL跳过仍有效=%s | 失败=%s"
            % (
                len(result.segments_written),
                len(result.segments_skipped_ttl),
                len(result.segments_failed),
            )
        )
        print(
            "  │ ④ 结论: 右脑可对「有信号」%s 只做叙事；无信号 %s 只在 C 可能「主营细分无信号」"
            % (len(passed), len(failed))
        )
        print("  └──────────────────────────────────────────────────────────────")
        print("  ── 下一模块 Module C 依赖 ──")
        print("  · C 右脑读 L2 segment_signal_cache；上表「有信号」列即本批可叙事标的。")
        print("  · 完整列表: unset PIPELINE_QUIET 后 make refresh-segment-signals")
    print()

    # 通过的标的 / 未通过的标的（含中文名）；非 PIPELINE_QUIET 时打印长列表
    if passed and not _pq:
        print("【有细分信号的标的】(%d 只):" % len(passed))
        for sym in sorted(passed)[:50]:
            print("  %s %s" % (sym, _name(sym)))
        if len(passed) > 50:
            print("  ... 共 %d 只，仅展示前 50" % len(passed))
        print()

    if failed and not _pq:
        print("【无细分信号的标的】(%d 只):" % len(failed))
        for sym in sorted(failed)[:50]:
            print("  %s %s" % (sym, _name(sym)))
        if len(failed) > 50:
            print("  ... 共 %d 只，仅展示前 50" % len(failed))

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
