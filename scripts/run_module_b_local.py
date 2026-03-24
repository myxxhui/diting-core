#!/usr/bin/env python3
# [Ref: 02_量化扫描引擎_实践] 一键本地运行 B 模块：基于 A 模块处理过的标的池（同源），执行扫描，输出写入 L2 供 Module C 使用
# 用法：在 diting-core 根目录 make run-module-b 或 PYTHONPATH=. python3 scripts/run_module_b_local.py
# 建议先执行 make run-module-a 使 L2 有 classifier_output_snapshot；本脚本使用与 A 同源标的池（diting_symbols.txt 或 DITING_SYMBOLS）
#
# B↔C 数据交换（L2，无额外「魔法表」）：
#   - quant_signal_snapshot：确认档 ∪ 预警档，与 Module C 默认 MOE_C_SCOPE=snapshot 一一对应；主交接表。
#   - quant_signal_scan_all：全量打分；C 在 MOE_PIPELINE=snapshot 时读此表按 batch 取 B。
#   终端末尾打印本批 batch_id；与 C 对齐请 export MOE_QUANT_BATCH_ID=<该 id>（见 run_module_c_local 说明）。

import os
import sys
import unicodedata
import uuid
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

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


def _pipeline_quiet() -> bool:
    """全链路 make run-full-pipeline 时置 PIPELINE_QUIET=1，减少终端噪音。"""
    return (os.environ.get("PIPELINE_QUIET") or "").strip().lower() in ("1", "true", "yes")


def _calibration_list_max() -> int:
    raw = (os.environ.get("PIPELINE_CALIBRATION_LIST_MAX") or "32").strip()
    try:
        n = int(raw, 10)
        return max(8, min(500, n))
    except ValueError:
        return 32


def _module_b_print_max() -> Optional[int]:
    """
    MODULE_B_PRINT_MAX：未设置或 0 = 不限制（确认档列表等默认全部打印；不打印执行标的清单以减少噪音）；
    正整数 = 最多打印多少行（极大名单时可设为 50 等避免刷屏）。
    """
    raw = (os.environ.get("MODULE_B_PRINT_MAX") or "").strip()
    if not raw:
        return None
    try:
        n = int(raw, 10)
        return None if n <= 0 else n
    except ValueError:
        return None


def _take_with_ellipsis(seq: Sequence, cap: Optional[int]) -> Tuple[List, bool]:
    """返回 (前 cap 项或全部, 是否截断)。"""
    lst = list(seq)
    if cap is None or len(lst) <= cap:
        return lst, False
    return lst[:cap], True


def _default_universe_from_diting_symbols():
    """与 Module A 一致：默认按 config/diting_symbols.txt 全部标的。"""
    from pathlib import Path
    root = Path(ROOT)
    path = root / "config" / "diting_symbols.txt"
    if not path.exists():
        return None
    from diting.universe import normalize_symbol
    symbols = []
    seen = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip().split("#")[0].strip()
            if line:
                sym = normalize_symbol(line)
                if sym and sym not in seen:
                    seen.add(sym)
                    symbols.append(sym)
    return symbols if symbols else None


# 策略池名称，与 02 规约、l2_snapshot_writer 一致，用于终端展示
POOL_NAMES = {0: "UNSPECIFIED", 1: "TREND", 2: "REVERSION", 3: "BREAKOUT", 4: "MOMENTUM"}
POOL_NAMES_CN = {0: "未命中", 1: "趋势", 2: "反转", 3: "突破", 4: "动量"}


def _strategy_display(signal):
    """返回 strategy_source 可读名与 pool_scores 简要串（便于看出策略与打分是否生效）。"""
    src = signal.get("strategy_source", 0)
    if isinstance(src, int):
        src_name = POOL_NAMES.get(src, "UNSPECIFIED")
        src_cn = POOL_NAMES_CN.get(src, "未命中")
    else:
        src_name = str(src or "UNSPECIFIED")[:16]
        src_cn = src_name
    ps = signal.get("pool_scores") or {}
    if isinstance(ps, dict):
        parts = []
        for pid, name in [(1, "趋势"), (2, "反转"), (3, "突破"), (4, "动量")]:
            parts.append("%s=%.0f" % (name, ps.get(pid, 0)))
        pool_str = " ".join(parts)
    else:
        pool_str = ""
    return src_name, src_cn, pool_str


def _display_width(s: str) -> int:
    """终端显示宽度：CJK 等宽字符计为 2。"""
    w = 0
    for ch in s:
        if unicodedata.east_asian_width(ch) in ("F", "W"):
            w += 2
        else:
            w += 1
    return w


def _pad_cell(s: object, width: int, align: str = "left") -> str:
    """按显示宽度填充，便于中文终端与表头对齐。"""
    t = str(s) if s is not None else ""
    extra = width - _display_width(t)
    if extra < 0:
        out = []
        w = 0
        for ch in t:
            cw = 2 if unicodedata.east_asian_width(ch) in ("F", "W") else 1
            if w + cw > width - 1:
                out.append("…")
                break
            out.append(ch)
            w += cw
        t = "".join(out)
        extra = width - _display_width(t)
    pad = max(0, extra)
    if align == "right":
        return " " * pad + t
    if align == "center":
        l = pad // 2
        r = pad - l
        return " " * l + t + " " * r
    return t + " " * pad


def _pct_vs_entry(entry: object, price: object):
    """相对参考入场价的涨跌幅%%：(price-entry)/entry*100；无效则 None。"""
    try:
        e = float(entry)
        p = float(price)
        if e <= 1e-12:
            return None
        return (p - e) / e * 100.0
    except (TypeError, ValueError):
        return None


def _risk_table_line(cells, widths, aligns):
    """一行表：每列按显示宽度对齐（中文终端）。"""
    parts = [_pad_cell(c, w, a) for c, w, a in zip(cells, widths, aligns)]
    return "  " + " ".join(parts)


def _print_risk_snapshot_table(snap_sorted):
    """风控参考表：表头与数据列宽一致，避免 CJK 错位。"""
    W = [4, 12, 6, 7, 12, 12, 10, 12, 10, 12, 10, 16]
    A = ["right", "left", "center", "right", "right", "right", "right", "right", "right", "right", "right", "left"]
    headers = ["#", "标的", "档位", "总分", "参考入场", "止损价", "止损%", "止盈1R", "止盈1%", "止盈2R", "止盈2%", "中文名"]
    print(_risk_table_line(headers, W, A))
    sep_w = sum(W) + (len(W) - 1) * 1
    print("  " + "-" * min(sep_w, 160))
    for rank, s in enumerate(snap_sorted, 1):
        sym = (s.get("symbol", "") or "")[:12]
        name = (s.get("symbol_name", "") or "")[:14]
        tier = "确认" if s.get("confirmed_passed") else "预警"
        entry_f, stop_f, tps, score_f = _risk_metrics(s)
        tp1 = tps[0] if len(tps) > 0 else None
        tp2 = tps[1] if len(tps) > 1 else None
        sl_pct = _pct_vs_entry(entry_f, stop_f)
        tp1_pct = _pct_vs_entry(entry_f, tp1)
        tp2_pct = _pct_vs_entry(entry_f, tp2)
        cells = [
            str(rank),
            sym,
            tier,
            "%.2f" % score_f,
            _fmt_price_short(entry_f),
            _fmt_price_short(stop_f),
            _fmt_pct_short(sl_pct),
            _fmt_price_short(tp1),
            _fmt_pct_short(tp1_pct),
            _fmt_price_short(tp2),
            _fmt_pct_short(tp2_pct),
            name or "-",
        ]
        print(_risk_table_line(cells, W, A))


def _fmt_price_short(x: object) -> str:
    if x is None:
        return "-"
    try:
        return "%.4f" % float(x)
    except (TypeError, ValueError):
        return "-"


def _fmt_pct_short(p: object) -> str:
    if p is None:
        return "-"
    try:
        return "%+.2f%%" % float(p)
    except (TypeError, ValueError):
        return "-"


def _risk_metrics(signal):
    """
    与 L2 stop_loss_price / take_profit_json 一致。
    返回 (entry_f, stop_f, tp_list, score_f) 供对齐打印与百分比。
    """
    entry_f = None
    try:
        e = signal.get("entry_reference_price")
        if e is not None:
            entry_f = float(e)
    except (TypeError, ValueError):
        pass
    stop_f = None
    try:
        st = signal.get("stop_loss_price")
        if st is not None:
            stop_f = float(st)
    except (TypeError, ValueError):
        pass
    tps = []
    for x in list(signal.get("take_profit_prices") or []):
        try:
            tps.append(float(x))
        except (TypeError, ValueError):
            pass
    try:
        score_f = float(signal.get("technical_score") or 0)
    except (TypeError, ValueError):
        score_f = 0.0
    return entry_f, stop_f, tps, score_f


def _print_pool_score_table(rows, title_line, sort_by_score_desc=True):
    """池打分明细：趋势/反转/突破/动量/总分/策略/是否通过（中文终端列对齐）。"""
    if not rows:
        print("  %s（0 条）" % title_line)
        return
    ordered = list(rows)
    if sort_by_score_desc:
        ordered.sort(key=lambda x: (-float(x.get("technical_score") or 0), x.get("symbol") or ""))
    hint = "，按总分降序" if sort_by_score_desc else "，顺序与扫描处理顺序一致"
    print("  %s（共 %s 条%s）：" % (title_line, len(ordered), hint))
    W = [12, 10, 8, 8, 8, 8, 8, 8, 6]
    A = ["left", "left", "right", "right", "right", "right", "right", "left", "center"]
    headers = ["标的", "中文名", "趋势", "反转", "突破", "动量", "总得分", "策略", "通过"]
    print(_risk_table_line(headers, W, A))
    print("  " + "-" * min(sum(W) + 8, 120))
    for s in ordered:
        sym = s.get("symbol", "") or ""
        name = (s.get("symbol_name", "") or "")[:8]
        score = float(s.get("technical_score", getattr(s, "technical_score", 0)) or 0)
        _, src_cn, _ = _strategy_display(s)
        ps = s.get("pool_scores") or {}
        t = float(ps.get(1, 0))
        r = float(ps.get(2, 0))
        b = float(ps.get(3, 0))
        m = float(ps.get(4, 0))
        p = "是" if s.get("passed", False) else "否"
        print(
            _risk_table_line(
                [
                    sym,
                    name or "-",
                    "%.2f" % t,
                    "%.2f" % r,
                    "%.2f" % b,
                    "%.2f" % m,
                    "%.2f" % score,
                    src_cn,
                    p,
                ],
                W,
                A,
            )
        )


def _query_l1_ohlcv_bars(dsn: str, symbols: list, period: str = "daily"):
    """查询 L1 各标的日 K 线条数，返回 {symbol: count}；用于判断是否满足 B 模块≥60 根。"""
    if not dsn or not symbols:
        return {}
    try:
        import psycopg2
        conn = psycopg2.connect(dsn, connect_timeout=15)
        cur = conn.cursor()
        cur.execute(
            """SELECT symbol, COUNT(*) FROM ohlcv WHERE period = %s AND symbol = ANY(%s) GROUP BY symbol""",
            (period, symbols),
        )
        out = {r[0]: r[1] for r in cur.fetchall()}
        cur.close()
        conn.close()
        return out
    except Exception:
        return {}


def _read_classifier_batch_from_l2(dsn: str, limit_batch: int = 1):
    """从 L2 读取最近一批 ClassifierOutput 的 batch_id 与 symbol 集合（可选，用于本步「基于 A 处理过的数据」验证）。"""
    try:
        import psycopg2
    except ImportError:
        return None, set()
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        cur.execute("""
            SELECT batch_id, symbol FROM classifier_output_snapshot
            WHERE batch_id = (SELECT batch_id FROM classifier_output_snapshot ORDER BY created_at DESC LIMIT 1)
            LIMIT 10000
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        if not rows:
            return None, set()
        batch_id = rows[0][0]
        symbols = {r[1] for r in rows if r[1]}
        return batch_id, symbols
    except Exception:
        return None, set()


def main():
    from diting.scanner import QuantScanner
    from diting.scanner import indicators
    from diting.scanner.l2_snapshot_writer import write_quant_signal_snapshot, write_quant_signal_scan_all
    from diting.universe import parse_symbol_list_from_env

    # 强制使用 TA-Lib：未安装则退出并提示
    if not indicators.has_talib():
        print("错误: 未检测到 TA-Lib。请先安装系统层 ta-lib C 库，再在 diting-core 执行: make deps-scanner", file=sys.stderr)
        print("  (deps-scanner 会使用 python3.8 安装 TA-Lib；make run-module-b 将使用同一 Python)", file=sys.stderr)
        sys.exit(1)

    # 始终使用采集模块的生产数据，禁止 Mock
    ohlcv_dsn = (os.environ.get("TIMESCALE_DSN") or "").strip()
    if not ohlcv_dsn:
        print("错误: 未配置 TIMESCALE_DSN。请于 .env 中配置 L1 连接串，使用采集模块采集的生产数据。", file=sys.stderr)
        sys.exit(1)

    universe = parse_symbol_list_from_env("DITING_SYMBOLS") or parse_symbol_list_from_env("MODULE_AB_SYMBOLS")
    if not universe:
        universe = _default_universe_from_diting_symbols()
    if not universe:
        print("错误: 未获取到标的列表（请配置 DITING_SYMBOLS 或保证 config/diting_symbols.txt 存在且非空）", file=sys.stderr)
        sys.exit(1)

    # 标的中文名：优先 L2 数据库，其次静态文件，缺失时从东方财富(akshare)拉取并写入 L2；不依赖静态文件
    from diting.scanner.symbol_names import get_symbol_names
    dsn_l2 = (os.environ.get("PG_L2_DSN") or "").strip()
    skip_akshare = bool(os.environ.get("DITING_SKIP_AKSHARE_NAMES"))
    symbol_to_name = get_symbol_names(
        list(universe),
        dsn=dsn_l2 or None,
        root=Path(ROOT),
        skip_akshare=skip_akshare,
    )

    _pq = _pipeline_quiet()

    # 可选：从 L2 读取 A 模块最新一批，用于「基于 A 处理过的数据」验证（同批标的一致）
    classifier_batch_id = None
    classifier_symbols = set()
    dsn = (os.environ.get("PG_L2_DSN") or "").strip()
    if dsn:
        classifier_batch_id, classifier_symbols = _read_classifier_batch_from_l2(dsn)
        if classifier_symbols:
            # 与 A 同批：优先使用 L2 中 A 的 batch 标的子集与当前 universe 的交集，或直接用 universe
            pass  # 本步仍用 universe 全量扫描；仅做信息展示

    # L1 K 线数量检查：B 模块策略需每标至少 60 根日 K（建议 120 根）
    bar_counts = _query_l1_ohlcv_bars(ohlcv_dsn, universe)
    l1_min_bars = None
    l1_max_bars = None
    l1_ok60_n = 0
    if bar_counts:
        _bc = [bar_counts.get(s, 0) for s in universe]
        l1_min_bars = min(_bc)
        l1_max_bars = max(_bc)
        l1_ok60_n = sum(1 for c in _bc if c >= 60)
    if bar_counts and not _pq:
        n = len(universe)
        print("======== L1 数据是否满足 B 模块策略 ========  ")
        print(
            "  B 模块需求: 每标至少 60 根日 K（建议 120 根）；当前 %s 标: 最少 %s 根、最多 %s 根，满足≥60 根: %s/%s 只"
            % (n, l1_min_bars, l1_max_bars, l1_ok60_n, n)
        )
        if l1_ok60_n < len(universe):
            print("  说明: 部分标的 K 线不足 60 根时，该标的不参与打分或得分为 0。")
        print()

    batch_id = str(uuid.uuid4())
    scanner = QuantScanner()
    # 全量结果（含 passed 标记），全部保存；通过/未通过分开存放
    signals = scanner.scan_market(universe, ohlcv_dsn=ohlcv_dsn, correlation_id=batch_id, return_all=True)
    for s in signals:
        s["symbol_name"] = symbol_to_name.get(s.get("symbol", ""), "")

    meta = getattr(scanner, "last_scan_pipeline", None)
    carry_extra: list = []
    if dsn and meta:
        cd_syms = meta.get("cooldown_skipped_symbols") or []
        carry_on = os.environ.get("DITING_B_COOLDOWN_CARRYOVER_L2", "1").strip().lower() not in (
            "0",
            "false",
            "no",
        )
        if cd_syms and carry_on:
            from diting.scanner.l2_cooldown_carryover import carryover_signals_from_l2
            from diting.scanner.config_fingerprint import compute_scanner_rules_fingerprint

            seen_syms = {str(s.get("symbol", "")).strip().upper() for s in signals if s.get("symbol")}
            _fp = ""
            if signals:
                _fp = str((signals[0] or {}).get("scanner_rules_fingerprint") or "")
            if not _fp:
                _fp = compute_scanner_rules_fingerprint()
            carry_extra = carryover_signals_from_l2(
                dsn,
                cd_syms,
                batch_id=batch_id,
                correlation_id=batch_id,
                scanner_rules_fingerprint=_fp,
                already_present_symbols=seen_syms,
            )
            for s in carry_extra:
                s["symbol_name"] = symbol_to_name.get(s.get("symbol", ""), "") or s.get("symbol_name", "")
            signals.extend(carry_extra)
    if meta and not _pq:
        print()
        print("======== 粗筛 / 指数 regime / 冷却 / Classifier（本批统计，见 scanner_rules.yaml）========  ")
        bull = meta.get("index_ma_bullish")
        bull_s = "是" if bull is True else ("否" if bull is False else "未知(不乘熊市)")
        print(
            "  粗筛: %s；动量分位≥%s、流动性分位≥%s；因粗筛跳过: %s 只"
            % (
                "开" if meta.get("coarse_screen_enabled") else "关",
                meta.get("coarse_min_momentum_pct"),
                meta.get("coarse_min_liquidity_pct"),
                meta.get("skipped_after_coarse"),
            )
        )
        print(
            "  指数 regime: %s；基准 %s；MA 多头=%s；趋势池乘子=%s"
            % (
                "开" if meta.get("index_regime_enabled") else "关",
                meta.get("index_benchmark"),
                bull_s,
                meta.get("index_regime_trend_mult"),
            )
        )
        print(
            "  指数波动应力: %s；index_atr_ratio=%s（stress 时压低突破池、略抬反转池）"
            % (
                "是" if meta.get("index_stress_vol") else "否",
                meta.get("index_atr_ratio"),
            )
        )
        print(
            "  冷却: %s 天；因冷却跳过: %s 只"
            % (meta.get("signal_cooldown_days"), meta.get("skipped_cooldown"))
        )
        if carry_extra:
            print(
                "  冷却沿用 L2 快照并入本批: %s 只（分数与止损/止盈为上次写入值，本轮未重算 TA-Lib；与当前 batch_id 一并写入 L2）"
                % len(carry_extra)
            )
        tags = meta.get("allowed_primary_tags") or []
        print(
            "  Classifier 门控: %s；match_mode=%s；batch_id=%s；允许标签=%s；因门控跳过: %s 只"
            % (
                "开" if meta.get("classifier_gate_enabled") else "关",
                meta.get("classifier_gate_match_mode", ""),
                meta.get("classifier_gate_batch_id") or "（最新）",
                tags if tags else "（空=全放行）",
                meta.get("skipped_classifier_gate"),
            )
        )
        print(
            "  L1 足根标的: %s 只 → TA-Lib 实际打分: %s 只"
            % (meta.get("symbols_with_ohlcv_ok"), meta.get("symbols_talib_scored"))
        )
        if meta.get("unmapped_sector_strength") is not None:
            print(
                "  板块强度: 无行业映射条数=%s，有映射条数=%s；unmapped_sector_strength=%s（见 filters.sector_strength）"
                % (
                    meta.get("sector_strength_unmapped_count"),
                    meta.get("sector_strength_mapped_count"),
                    meta.get("unmapped_sector_strength"),
                )
            )
        print()

    sm = getattr(scanner, "last_scan_metrics", None)
    if sm and not _pq:
        print("======== 性能与可观测性（scanner_run_metrics，见 scanner_performance）========  ")
        print(
            "  总耗时 %.1f ms | 拉取OHLCV %.1f | 分位 %.1f | L2预检 %.1f | 多池 %.1f | 板块 %.1f | 组装输出 %.1f"
            % (
                sm.get("ms_total", 0),
                sm.get("ms_fetch_batch_ohlcv", 0),
                sm.get("ms_percentile_ranks", 0),
                sm.get("ms_l2_precheck", 0),
                sm.get("ms_evaluate_pools", 0),
                sm.get("ms_sector_strength", 0),
                sm.get("ms_build_output", 0),
            )
        )
        print("  并行 ThreadPoolExecutor workers=%s | 输出条数=%s" % (sm.get("parallel_workers_used"), sm.get("symbols_out")))
        _ex = sm.get("extra") or {}
        _fp = _ex.get("scanner_rules_fingerprint")
        if _fp:
            print("  scanner_rules.yaml 指纹(sha256 前16hex，与 L2 行一致): %s" % _fp)
        print()

    passed_list = [s for s in signals if s.get("passed")]
    snapshot_list = [s for s in signals if s.get("confirmed_passed") or s.get("alert_passed")]
    ats = getattr(scanner, "_a_track", {}) or {}
    threshold = getattr(scanner, "_score_threshold", 70)
    alert_t = ats.get("alert_threshold", max(40, threshold - 15))
    prof = ats.get("signal_profile", "balanced")

    if not _pq:
        print()
        _pb_max = _module_b_print_max()

        if classifier_batch_id and classifier_symbols:
            print("======== 基于 A 模块数据（L2 最新 batch）========  ")
            print("  classifier_output_snapshot 最新 batch_id: %s，标的数: %s" % (classifier_batch_id[:32] + "..", len(classifier_symbols)))
            print()

        print("======== B 模块扫描结果（全量保存当前分数，通过/未通过分开存放）========  ")
        print(
            "  A 轨短线模式: signal_profile=%s；确认档≥%s、预警档≥%s（dual_tier=%s）；打分为 [0,100] 连续分"
            % (prof, threshold, alert_t, ats.get("dual_tier", True))
        )
        print("  全量条数: %s（均已打分）  确认档通过: %s  写入通过表(预警+确认): %s" % (len(signals), len(passed_list), len(snapshot_list)))
        n_alert_only = sum(1 for s in signals if s.get("alert_passed") and not s.get("confirmed_passed"))
        print(
            "  说明: 「确认档通过」= passed，仅总分≥%s 且满足板块/收紧等条件；「写入通过表」= 确认档 ∪ 预警档（dual_tier：仅达预警≥%s 未达确认也写入 snapshot，供 Module C）。"
            " 本批分解: 确认 %s + 仅预警 %s = %s。"
            % (threshold, alert_t, len(passed_list), n_alert_only, len(snapshot_list))
        )
        print("  说明: 得分 0 表示四池（趋势/反转/突破/动量）条件均未形成有效子分，并非未打分；下方「各池得分」可看出策略与打分是否生效。")
        if passed_list:
            passed_by_score = sorted(
                passed_list,
                key=lambda x: (-float(x.get("technical_score") or 0), x.get("symbol") or ""),
            )
            passed_show, passed_trunc = _take_with_ellipsis(passed_by_score, _pb_max)
            print(
                "  本批确认档通过标的（passed=True，得分≥%s，按总分降序，共 %s 条%s）："
                % (
                    threshold,
                    len(passed_list),
                    "；仅显示前 %s 行（MODULE_B_PRINT_MAX=%s）" % (_pb_max, _pb_max)
                    if passed_trunc
                    else "；全部列出",
                )
            )
            for s in passed_show:
                sym = s.get("symbol", getattr(s, "symbol", ""))
                name = s.get("symbol_name", "") or ""
                score = s.get("technical_score", getattr(s, "technical_score", 0))
                src_name, src_cn, pool_str = _strategy_display(s)
                print(
                    "    %s  %s  technical_score=%.2f  策略=%s(%s)  各池得分: %s"
                    % (sym, name or "(无中文名)", score, src_name, src_cn, pool_str or "-")
                )
            if passed_trunc:
                print("    ... 共 %s 条确认档，上表仅前 %s 行" % (len(passed_list), len(passed_show)))
        if snapshot_list:
            snap_sorted = sorted(
                snapshot_list,
                key=lambda x: (
                    -float(x.get("technical_score") or 0),
                    x.get("symbol") or "",
                ),
            )
            print("======== 风控参考（写入 snapshot · 按总分降序 · 单位：元）========  ")
            print(
                "  共 %s 条；入场=最后一根日线收盘；止损/止盈与 L2 字段 stop_loss_price、take_profit_json 一致。"
                " 盈亏%%=(价-入场)/入场×100%%（止损%% 多为负表示触及止损的亏损幅度）。"
                " 下列已按「显示宽度」对齐，适配中文终端。"
                % len(snap_sorted)
            )
            _print_risk_snapshot_table(snap_sorted)
        if signals and snapshot_list:
            print()
            print(
                "  说明: 总分与排名会随「最新一根日 K」及全截面分位（如动量池）变化；"
                "不同交易日或 L1 数据更新后重跑，荣昌生物/宁德时代等是否仍最高不固定。"
            )
            print()
            _print_pool_score_table(
                snapshot_list,
                "确认档∪预警档标的·池打分明细（写入 snapshot 供 Module C；不含未通过标的）",
                sort_by_score_desc=True,
            )
            print("  提示: 止损/止盈与盈亏%%见上方「风控参考」表；本段为各池打分，不含 stop_loss / take_profit。")
        print()
    else:
        from diting.pipeline_io import pipeline_frame_quiet

        pipeline_frame_quiet()
        print()
        print("======== Module B（管道精简）========  ")
        print(
            "  全量=%s 确认档=%s 快照(确认∪预警)=%s | batch_id=%s"
            % (len(signals), len(passed_list), len(snapshot_list), batch_id)
        )
        if bar_counts and l1_min_bars is not None:
            print(
                "  [L1→B] %s 标: 日K 最少 %s 根、最多 %s 根，≥60根 %s/%s（不足则该标常无 scan 行/0 分）"
                % (len(universe), l1_min_bars, l1_max_bars, l1_ok60_n, len(universe))
            )
        if dsn and classifier_batch_id:
            print(
                "  [A→B] L2 classifier 最新 batch_id=%s（标的 %s 只）"
                % (classifier_batch_id, len(classifier_symbols))
            )
        if _pq and dsn and classifier_symbols:
            u_set = {(s or "").strip().upper() for s in universe}
            cs_set = {(s or "").strip().upper() for s in classifier_symbols}
            inter = u_set & cs_set
            only_u = sorted(u_set - cs_set)
            only_c = sorted(cs_set - u_set)
            cap = _calibration_list_max()
            print(
                "  [人眼校准] universe∩L2(classifier 最新批)=%s 只 | 仅 universe 有 %s | 仅 classifier 有 %s"
                % (len(inter), len(only_u), len(only_c))
            )
            if only_u:
                print("    仅 universe(前 %s): %s" % (cap, ", ".join(only_u[:cap])))
            if only_c:
                print("    仅 classifier(前 %s): %s" % (cap, ", ".join(only_c[:cap])))
        if meta:
            print(
                "  [门控/扫描] 粗筛跳过 %s | 冷却跳过 %s | classifier 门控跳过 %s | TA-Lib 打分 %s 只"
                % (
                    meta.get("skipped_after_coarse"),
                    meta.get("skipped_cooldown"),
                    meta.get("skipped_classifier_gate"),
                    meta.get("symbols_talib_scored"),
                )
            )
        if carry_extra:
            print("  [冷却沿用 L2] 并入本批 %s 只（分数为上次快照，本轮未重算 TA）" % len(carry_extra))
        if sm:
            print(
                "  [性能] 总耗时 %.0f ms | 并行输出条数 %s"
                % (sm.get("ms_total", 0), sm.get("symbols_out"))
            )
        print("  完整终端输出请: make run-module-b ；L2 汇总: make query-full-pipeline-result")
        print()

    n_written_snapshot = 0
    n_written_scan_all = 0
    write_location = "未写入"
    if dsn:
        try:
            n_written_scan_all = write_quant_signal_scan_all(dsn, signals, batch_id=batch_id, correlation_id=batch_id)
            n_written_snapshot = write_quant_signal_snapshot(dsn, signals, batch_id=batch_id, correlation_id=batch_id)
            if n_written_scan_all > 0:
                write_location = "L2 全量表 quant_signal_scan_all: %s 条（通过/未通过可查）；通过表 quant_signal_snapshot: %s 条（供 Module C），batch_id=%s.." % (n_written_scan_all, n_written_snapshot, batch_id[:32])
            else:
                write_location = "L2 写入未成功（表可能未创建）。请先执行 make init-l2-quant-signal-table"
        except Exception as e:
            write_location = "L2 写入失败: %s" % e
    else:
        write_location = "未写入（未配置 PG_L2_DSN）"

    print("======== 写入 L2（通过表供 Module C，全量表供查询）========  ")
    print("  %s" % write_location)
    if _pq and dsn and signals:
        snap_n = len([s for s in signals if s.get("confirmed_passed") or s.get("alert_passed")])
        ok_s = (n_written_scan_all == len(signals) and n_written_snapshot == snap_n)
        print(
            "  [人眼校准] L2 写入 vs 内存信号: scan_all=%s/%s 行 | snapshot=%s/%s 行 | %s"
            % (
                n_written_scan_all,
                len(signals),
                n_written_snapshot,
                snap_n,
                "OK" if ok_s else "NG 须核对表约束/门控",
            )
        )
    print()
    print("======== B→C 数据交换（L2；与 C 对齐用 batch_id）========  ")
    if _pq:
        print("  ── 下一模块 信号层 refresh → Module C 依赖 ──")
        print("  · refresh 默认从 quant_signal_snapshot「最近 batch」取标的（即本批刚写入），细分结果进 segment_signal_cache。")
        print("  · Module C 右脑读 segment_signal_cache；量化门控读本批 quant_signal_snapshot / scan_all。强锁本批可: export MOE_QUANT_BATCH_ID=<下行 batch_id>。")
    print("  表1 quant_signal_snapshot：本批「确认∪预警」档行，供 Module C 默认 MOE_C_SCOPE=snapshot（与写入行数一致）。")
    print("  表2 quant_signal_scan_all：全量打分（含未通过）；C 在 MOE_PIPELINE=snapshot 时按 MOE_QUANT_BATCH_ID 读 B。")
    print("  本批 batch_id（请完整复制，与 L2 两表 correlation 一致）:")
    print("    %s" % batch_id)
    if snapshot_list:
        snap_syms_sorted = sorted(
            (str(s.get("symbol") or "").strip().upper() for s in snapshot_list if s.get("symbol")),
            key=lambda x: x,
        )
        line = ", ".join(snap_syms_sorted)
        if len(line) <= 200:
            print("  本批 snapshot 标的清单（共 %s 只，与 C 门控集合一致）: %s" % (len(snap_syms_sorted), line))
        else:
            print("  本批 snapshot 标的清单（共 %s 只，与 C 门控集合一致）:" % len(snap_syms_sorted))
            for i in range(0, len(snap_syms_sorted), 12):
                chunk = snap_syms_sorted[i : i + 12]
                print("    %s" % ", ".join(chunk))
    else:
        print("  本批 snapshot 标的: （无，quant_signal_snapshot 写入 0 行）")
    print("  与本次 B 同批跑 Module C：默认 make run-module-c 在已配 PG_L2_DSN 时会自动用 L2 最新批（含本批）；若需与本次完全一致也可显式指定：")
    print("    export MOE_QUANT_BATCH_ID=%s" % batch_id)
    print("    # 默认 MOE_PIPELINE 在已配 L2 时为 snapshot；MOE_C_SCOPE 默认 snapshot")
    print()

    expect_ok = len(universe) >= 1 and len(signals) >= 0
    if dsn and signals:
        snap_n = len([s for s in signals if s.get("confirmed_passed") or s.get("alert_passed")])
        expect_ok = expect_ok and (n_written_scan_all == len(signals) and n_written_snapshot == snap_n)
    print("======== 输出是否符合预期 ========  ")
    print("  执行标的数=%s，全量保存=%s，通过=%s；L2 全量写入=%s，通过表写入=%s" % (len(universe), len(signals), len(passed_list), n_written_scan_all, n_written_snapshot))
    print("  是否符合预期: %s" % ("是" if expect_ok else "否（请检查 PG_L2_DSN 或执行 make init-l2-quant-signal-table）"))
    if _pq:
        du = len(universe) - len(signals)
        n_alert_only = sum(1 for s in signals if s.get("alert_passed") and not s.get("confirmed_passed"))
        gap_l1 = (len(universe) - l1_ok60_n) if bar_counts else du
        if du == 0:
            du_note = "universe 与 scan 全量条数对齐"
        elif bar_counts and du == gap_l1:
            du_note = "差值与「universe−L1≥60根」只数一致 → 设计允许（不足根常无 scan 行）"
        else:
            du_note = "请核对扫描日志/配置是否与 universe 一致"
        print()
        print("  ┌─ 模块 B 准出（设计对照 · 判断能否进入 refresh/C）────────────────")
        print("  │ ① universe vs scan 全量: %s vs %s | %s" % (len(universe), len(signals), du_note))
        if l1_min_bars is not None:
            print(
                "  │ ② L1: ≥60 根 %s/%s 只 | 最少/最多日K=%s/%s"
                % (l1_ok60_n, len(universe), l1_min_bars, l1_max_bars)
            )
        print(
            "  │ ③ 策略分层: 确认档(passed)=%s | 仅预警=%s | snapshot 写入=%s（C 默认用 snapshot）"
            % (len(passed_list), n_alert_only, len(snapshot_list))
        )
        print("  │ ④ L2: 上表「是否符合预期」= 写入与内存条数一致；batch_id 供下游对齐")
        print(
            "  │ ⑤ 结论: %s"
            % ("可进入 refresh / Module C" if expect_ok else "请先修复 L2 写入或门控再往下游")
        )
        print("  └──────────────────────────────────────────────────────────────")
    print()
    sys.exit(0 if expect_ok else 1)


if __name__ == "__main__":
    main()
