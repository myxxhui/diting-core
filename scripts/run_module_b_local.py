#!/usr/bin/env python3
# [Ref: 02_量化扫描引擎_实践] 一键本地运行 B 模块：基于 A 模块处理过的标的池（同源），执行扫描，输出写入 L2 供 Module C 使用
# 用法：在 diting-core 根目录 make run-module-b 或 PYTHONPATH=. python3 scripts/run_module_b_local.py
# 建议先执行 make run-module-a 使 L2 有 classifier_output_snapshot；本脚本使用与 A 同源标的池（diting_symbols.txt 或 DITING_SYMBOLS）

import os
import sys
import uuid
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
    if bar_counts:
        counts = [bar_counts.get(s, 0) for s in universe]
        min_bars = min(counts)
        max_bars = max(counts)
        ok_60 = sum(1 for c in counts if c >= 60)
        n = len(universe)
        print("======== L1 数据是否满足 B 模块策略 ========  ")
        print("  B 模块需求: 每标至少 60 根日 K（建议 120 根）；当前 %s 标: 最少 %s 根、最多 %s 根，满足≥60 根: %s/%s 只" % (n, min_bars, max_bars, ok_60, n))
        if ok_60 < len(universe):
            print("  说明: 部分标的 K 线不足 60 根时，该标的不参与打分或得分为 0。")
        print()

    batch_id = str(uuid.uuid4())
    scanner = QuantScanner()
    # 全量结果（含 passed 标记），全部保存；通过/未通过分开存放
    signals = scanner.scan_market(universe, ohlcv_dsn=ohlcv_dsn, correlation_id=batch_id, return_all=True)
    for s in signals:
        s["symbol_name"] = symbol_to_name.get(s.get("symbol", ""), "")

    print()
    print("======== 执行标的（共 %s 只，与 Module A 同源；数据源: TIMESCALE_DSN 生产数据）========  " % len(universe))
    for i, s in enumerate(universe[:20], 1):
        print("  %s. %s" % (i, s))
    if len(universe) > 20:
        print("  ... 等共 %s 只" % len(universe))
    print()

    if classifier_batch_id and classifier_symbols:
        print("======== 基于 A 模块数据（L2 最新 batch）========  ")
        print("  classifier_output_snapshot 最新 batch_id: %s，标的数: %s" % (classifier_batch_id[:32] + "..", len(classifier_symbols)))
        print()

    passed_list = [s for s in signals if s.get("passed")]
    # 阈值来自 config/scanner_rules.yaml technical_score_threshold（默认 70）
    threshold = getattr(scanner, "_score_threshold", 70)
    print("======== B 模块扫描结果（全量保存当前分数，通过/未通过分开存放）========  ")
    print("  阈值: %s（仅得分≥阈值才通过）；打分为 [0,100] 连续分，多池取最高分并可加共振加成" % threshold)
    print("  全量条数: %s（均已打分）  通过阈值条数: %s" % (len(signals), len(passed_list)))
    print("  说明: 得分 0 表示该标的三池（趋势/反转/突破）条件均不满足，并非未打分；下方「各池得分」可看出策略与打分是否生效。")
    if passed_list:
        print("  本批通过阈值的标的（得分≥%s）：" % threshold)
        for s in passed_list[:20]:
            sym = s.get("symbol", getattr(s, "symbol", ""))
            name = s.get("symbol_name", "") or ""
            score = s.get("technical_score", getattr(s, "technical_score", 0))
            src_name, src_cn, pool_str = _strategy_display(s)
            print("    %s  %s  technical_score=%.2f  策略=%s(%s)  各池得分: %s" % (sym, name or "(无中文名)", score, src_name, src_cn, pool_str or "-"))
        if len(passed_list) > 20:
            print("    ... 共 %s 条" % len(passed_list))
    if signals:
        print("  全量样例（前 10 条，含未通过）：标的 | 中文名 | 得分 | 策略(中文) | 各池得分 | 是否通过")
        for s in signals[:10]:
            sym = s.get("symbol", getattr(s, "symbol", ""))
            name = s.get("symbol_name", "") or ""
            score = s.get("technical_score", getattr(s, "technical_score", 0))
            src_name, src_cn, pool_str = _strategy_display(s)
            p = s.get("passed", False)
            print("    %s  %s  technical_score=%.2f  %s(%s)  %s  passed=%s" % (sym, name or "-", score, src_name, src_cn, pool_str or "-", p))
        if len(signals) > 10:
            print("    ... 共 %s 条" % len(signals))
        # 全部标的详细打分：每标的每策略得分与总得分（便于排查为何多数为 0）
        print()
        print("  全部标的详细打分（每标的各策略池得分与总得分）：")
        print("    %-12s %-10s %8s %8s %8s %8s %8s %-10s %6s" % ("标的", "中文名", "趋势", "反转", "突破", "动量", "总得分", "策略", "通过"))
        print("    " + "-" * 90)
        for s in signals:
            sym = s.get("symbol", getattr(s, "symbol", ""))
            name = (s.get("symbol_name", "") or "")[:8]
            score = s.get("technical_score", getattr(s, "technical_score", 0))
            src_name, src_cn, _ = _strategy_display(s)
            ps = s.get("pool_scores") or {}
            t = float(ps.get(1, 0))
            r = float(ps.get(2, 0))
            b = float(ps.get(3, 0))
            m = float(ps.get(4, 0))
            p = "是" if s.get("passed", False) else "否"
            print("    %-12s %-10s %8.2f %8.2f %8.2f %8.2f %8.2f %-10s %6s" % (sym or "", name or "-", t, r, b, m, score, src_cn, p))
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
    print()

    expect_ok = len(universe) >= 1 and len(signals) >= 0
    if dsn and signals:
        expect_ok = expect_ok and (n_written_scan_all == len(signals) and n_written_snapshot == len(passed_list))
    print("======== 输出是否符合预期 ========  ")
    print("  执行标的数=%s，全量保存=%s，通过=%s；L2 全量写入=%s，通过表写入=%s" % (len(universe), len(signals), len(passed_list), n_written_scan_all, n_written_snapshot))
    print("  是否符合预期: %s" % ("是" if expect_ok else "否（请检查 PG_L2_DSN 或执行 make init-l2-quant-signal-table）"))
    print()
    sys.exit(0 if expect_ok else 1)


if __name__ == "__main__":
    main()
