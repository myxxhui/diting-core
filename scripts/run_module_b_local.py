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

    # 标的中文名：config/symbol_names.csv 或 diting_symbols.txt 的「symbol,name」；可选 akshare 补全
    from diting.scanner.symbol_names import load_symbol_names, fill_names_from_akshare
    symbol_to_name = load_symbol_names(root=Path(ROOT))
    fill_names_from_akshare(symbol_to_name, list(universe))

    # 可选：从 L2 读取 A 模块最新一批，用于「基于 A 处理过的数据」验证（同批标的一致）
    classifier_batch_id = None
    classifier_symbols = set()
    dsn = (os.environ.get("PG_L2_DSN") or "").strip()
    if dsn:
        classifier_batch_id, classifier_symbols = _read_classifier_batch_from_l2(dsn)
        if classifier_symbols:
            # 与 A 同批：优先使用 L2 中 A 的 batch 标的子集与当前 universe 的交集，或直接用 universe
            pass  # 本步仍用 universe 全量扫描；仅做信息展示

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
    # 阈值来自 config/scanner_rules.yaml technical_score_threshold（默认 70）；三池只产出 0/40/80，故只有 80 分通过
    threshold = getattr(scanner, "_score_threshold", 70)
    print("======== B 模块扫描结果（全量保存当前分数，通过/未通过分开存放）========  ")
    print("  阈值: %s（仅得分≥阈值才通过）；三池打分为 0/40/80，故实际只有 80 分会通过，40 分不会通过" % threshold)
    print("  全量条数: %s（均已打分）  通过阈值条数: %s" % (len(signals), len(passed_list)))
    print("  说明: 得分 0 表示该标的三池（趋势/反转/突破）条件均不满足，并非未打分。")
    if passed_list:
        print("  本批通过阈值的标的（得分≥%s）:" % threshold)
        for s in passed_list[:20]:
            sym = s.get("symbol", getattr(s, "symbol", ""))
            name = s.get("symbol_name", "") or ""
            score = s.get("technical_score", getattr(s, "technical_score", 0))
            src = s.get("strategy_source", getattr(s, "strategy_source", 0))
            print("    %s  %s  technical_score=%.2f  strategy_source=%s" % (sym, name or "(无中文名)", score, src))
        if len(passed_list) > 20:
            print("    ... 共 %s 条" % len(passed_list))
    if signals:
        print("  全量样例（前 10 条，含未通过）：标的 | 中文名 | 得分 | 策略 | 是否通过")
        for s in signals[:10]:
            sym = s.get("symbol", getattr(s, "symbol", ""))
            name = s.get("symbol_name", "") or ""
            score = s.get("technical_score", getattr(s, "technical_score", 0))
            src = s.get("strategy_source", getattr(s, "strategy_source", 0))
            p = s.get("passed", False)
            print("    %s  %s  technical_score=%.2f  strategy_source=%s  passed=%s" % (sym, name or "-", score, src, p))
        if len(signals) > 10:
            print("    ... 共 %s 条（可查 L2 quant_signal_scan_all 看全部分数）" % len(signals))
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
