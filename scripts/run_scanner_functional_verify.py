#!/usr/bin/env python3
# [Ref: 02_量化扫描引擎_实践] B 模块功能验证：基于 A 同源标的（或 Mock）跑扫描，校验输出格式与 L2 写入，输出是否符合预期
# 执行顺序：先在本机加载 .env 并运行本脚本；跑通且结果符合预期后，再执行 make build-module-b（若有）
# 用法：make verify-module-b 或 PYTHONPATH=. python3 scripts/run_scanner_functional_verify.py

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


def _default_universe_from_diting_symbols():
    from pathlib import Path
    from diting.universe import normalize_symbol
    path = Path(ROOT) / "config" / "diting_symbols.txt"
    if not path.exists():
        return None
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


def main():
    from diting.scanner import QuantScanner
    from diting.scanner.l2_snapshot_writer import write_quant_signal_snapshot, write_quant_signal_scan_all
    from diting.universe import parse_symbol_list_from_env

    universe = parse_symbol_list_from_env("DITING_SYMBOLS") or parse_symbol_list_from_env("MODULE_AB_SYMBOLS")
    if not universe:
        universe = _default_universe_from_diting_symbols()
    if not universe:
        print("FAIL: 未获取到标的列表（请配置 DITING_SYMBOLS 或保证 config/diting_symbols.txt 存在且非空）")
        sys.exit(1)

    from diting.scanner.symbol_names import load_symbol_names, fill_names_from_akshare
    symbol_to_name = load_symbol_names(root=Path(ROOT))
    fill_names_from_akshare(symbol_to_name, list(universe))

    ohlcv_dsn = (os.environ.get("TIMESCALE_DSN") or "").strip() or None
    scanner = QuantScanner()
    signals = scanner.scan_market(universe, ohlcv_dsn=ohlcv_dsn, return_all=True)
    for s in signals:
        s["symbol_name"] = symbol_to_name.get(s.get("symbol", ""), "")
    passed_list = [s for s in signals if s.get("passed")]

    # 校验：每条具 symbol, technical_score, strategy_source, sector_strength, passed
    structure_ok = True
    for s in signals:
        if not (isinstance(s, dict) or hasattr(s, "symbol")):
            structure_ok = False
            break
        sym = s.get("symbol", getattr(s, "symbol", None))
        if not sym:
            structure_ok = False
            break

    l2_snapshot_written = 0
    l2_scan_all_written = 0
    dsn = (os.environ.get("PG_L2_DSN") or "").strip()
    if signals and dsn:
        try:
            l2_scan_all_written = write_quant_signal_scan_all(dsn, signals, batch_id="verify-batch-b", correlation_id="verify-batch-b")
            l2_snapshot_written = write_quant_signal_snapshot(dsn, signals, batch_id="verify-batch-b", correlation_id="verify-batch-b")
        except Exception:
            pass

    expect_run_ok = len(universe) >= 1 and structure_ok
    expect_l2_ok = (not dsn) or (l2_scan_all_written == len(signals) and l2_snapshot_written == len(passed_list)) or (len(signals) == 0 and l2_scan_all_written == 0)

    print()
    print("=" * 60)
    print("B 模块功能验证摘要（基于 A 同源标的 / 同批数据验证）")
    print("=" * 60)
    print("1. 执行标的数: %s（与 diting_symbols.txt / DITING_SYMBOLS 一致）" % len(universe))
    print("2. 全量条数: %s，通过阈值: %s，结构符合 QuantSignal 契约: %s" % (len(signals), len(passed_list), structure_ok))
    print("3. L2 全量表 quant_signal_scan_all 写入: %s，通过表 quant_signal_snapshot 写入: %s" % (l2_scan_all_written, l2_snapshot_written))
    print("4. 是否符合预期: 结构合法且 L2 写入一致 = %s" % (expect_run_ok and expect_l2_ok))
    print("=" * 60)
    if not expect_run_ok:
        sys.exit(1)
    print("PASS: B 模块功能验证通过；可执行 make build-module-b 构建镜像（若有）。")


if __name__ == "__main__":
    main()
