#!/usr/bin/env python3
# [Ref: 02_B模块策略_策略实现规约 §3.12]
"""Golden batch 回归：无 L1/L2 DSN、PYTHONHASHSEED=0 下对固定标的断言分数区间。退出码 0=通过，1=失败。"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def main() -> int:
    for _k in ("PG_L2_DSN", "TIMESCALE_DSN"):
        os.environ.pop(_k, None)
    hs = os.environ.get("PYTHONHASHSEED", "")
    if hs != "0":
        print(
            "错误: 请设置 PYTHONHASHSEED=0 后执行（mock OHLCV 依赖稳定 hash）。"
            " 示例: PYTHONHASHSEED=0 python3 scripts/golden_scanner_batch.py",
            file=sys.stderr,
        )
        return 1
    fixture = _ROOT / "tests" / "fixtures" / "golden_scanner_batch.json"
    from diting.scanner.golden_batch import validate_fixture_file

    errs = validate_fixture_file(fixture)
    if errs:
        for e in errs:
            print(e, file=sys.stderr)
        return 1
    print("golden_scanner_batch OK: %s" % fixture)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
