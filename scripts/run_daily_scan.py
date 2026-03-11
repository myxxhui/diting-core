#!/usr/bin/env python3
# [Ref: 待办_全A股标的池_20260301.md T5] [Ref: 09_/11_ 同批一致]
# 编排层：先调用 get_current_a_share_universe() 一次，将同一 universe 传入 Module A 与 Module B，保证同批一致。
# 工作目录: diting-core

import logging
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from diting.universe import get_current_a_share_universe
from diting.classifier import SemanticClassifier
from diting.scanner import QuantScanner

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    try:
        from diting.universe import parse_symbol_list_from_env
        # 与采集共用一套指定股票：DITING_SYMBOLS；未设置时再读 MODULE_AB_SYMBOLS
        universe = parse_symbol_list_from_env("DITING_SYMBOLS") or parse_symbol_list_from_env("MODULE_AB_SYMBOLS")
        if universe:
            logger.info("指定股票模式: 共 %s 只，传入 Module A 与 B", len(universe))
        else:
            universe = get_current_a_share_universe()
            logger.info("run_daily_scan: len(universe)=%s, passing same list to Module A and B", len(universe))
        classifier_results = SemanticClassifier.run_full(universe=universe)
        scanner_results = QuantScanner.run_full(universe=universe)
        logger.info("run_daily_scan: Module A classified %s, Module B signals %s", len(classifier_results), len(scanner_results))
        return 0
    except Exception as e:
        logger.exception("run_daily_scan failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
