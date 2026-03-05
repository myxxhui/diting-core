#!/usr/bin/env python3
# [Ref: 04_阶段规划与实践/Stage2_数据采集与存储/06_生产级数据要求_实践.md]
# 生产级日终增量：全 A 股标的、仅补最近 N 天日线，供每个交易日结束后执行；与 06_「每个交易日结束后的增量采集」一致。
# 工作目录: diting-core；需 .env 中 TIMESCALE_DSN 等。建议配合 INGEST_OHLCV_CONCURRENT / INGEST_OHLCV_RATE_PER_SEC 使用。

import logging
import os
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from diting.ingestion import (
    run_ingest_universe,
    run_ingest_ohlcv,
    run_ingest_industry_revenue,
    run_ingest_news,
)
from diting.universe import get_current_a_share_universe

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

# 仅补最近 N 天（默认 7，可配 INGEST_PRODUCTION_INCREMENTAL_DAYS）
DAYS_BACK_DEFAULT = 7


def main() -> int:
    if os.environ.get("DITING_INGEST_MOCK", "").strip().lower() in ("1", "true", "yes"):
        logger.error("生产级增量禁止使用 DITING_INGEST_MOCK=1，请去掉该环境变量后执行")
        return 1
    try:
        import akshare  # noqa: F401
    except ImportError as e:
        logger.exception("增量采集 import akshare 失败: %s", e)
        logger.error("请先执行：make deps-ingest 或 pip install -r requirements-ingest-core.txt")
        return 1

    days_back = int(os.environ.get("INGEST_PRODUCTION_INCREMENTAL_DAYS", str(DAYS_BACK_DEFAULT)).strip() or DAYS_BACK_DEFAULT)
    days_back = max(1, min(365, days_back))

    try:
        logger.info("生产级日终增量 step1: 刷新 universe（全A股）")
        run_ingest_universe()

        symbols_ts = get_current_a_share_universe(force_refresh=False)
        if not symbols_ts:
            logger.error("universe 表无标的，无法执行增量 OHLCV 采集")
            return 1
        symbols_raw = [s.split(".")[0] for s in symbols_ts]
        logger.info("生产级日终增量 step2: 共 %s 只标的，仅补最近 %s 天日线", len(symbols_raw), days_back)

        run_ingest_ohlcv(symbols=symbols_raw, days_back=days_back)

        run_ingest_industry_revenue()
        run_ingest_news()

        logger.info("生产级日终增量完成（全 A 股 + 最近 %s 天）", days_back)
        return 0
    except Exception as e:
        logger.exception("ingest-production-incremental failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
