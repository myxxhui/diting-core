#!/usr/bin/env python3
# [Ref: 04_阶段规划与实践/Stage2_数据采集与存储/06_生产级数据要求_实践.md]
# 全量生产级数据采集：先刷新全 A 股 universe，再按 universe 标的拉取单标≥5 年日线写入 L1；
# 与 06_ 设计、11_ 规约一致。步骤 8 必须执行本脚本（或 make ingest-production），不得以 ingest-test 代替。
# 工作目录: diting-core；需 .env 中 TIMESCALE_DSN 等。禁止 DITING_INGEST_MOCK=1 下执行（mock 不满足 5 年深度）。

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

# 5 年日线：按日历天约 5*365，保证≥1260 交易日
DAYS_BACK_5_YEARS = 5 * 365


def main() -> int:
    if os.environ.get("DITING_INGEST_MOCK", "").strip().lower() in ("1", "true", "yes"):
        logger.error("全量生产级采集禁止使用 DITING_INGEST_MOCK=1，请去掉该环境变量后执行")
        return 1
    try:
        import akshare  # noqa: F401
    except ImportError as e:
        logger.exception("全量采集 import akshare 失败（镜像内须已 pip install akshare + requirements-ingest-core.txt）: %s", e)
        logger.error("全量采集依赖 akshare，请先执行：make deps-ingest 或 pip install -r requirements-ingest-core.txt")
        return 1

    try:
        # ① 先刷新全 A 股标的池（universe 表）
        logger.info("全量采集 step1: 刷新 universe（全A股）")
        run_ingest_universe()

        # ② 从表内读取当前全 A 股标的
        symbols_ts = get_current_a_share_universe(force_refresh=False)
        if not symbols_ts:
            logger.error("universe 表无标的，无法执行全量 OHLCV 采集")
            return 1
        # akshare 接口需要纯代码，去掉 .SH/.SZ 后缀
        symbols_raw = [s.split(".")[0] for s in symbols_ts]
        logger.info("全量采集 step2: 共 %s 只标的，拉取单标≥5 年日线", len(symbols_raw))

        # ③ 全量 OHLCV：全 A 股 + 5 年深度
        run_ingest_ohlcv(symbols=symbols_raw, days_back=DAYS_BACK_5_YEARS)

        # ④ 行业/新闻等与 ingest-test 一致
        run_ingest_industry_revenue()
        run_ingest_news()

        logger.info("全量采集完成（全 A 股 + 单标≥5 年日线）")
        return 0
    except Exception as e:
        logger.exception("ingest-production failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
