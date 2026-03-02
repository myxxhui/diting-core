#!/usr/bin/env python3
# [Ref: 04_阶段规划与实践/Stage2_数据采集与存储/02_采集逻辑与Dockerfile_实践.md]
# 工作目录: diting-core。执行 ingest_ohlcv、ingest_industry_revenue、ingest_news、ingest_universe；退出码 0 表示通过。
# 生产数据环境（06_ 步骤 3、7）须使用真实行情数据：执行时不得设置 DITING_INGEST_MOCK；建议 INGEST_FORBID_MOCK=1 make ingest-test 强制校验。

import logging
import os
import sys

# 加载项目根目录
from pathlib import Path

root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from diting.ingestion import (
    run_ingest_ohlcv,
    run_ingest_industry_revenue,
    run_ingest_news,
    run_ingest_universe,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    # 生产数据环境禁止 mock：INGEST_FORBID_MOCK=1 时若设置了 DITING_INGEST_MOCK 则直接失败
    if os.environ.get("INGEST_FORBID_MOCK", "").strip().lower() in ("1", "true", "yes"):
        if os.environ.get("DITING_INGEST_MOCK", "").strip().lower() in ("1", "true", "yes"):
            logger.error("生产数据环境禁止使用 mock 数据，请勿设置 DITING_INGEST_MOCK；须使用真实行情（先 make deps-ingest 或 pip install -r requirements-ingest.txt）")
            return 1
        # 真实行情依赖 akshare
        try:
            import akshare  # noqa: F401
        except ImportError:
            logger.error("真实行情依赖 akshare，请先执行：make deps-ingest 或 pip install -r requirements-ingest.txt")
            return 1
    try:
        run_ingest_ohlcv()
        run_ingest_industry_revenue()
        run_ingest_news()
        run_ingest_universe()
        logger.info("ingest-test OK")
        return 0
    except Exception as e:
        logger.exception("ingest-test failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
