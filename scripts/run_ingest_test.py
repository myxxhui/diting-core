#!/usr/bin/env python3
# [Ref: 04_阶段规划与实践/Stage2_数据采集与存储/02_采集逻辑与Dockerfile.md]
# 工作目录: diting-core。执行 ingest_ohlcv、ingest_industry_revenue、ingest_news；退出码 0 表示通过。

import logging
import sys

# 加载项目根目录
from pathlib import Path

root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from diting.ingestion import run_ingest_ohlcv, run_ingest_industry_revenue, run_ingest_news

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    try:
        run_ingest_ohlcv()
        run_ingest_industry_revenue()
        run_ingest_news()
        logger.info("ingest-test OK")
        return 0
    except Exception as e:
        logger.exception("ingest-test failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
