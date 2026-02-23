# [Ref: 03_原子目标与规约/_共享规约/11_数据采集与输入层规约]
# [Ref: 03_原子目标与规约/Stage2_数据采集与存储/02_采集逻辑与Dockerfile设计.md]
# 采集任务：ingest_ohlcv、ingest_industry_revenue、ingest_news

from diting.ingestion.ohlcv import run_ingest_ohlcv
from diting.ingestion.industry_revenue import run_ingest_industry_revenue
from diting.ingestion.news import run_ingest_news

__all__ = [
    "run_ingest_ohlcv",
    "run_ingest_industry_revenue",
    "run_ingest_news",
]
