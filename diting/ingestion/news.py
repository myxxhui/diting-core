# [Ref: 03_原子目标与规约/Stage2_数据采集与存储/02_采集逻辑与Dockerfile设计.md#design-stage2-02-integration-akshare]
# [Ref: design-stage2-02-integration-openbb]
# ingest_news：AkShare 国内部分 + OpenBB 国际/宏观 → L2 data_versions

import logging
import time
from datetime import datetime, timezone

import psycopg2

from diting.ingestion.config import get_pg_l2_dsn
from diting.ingestion.l2_writer import write_data_version

logger = logging.getLogger(__name__)

DATA_TYPE_NEWS = "news"


def _fetch_akshare_news(max_retries: int = 3, retry_delay: float = 2.0) -> list:
    """国内：AkShare 财经资讯。优先 js_news；不可用时退化为 stock_news_em（个股新闻）。"""
    for attempt in range(max_retries):
        try:
            import akshare as ak

            if hasattr(ak, "js_news"):
                df = ak.js_news(indicator="最新资讯")
            elif hasattr(ak, "stock_news_em"):
                df = ak.stock_news_em(symbol="000001")
            else:
                df = None
            if df is None or df.empty:
                return []
            return df.to_dict("records")
        except Exception as e:
            logger.warning("akshare news attempt %s failed: %s", attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            else:
                raise
    return []


def _fetch_openbb_macro_or_news(max_retries: int = 2, retry_delay: float = 2.0) -> dict:
    """
    OpenBB 国际/宏观：至少一条到 L2 的写入路径。
    [Ref: design-stage2-02-integration-openbb] Provider 抽象，OpenBB 为默认实现。
    """
    for attempt in range(max_retries):
        try:
            from openbb import obb

            # 宏观：economy.gdp.nominal 或 real（OpenBB Platform 4.x）
            result = obb.economy.gdp.nominal(country="united_states", provider="oecd")
            if result and getattr(result, "results", None):
                return {"source": "openbb", "provider": "oecd", "count": len(result.results)}
            result = obb.economy.gdp.real(country="united_states")
            if result and getattr(result, "results", None):
                return {"source": "openbb", "provider": "real_gdp", "count": len(result.results)}
            return {"source": "openbb", "provider": "none", "count": 0}
        except Exception as e:
            logger.warning("openbb attempt %s failed: %s", attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            else:
                raise
    return {"source": "openbb", "error": "all_retries_failed"}


def run_ingest_news() -> int:
    """
    执行 ingest_news：国内 AkShare + 国际 OpenBB，写入 L2 data_versions。
    工作目录: diting-core
    """
    written = 0
    now = datetime.now(timezone.utc)
    dsn = get_pg_l2_dsn()
    conn = psycopg2.connect(dsn)

    try:
        # 国内：AkShare 最新资讯
        try:
            records = _fetch_akshare_news()
            if records:
                version_id = f"news_akshare_{now.strftime('%Y%m%d%H%M%S')}"
                file_path = "l2/news/akshare_latest.json"
                write_data_version(
                    conn,
                    data_type=DATA_TYPE_NEWS,
                    version_id=version_id,
                    timestamp=now,
                    file_path=file_path,
                    file_size=len(str(records)),
                    checksum="",
                )
                written += 1
        except Exception as e:
            logger.exception("ingest_news akshare failed: %s", e)

        # 国际/宏观：OpenBB
        try:
            meta = _fetch_openbb_macro_or_news()
            version_id = f"news_openbb_{now.strftime('%Y%m%d%H%M%S')}"
            file_path = "l2/news/openbb_macro.json"
            write_data_version(
                conn,
                data_type=DATA_TYPE_NEWS,
                version_id=version_id,
                timestamp=now,
                file_path=file_path,
                file_size=len(str(meta)),
                checksum="",
            )
            written += 1
        except Exception as e:
            logger.exception("ingest_news openbb failed: %s", e)

        return written
    finally:
        conn.close()
