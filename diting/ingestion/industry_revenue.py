# [Ref: 03_原子目标与规约/Stage2_数据采集与存储/02_采集逻辑与Dockerfile设计.md#design-stage2-02-integration-akshare]
# ingest_industry_revenue：AkShare 行业/财报/营收 → 约定表或 L2 版本化（Module A 输入）

import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2

from diting.ingestion.config import get_pg_l2_dsn
from diting.ingestion.l2_writer import write_data_version

logger = logging.getLogger(__name__)

# ingest-test 目标：至少 1 只标的的财务摘要写入 L2 data_versions
DEFAULT_SYMBOL = "000001"
DATA_TYPE = "industry_revenue"


def _is_mock() -> bool:
    return os.environ.get("DITING_INGEST_MOCK", "").strip().lower() in ("1", "true", "yes")


def _fetch_akshare_financial_abstract(
    symbol: str,
    max_retries: int = 3,
    retry_delay: float = 2.0,
):
    """AkShare 股票财务摘要；错误与限流：重试+退避。"""
    import akshare as ak

    for attempt in range(max_retries):
        try:
            df = ak.stock_financial_abstract(symbol=symbol)
            return df
        except Exception as e:
            logger.warning("akshare stock_financial_abstract attempt %s failed: %s", attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            else:
                raise
    return None


def run_ingest_industry_revenue(symbol: str = None) -> int:
    """
    执行 ingest_industry_revenue：从 AkShare 拉取财务摘要并写入 L2 data_versions。
    工作目录: diting-core。DITING_INGEST_MOCK=1 时写入一条 mock 版本。
    """
    symbol = symbol or DEFAULT_SYMBOL
    now = datetime.now(timezone.utc)
    version_id = f"industry_revenue_{symbol}_{now.strftime('%Y%m%d%H%M%S')}"
    file_path = f"l2/industry_revenue/{symbol}.json"

    if _is_mock():
        file_size = len(b'{"mock": true}')
        dsn = get_pg_l2_dsn()
        conn = psycopg2.connect(dsn)
        try:
            write_data_version(
                conn,
                data_type=DATA_TYPE,
                version_id=version_id,
                timestamp=now,
                file_path=file_path,
                file_size=file_size,
                checksum="",
            )
            logger.info("ingest_industry_revenue: mock mode, 1 version")
            return 1
        finally:
            conn.close()
    else:
        df = _fetch_akshare_financial_abstract(symbol)
        if df is None or df.empty:
            logger.warning("ingest_industry_revenue: no data for symbol=%s", symbol)
            return 0
        try:
            first = df.iloc[0].to_dict()
            for k, v in first.items():
                if hasattr(v, "isoformat"):
                    first[k] = v.isoformat()
            payload = json.dumps(first, ensure_ascii=False, default=str)
            file_size = len(payload.encode("utf-8"))
        except Exception:
            file_size = 0
        dsn = get_pg_l2_dsn()
        conn = psycopg2.connect(dsn)
        try:
            write_data_version(
                conn,
                data_type=DATA_TYPE,
                version_id=version_id,
                timestamp=now,
                file_path=file_path,
                file_size=file_size,
                checksum="",
            )
            return 1
        finally:
            conn.close()
