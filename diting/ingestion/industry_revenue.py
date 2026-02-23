# [Ref: 03_原子目标与规约/Stage2_数据采集与存储/02_采集逻辑与Dockerfile设计.md#design-stage2-02-integration-akshare]
# ingest_industry_revenue：AkShare 行业/财报/营收 → 约定表或 L2 版本化（Module A 输入）

import json
import logging
import time
from datetime import datetime, timezone

import psycopg2

import akshare as ak

from diting.ingestion.config import get_pg_l2_dsn
from diting.ingestion.l2_writer import write_data_version

logger = logging.getLogger(__name__)

# ingest-test 目标：至少 1 只标的的财务摘要写入 L2 data_versions
DEFAULT_SYMBOL = "000001"
DATA_TYPE = "industry_revenue"


def _fetch_akshare_financial_abstract(
    symbol: str,
    max_retries: int = 3,
    retry_delay: float = 2.0,
):
    """AkShare 股票财务摘要；错误与限流：重试+退避。"""
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
    工作目录: diting-core
    """
    symbol = symbol or DEFAULT_SYMBOL
    df = _fetch_akshare_financial_abstract(symbol)
    if df is None or df.empty:
        logger.warning("ingest_industry_revenue: no data for symbol=%s", symbol)
        return 0

    # 版本化：data_type + version_id；07_ 规约 version 格式可含 timestamp
    now = datetime.now(timezone.utc)
    version_id = f"industry_revenue_{symbol}_{now.strftime('%Y%m%d%H%M%S')}"
    # 逻辑路径：L2 侧仅存元数据，实际内容可存 L3；此处用占位路径表示「已采集」
    file_path = f"l2/industry_revenue/{symbol}.json"
    # 将摘要首行序列化为占位内容（可选：仅存元数据也可）
    try:
        first = df.iloc[0].to_dict()
        for k, v in first.items():
            if hasattr(v, "isoformat"):
                first[k] = v.isoformat()
        payload = json.dumps(first, ensure_ascii=False, default=str)
        file_size = len(payload.encode("utf-8"))
    except Exception:
        payload = "{}"
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
