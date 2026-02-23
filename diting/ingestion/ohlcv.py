# [Ref: 03_原子目标与规约/Stage2_数据采集与存储/02_采集逻辑与Dockerfile设计.md#design-stage2-02-integration-akshare]
# ingest_ohlcv：AkShare A 股日线 → L1 ohlcv

import logging
from datetime import datetime, timedelta

import psycopg2

from diting.ingestion.config import get_timescale_dsn
from diting.ingestion.l1_writer import write_ohlcv_batch

logger = logging.getLogger(__name__)

# 逻辑填充期：ingest-test 目标 symbol 与 period（见 docs/ingest-test-target.md）
DEFAULT_SYMBOLS = ["000001", "600000"]  # 平安银行(SZ)、浦发银行(SH)
DEFAULT_PERIOD = "daily"


def _symbol_to_ts(symbol: str) -> str:
    """A 股代码转 exchange 后缀：6xxxxx -> .SH，否则 .SZ"""
    if symbol.startswith("6"):
        return f"{symbol}.SH"
    return f"{symbol}.SZ"


def _fetch_akshare_ohlcv(
    symbol: str,
    period: str,
    start_date: str,
    end_date: str,
    adjust: str = "",
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> list:
    """
    AkShare 拉取 A 股日线。接口边界与限流：重试 + 退避。
    [Ref: design-stage2-02-integration-akshare]
    """
    import time

    import akshare as ak

    for attempt in range(max_retries):
        try:
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            )
            if df is None or df.empty:
                return []
            # 列名：日期, 开盘, 收盘, 最高, 最低, 成交量, ...
            df = df.rename(
                columns={
                    "日期": "date",
                    "开盘": "open",
                    "收盘": "close",
                    "最高": "high",
                    "最低": "low",
                    "成交量": "volume",
                }
            )
            rows = []
            for _, r in df.iterrows():
                dt = r["date"]
                if hasattr(dt, "to_pydatetime"):
                    dt = dt.to_pydatetime()
                elif isinstance(dt, str):
                    dt = datetime.strptime(dt[:10], "%Y-%m-%d")
                else:
                    from datetime import date as date_type
                    if isinstance(dt, date_type) and not isinstance(dt, datetime):
                        dt = datetime.combine(dt, datetime.min.time())
                # 确保 timezone-aware（UTC 存）
                if getattr(dt, "tzinfo", None) is None:
                    from datetime import timezone
                    dt = dt.replace(tzinfo=timezone.utc)
                symbol_ts = _symbol_to_ts(symbol)
                vol = r["volume"]
                try:
                    vol_int = int(float(vol)) if vol == vol else 0
                except (TypeError, ValueError):
                    vol_int = 0
                rows.append(
                    (
                        symbol_ts,
                        DEFAULT_PERIOD,
                        dt,
                        float(r["open"]),
                        float(r["high"]),
                        float(r["low"]),
                        float(r["close"]),
                        vol_int,
                    )
                )
            return rows
        except Exception as e:
            logger.warning("akshare stock_zh_a_hist attempt %s failed: %s", attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            else:
                raise
    return []


def run_ingest_ohlcv(
    symbols: list = None,
    period: str = DEFAULT_PERIOD,
    days_back: int = 30,
) -> int:
    """
    执行 ingest_ohlcv：从 AkShare 拉取 A 股日线并写入 L1 ohlcv。
    工作目录: diting-core（由 Makefile / 调用方保证）
    """
    symbols = symbols or DEFAULT_SYMBOLS
    end = datetime.utcnow()
    start = end - timedelta(days=days_back)
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")

    all_rows = []
    for sym in symbols:
        try:
            rows = _fetch_akshare_ohlcv(sym, period, start_str, end_str)
            all_rows.extend(rows)
        except Exception as e:
            logger.exception("ingest_ohlcv symbol=%s failed: %s", sym, e)
            raise

    if not all_rows:
        logger.warning("ingest_ohlcv: no rows fetched for symbols=%s", symbols)
        return 0

    dsn = get_timescale_dsn()
    conn = psycopg2.connect(dsn)
    try:
        n = write_ohlcv_batch(conn, all_rows)
        return n
    finally:
        conn.close()
