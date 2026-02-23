# [Ref: 03_原子目标与规约/_共享规约/11_数据采集与输入层规约]
# [Ref: diting-infra schemas/sql/01_l1_ohlcv.sql]
# 写入 L1 TimescaleDB 表 ohlcv：(symbol, period, datetime, open, high, low, close, volume)

import logging
from typing import List, Tuple, Any

logger = logging.getLogger(__name__)


def write_ohlcv_batch(
    conn,
    rows: List[Tuple[str, str, Any, float, float, float, float, int]],
) -> int:
    """
    批量写入 ohlcv。每行 (symbol, period, datetime, open, high, low, close, volume)。
    主键 (symbol, period, datetime)，冲突时 ON CONFLICT DO UPDATE。
    """
    if not rows:
        return 0
    sql = """
    INSERT INTO ohlcv (symbol, period, datetime, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, period, datetime) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume
    """
    cur = conn.cursor()
    try:
        cur.executemany(sql, rows)
        conn.commit()
        n = cur.rowcount
        logger.info("write_ohlcv_batch: inserted/updated %s rows", n)
        return n
    finally:
        cur.close()
