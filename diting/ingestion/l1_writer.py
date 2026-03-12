# [Ref: 03_原子目标与规约/_共享规约/11_数据采集与输入层规约]
# [Ref: diting-infra schemas/sql/01_l1_ohlcv.sql]
# 写入 L1 TimescaleDB 表 ohlcv：(symbol, period, datetime, open, high, low, close, volume)

import logging
from typing import List, Tuple, Any

logger = logging.getLogger(__name__)


def _normalize_symbol(sym: str) -> str:
    """确保 symbol 带 .SH/.SZ 后缀：6 开头 → .SH，其余 → .SZ。"""
    s = (sym or "").strip()
    if not s:
        return s
    if ".SH" in s or ".SZ" in s:
        return s
    code = s.split(".")[0]
    if code.startswith("6") or code.startswith("58") or code.startswith("51") or code.startswith("50"):
        return f"{code}.SH"
    return f"{code}.SZ"


def write_ohlcv_batch(
    conn,
    rows: List[Tuple[str, str, Any, float, float, float, float, int]],
) -> int:
    """
    批量写入 ohlcv。每行 (symbol, period, datetime, open, high, low, close, volume)。
    主键 (symbol, period, datetime)，冲突时 ON CONFLICT DO UPDATE。
    写入前自动规范化 symbol（补 .SH/.SZ 后缀），保证格式一致。
    """
    if not rows:
        return 0
    normalized = [(_normalize_symbol(r[0]), r[1], r[2], r[3], r[4], r[5], r[6], r[7]) for r in rows]
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
        cur.executemany(sql, normalized)
        conn.commit()
        n = cur.rowcount
        logger.info("write_ohlcv_batch: inserted/updated %s rows", n)
        return n
    finally:
        cur.close()
