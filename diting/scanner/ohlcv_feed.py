# [Ref: 02_量化扫描引擎_实践] [Ref: 02_量化扫描引擎_策略实现规约] OHLCV 数据馈送：L1 或 Mock
# 供 QuantScanner 按标的获取 K 线序列，用于 TA-Lib 指标计算

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    _HAS_NUMPY = False
    np = None  # type: ignore


def get_ohlcv_for_symbol(
    symbol: str,
    period: str = "daily",
    limit: int = 120,
    dsn: Optional[str] = None,
) -> Optional[Tuple[List[float], List[float], List[float], List[float], List[float]]]:
    """
    获取单标的 OHLCV 序列，按时间正序（旧→新）。
    :return: (open, high, low, close, volume) 五个列表，长度一致；不足 limit 或失败时返回 None。
    """
    dsn = dsn or os.environ.get("TIMESCALE_DSN", "").strip()
    if dsn:
        try:
            return _fetch_l1_ohlcv(symbol, period, limit, dsn)
        except Exception as e:
            logger.warning("L1 OHLCV 读取失败 symbol=%s: %s（始终使用采集生产数据，不使用 Mock）", symbol, e)
            return None
    return _mock_ohlcv_arrays(symbol, limit)


def _fetch_l1_ohlcv(
    symbol: str,
    period: str,
    limit: int,
    dsn: str,
) -> Optional[Tuple[List[float], List[float], List[float], List[float], List[float]]]:
    """从 L1 表 ohlcv 读取，按 datetime 升序取最近 limit 条。"""
    try:
        import psycopg2
    except ImportError:
        return None
    # 连接超时 15 秒，避免远程不可达时长时间挂起
    conn = psycopg2.connect(dsn, connect_timeout=15)
    try:
        cur = conn.cursor()
        # 按时间升序，取最近 limit 条（子查询先 DESC 取 limit 再 ORDER BY datetime ASC）
        cur.execute(
            """
            SELECT open, high, low, close, volume
            FROM (
                SELECT datetime, open, high, low, close, volume
                FROM ohlcv
                WHERE symbol = %s AND period = %s
                ORDER BY datetime DESC
                LIMIT %s
            ) t
            ORDER BY datetime ASC
            """,
            (symbol, period, limit),
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()
    if not rows or len(rows) < 20:
        return None
    opens = [float(r[0]) for r in rows]
    highs = [float(r[1]) for r in rows]
    lows = [float(r[2]) for r in rows]
    closes = [float(r[3]) for r in rows]
    volumes = [float(r[4]) for r in rows]
    return (opens, highs, lows, closes, volumes)


def _mock_ohlcv_arrays(symbol: str, bars: int = 80) -> Tuple[List[float], List[float], List[float], List[float], List[float]]:
    """生成确定性 Mock OHLCV，保证指标可算；部分 symbol 哈希可使 RSI/趋势等有条件成立便于单测。"""
    base = 10.0 + (hash(symbol) % 50) / 10.0
    opens, highs, lows, closes, volumes = [], [], [], [], []
    for i in range(bars):
        o = base + i * 0.02 + (hash(symbol + str(i)) % 5) * 0.01
        c = o + (hash(symbol + "c" + str(i)) % 7 - 3) * 0.05
        h = max(o, c) + 0.1
        l = min(o, c) - 0.1
        v = 1_000_000 + (hash(symbol + "v" + str(i)) % 500_000)
        opens.append(o)
        highs.append(h)
        lows.append(l)
        closes.append(c)
        volumes.append(v)
        base = c
    return (opens, highs, lows, closes, volumes)


def get_ohlcv_batch_arrays_for_talib(
    symbols: List[str],
    period: str = "daily",
    limit: int = 120,
    dsn: Optional[str] = None,
) -> Dict[str, Tuple[Any, Any, Any, Any, Any]]:
    """
    批量读取多标的 OHLCV，单次 SQL + 内存分组，降低全市场扫描时的 DB 往返。
    无 DSN、失败或 psycopg2 不可用时返回 {}，调用方回退为逐标的 get_ohlcv_arrays_for_talib。
    """
    if not symbols:
        return {}
    dsn = dsn or os.environ.get("TIMESCALE_DSN", "").strip()
    if not dsn:
        return {}
    try:
        import psycopg2
    except ImportError:
        return {}
    uniq = []
    seen = set()
    for s in symbols:
        u = str(s).strip().upper()
        if u and u not in seen:
            seen.add(u)
            uniq.append(u)
    if not uniq:
        return {}
    out: Dict[str, Tuple[Any, Any, Any, Any, Any]] = {}
    try:
        conn = psycopg2.connect(dsn, connect_timeout=15)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT symbol, open, high, low, close, volume
                FROM (
                    SELECT symbol, open, high, low, close, volume, datetime,
                        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY datetime DESC) AS rn
                    FROM ohlcv
                    WHERE symbol = ANY(%s) AND period = %s
                ) t
                WHERE rn <= %s
                ORDER BY symbol, datetime ASC
                """,
                (uniq, period, limit),
            )
            rows = cur.fetchall()
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("OHLCV 批量读取失败: %s", e)
        return {}
    from collections import defaultdict

    by_sym: Dict[str, List] = defaultdict(list)
    for r in rows:
        sym = str(r[0]).strip().upper()
        by_sym[sym].append((float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])))
    for sym, bars in by_sym.items():
        if len(bars) < 20:
            continue
        opens, highs, lows, closes, vols = zip(*bars)
        o, h, l, c, v = (list(opens), list(highs), list(lows), list(closes), list(vols))
        if _HAS_NUMPY:
            out[sym] = (
                np.asarray(o, dtype=float),
                np.asarray(h, dtype=float),
                np.asarray(l, dtype=float),
                np.asarray(c, dtype=float),
                np.asarray(v, dtype=float),
            )
        else:
            out[sym] = (o, h, l, c, v)
    return out


def get_ohlcv_arrays_for_talib(
    symbol: str,
    period: str = "daily",
    limit: int = 120,
    dsn: Optional[str] = None,
):
    """
    返回可供 TA-Lib 使用的 numpy 数组 (open, high, low, close, volume)。
    若 numpy 不可用则返回列表（TA-Lib 也接受 array-like）。
    """
    raw = get_ohlcv_for_symbol(symbol, period, limit, dsn)
    if not raw:
        return None
    o, h, l, c, v = raw
    if _HAS_NUMPY:
        return (np.asarray(o, dtype=float), np.asarray(h, dtype=float), np.asarray(l, dtype=float),
                np.asarray(c, dtype=float), np.asarray(v, dtype=float))
    return (o, h, l, c, v)
