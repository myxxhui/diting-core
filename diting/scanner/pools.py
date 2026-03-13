# [Ref: 02_量化扫描引擎_实践] [Ref: 02_量化扫描引擎_策略实现规约] 三大策略池判定与 technical_score
# 趋势 / 反转 / 突破；指标 100% 来自 TA-Lib（indicators.py）

import logging
from typing import Any, Dict, List, Optional, Tuple

from diting.scanner import indicators

logger = logging.getLogger(__name__)

# Proto StrategyPool: 0=UNSPECIFIED, 1=TREND, 2=REVERSION, 3=BREAKOUT
POOL_TREND = 1
POOL_REVERSION = 2
POOL_BREAKOUT = 3


def _last_valid(values: Optional[List[float]]) -> Optional[float]:
    if not values:
        return None
    for v in reversed(values):
        if v is not None and (isinstance(v, float) and (v == v)):  # not NaN
            return float(v)
    return None


def _last_of(arr: Any) -> Optional[float]:
    """取 array-like 的最后一个元素。"""
    if arr is None:
        return None
    try:
        n = len(arr)
        if n == 0:
            return None
        v = arr[n - 1]
        return float(v) if v is not None and (v == v) else None
    except (TypeError, IndexError):
        return None


def evaluate_trend(open_: Any, high: Any, low: Any, close: Any, volume: Any) -> int:
    """
    趋势池：MA5>MA10>MA20 且 MACD 水上金叉（DIF>DEA 且 MACD>0）。
    两条件均满足 80，满足其一 40，否则 0。
    """
    if not indicators.has_talib():
        return 0
    ma5 = indicators.ma(close, 5)
    ma10 = indicators.ma(close, 10)
    ma20 = indicators.ma(close, 20)
    macd_res = indicators.macd(close, 12, 26, 9)
    if not macd_res or not ma5 or not ma10 or not ma20:
        return 0
    macd_line, signal_line, _ = macd_res
    m5 = _last_valid(ma5)
    m10 = _last_valid(ma10)
    m20 = _last_valid(ma20)
    dif = _last_valid(macd_line)
    dea = _last_valid(signal_line)
    if None in (m5, m10, m20, dif, dea):
        return 0
    cond_ma = m5 > m10 > m20
    cond_macd = dif > dea and dif > 0
    if cond_ma and cond_macd:
        return 80
    if cond_ma or cond_macd:
        return 40
    return 0


def evaluate_reversion(open_: Any, high: Any, low: Any, close: Any, volume: Any) -> int:
    """
    反转池：RSI<30 或 收盘价触及布林下轨（close <= lower*1.01）。
    两条件均满足 80，满足其一 40，否则 0。
    """
    if not indicators.has_talib():
        return 0
    rsi_vals = indicators.rsi(close, 14)
    bb = indicators.bbands(close, 20, 2.0, 2.0)
    if not rsi_vals or not bb:
        return 0
    r = _last_valid(rsi_vals)
    upper, middle, lower = bb
    c_last = _last_of(close)
    l_last = _last_valid(lower)
    if r is None or c_last is None or l_last is None:
        return 0
    cond_rsi = r < 30
    cond_bb = c_last <= l_last * 1.01
    if cond_rsi and cond_bb:
        return 80
    if cond_rsi or cond_bb:
        return 40
    return 0


def evaluate_breakout(open_: Any, high: Any, low: Any, close: Any, volume: Any) -> int:
    """
    突破池：收盘价 > 前 20 日最高（不含当日）；成交量 > 2*SMA(volume,20)。
    MAX(high,20) 在 -2 位置为前 20 日最高（不含当前 bar）；close 取最后一条。
    """
    if not indicators.has_talib():
        return 0
    max_h = indicators.max_high(high, 20)
    vol_sma = indicators.sma_volume(volume, 20)
    if not max_h or not vol_sma or len(max_h) < 22 or len(vol_sma) < 21:
        return 0
    # 前一日看到的 20 日最高：取 max_high 的倒数第二个值（对应不含当前 bar 的 20 日最高）
    max_high_prev = max_h[-2] if len(max_h) >= 2 else None
    c_last = _last_of(close)
    v_last = _last_of(volume)
    vol_sma_last = _last_valid(vol_sma)
    if max_high_prev is None or c_last is None or v_last is None or vol_sma_last is None or vol_sma_last <= 0:
        return 0
    cond_price = c_last > max_high_prev
    cond_vol = v_last > 2.0 * vol_sma_last
    if cond_price and cond_vol:
        return 80
    if cond_price or cond_vol:
        return 40
    return 0


def evaluate_pools(
    open_: Any, high: Any, low: Any, close: Any, volume: Any
) -> Tuple[int, int]:
    """
    对一条 OHLCV 序列计算三池得分，返回 (technical_score 0-100, strategy_source 0|1|2|3)。
    当三池均为 0 时 strategy_source 为 0（UNSPECIFIED）。
    """
    t = evaluate_trend(open_, high, low, close, volume)
    r = evaluate_reversion(open_, high, low, close, volume)
    b = evaluate_breakout(open_, high, low, close, volume)
    best = max((t, POOL_TREND), (r, POOL_REVERSION), (b, POOL_BREAKOUT), key=lambda x: x[0])
    score, pool_id = best[0], best[1]
    if score == 0:
        pool_id = 0  # UNSPECIFIED
    return (score, pool_id)
