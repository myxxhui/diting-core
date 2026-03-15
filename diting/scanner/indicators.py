# [Ref: 02_量化扫描引擎_实践] [Ref: 02_量化扫描引擎_策略实现规约] TA-Lib 指标封装
# 全部技术指标统一基于 TA-Lib，与设计文档 TA-Lib 接入点一致

import logging
from typing import Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    np = None  # type: ignore
    _HAS_NUMPY = False

try:
    import talib
    _HAS_TALIB = True
except ImportError:
    talib = None  # type: ignore
    _HAS_TALIB = False


def _to_array(x: Any):
    """转为 talib 可接受的类型：0.4.x 需 numpy.ndarray，0.6.x 可接受 list。"""
    if hasattr(x, "__iter__") and not isinstance(x, (str, dict)):
        if _HAS_NUMPY and np is not None:
            return np.asarray(x, dtype=float)
        return list(x)
    return [x] if not _HAS_NUMPY else np.asarray([x], dtype=float)


def has_talib() -> bool:
    return _HAS_TALIB


def ma(close: Any, period: int) -> Optional[List[float]]:
    """均线。TA-Lib: MA(close, timeperiod)."""
    if not _HAS_TALIB:
        return None
    c = _to_array(close)
    if len(c) < period:
        return None
    out = talib.MA(c, timeperiod=period)
    return list(out) if out is not None else None


def macd(close: Any, fast: int = 12, slow: int = 26, signal: int = 9
         ) -> Optional[Tuple[List[float], List[float], List[float]]]:
    """MACD。返回 (macd_line, signal_line, hist)。DIF=macd_line, DEA=signal_line."""
    if not _HAS_TALIB:
        return None
    c = _to_array(close)
    if len(c) < slow + signal:
        return None
    macd_line, signal_line, hist = talib.MACD(c, fastperiod=fast, slowperiod=slow, signalperiod=signal)
    if macd_line is None:
        return None
    return (list(macd_line), list(signal_line), list(hist))


def rsi(close: Any, period: int = 14) -> Optional[List[float]]:
    """RSI(close, 14). 0-100."""
    if not _HAS_TALIB:
        return None
    c = _to_array(close)
    if len(c) < period + 1:
        return None
    out = talib.RSI(c, timeperiod=period)
    return list(out) if out is not None else None


def bbands(close: Any, period: int = 20, nbdevup: float = 2.0, nbdevdn: float = 2.0
           ) -> Optional[Tuple[List[float], List[float], List[float]]]:
    """布林带。返回 (upper, middle, lower)."""
    if not _HAS_TALIB:
        return None
    c = _to_array(close)
    if len(c) < period:
        return None
    u, m, l = talib.BBANDS(c, timeperiod=period, nbdevup=nbdevup, nbdevdn=nbdevdn)
    if u is None:
        return None
    return (list(u), list(m), list(l))


def max_high(high: Any, period: int) -> Optional[List[float]]:
    """N 日最高价。TA-Lib: MAX(high, timeperiod). 当前 bar 的 MAX 含当前 bar。"""
    if not _HAS_TALIB:
        return None
    h = _to_array(high)
    if len(h) < period:
        return None
    out = talib.MAX(h, timeperiod=period)
    return list(out) if out is not None else None


def sma_volume(volume: Any, period: int = 20) -> Optional[List[float]]:
    """成交量均线。TA-Lib: SMA(volume, timeperiod)."""
    if not _HAS_TALIB:
        return None
    v = _to_array(volume)
    if len(v) < period:
        return None
    out = talib.SMA(v, timeperiod=period)
    return list(out) if out is not None else None


def atr(high: Any, low: Any, close: Any, period: int = 14) -> Optional[List[float]]:
    """ATR(high, low, close, period)。用于波动率 regime。"""
    if not _HAS_TALIB:
        return None
    h, l, c = _to_array(high), _to_array(low), _to_array(close)
    if len(h) < period or len(l) < period or len(c) < period:
        return None
    out = talib.ATR(h, l, c, timeperiod=period)
    return list(out) if out is not None else None


def min_close(close: Any, period: int) -> Optional[List[float]]:
    """N 日收盘价最低。TA-Lib: MIN(close, timeperiod)。用于 60 日高低区间。"""
    if not _HAS_TALIB:
        return None
    c = _to_array(close)
    if len(c) < period:
        return None
    out = talib.MIN(c, timeperiod=period)
    return list(out) if out is not None else None


def min_low(low: Any, period: int) -> Optional[List[float]]:
    """N 日最低价最低。TA-Lib: MIN(low, timeperiod)。用于 60 日区间分母。"""
    if not _HAS_TALIB:
        return None
    l = _to_array(low)
    if len(l) < period:
        return None
    out = talib.MIN(l, timeperiod=period)
    return list(out) if out is not None else None


def adx(high: Any, low: Any, close: Any, period: int = 14) -> Optional[List[float]]:
    """ADX(high, low, close, period)。趋势强度，用于趋势池过滤弱趋势。"""
    if not _HAS_TALIB:
        return None
    h, l, c = _to_array(high), _to_array(low), _to_array(close)
    if len(h) < period or len(l) < period or len(c) < period:
        return None
    out = talib.ADX(h, l, c, timeperiod=period)
    return list(out) if out is not None else None
