# [Ref: 02_B模块策略_策略实现规约] 基准指数 MA 多空 + 可选波动「应力」调制（非预测，仅 regime）

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from diting.scanner import indicators
from diting.scanner.ohlcv_feed import get_ohlcv_arrays_for_talib

logger = logging.getLogger(__name__)


def index_ma_bullish(
    benchmark_symbol: str,
    dsn: Optional[str],
    ma_short: int = 20,
    ma_long: int = 60,
    ohlcv_limit: int = 120,
) -> Optional[bool]:
    """
    指数收盘价简单均线：MA_short > MA_long 视为多头 regime。
    无数据或数据不足时返回 None（调用方应视为不施加熊市乘子，即当作多头）。
    """
    sym = str(benchmark_symbol or "").strip().upper()
    if not sym:
        return None
    dsn = (dsn or os.environ.get("TIMESCALE_DSN", "") or "").strip()
    if not dsn:
        return None
    arr = get_ohlcv_arrays_for_talib(sym, period="daily", limit=max(ohlcv_limit, ma_long + 5), dsn=dsn)
    if not arr or len(arr[3]) < ma_long:
        return None
    _, _, _, c, _ = arr
    closes = [float(x) for x in c if x is not None]
    if len(closes) < ma_long:
        return None
    ma_s = sum(closes[-ma_short:]) / float(ma_short)
    ma_l = sum(closes[-ma_long:]) / float(ma_long)
    return bool(ma_s > ma_l)


def _atr_close_series(high: Any, low: Any, close: Any) -> List[float]:
    atr = indicators.atr(high, low, close, 14)
    if not atr or len(atr) < 2 or len(close) < 2:
        return []
    out: List[float] = []
    n = min(len(atr), len(close))
    for i in range(n):
        try:
            a = atr[i]
            cl = close[i] if hasattr(close, "__getitem__") else None
            if a is not None and cl is not None and float(cl) > 1e-12:
                out.append(float(a) / float(cl))
        except (TypeError, ValueError, IndexError):
            continue
    return out


def compute_index_regime_modifiers(
    ir: Dict[str, Any],
    ohlcv_dsn: Optional[str],
) -> Dict[str, Any]:
    """
    综合指数环境：趋势熊市乘子 + 可选「高波动应力」对突破/反转池的调制。
    单次拉取基准指数 OHLCV，避免重复查询。
    """
    out: Dict[str, Any] = {
        "index_regime_trend_mult": 1.0,
        "index_regime_breakout_mult": 1.0,
        "index_regime_reversion_mult": 1.0,
        "index_ma_bullish": None,
        "index_atr_ratio": None,
        "index_stress_vol": False,
    }
    if not ir or not bool(ir.get("enabled", False)) or not ohlcv_dsn:
        return out
    bench = str(ir.get("benchmark_symbol") or "000300.SH").strip().upper()
    ma_short = int(ir.get("ma_short", 20))
    ma_long = int(ir.get("ma_long", 60))
    bear_mult = float(ir.get("bear_trend_pool_mult", 0.72))
    need_stress = bool(ir.get("stress_vol_enabled", False))
    lb_need = max(ma_long + 5, int(ir.get("stress_lookback_bars", 60)) + 20, 120)
    arr = get_ohlcv_arrays_for_talib(bench, period="daily", limit=lb_need, dsn=ohlcv_dsn)
    if not arr or len(arr[3]) < ma_long:
        return out
    o, h, l, c, _v = arr
    closes = [float(x) for x in c if x is not None]
    if len(closes) < ma_long:
        return out
    ma_s = sum(closes[-ma_short:]) / float(ma_short)
    ma_l = sum(closes[-ma_long:]) / float(ma_long)
    index_bull = bool(ma_s > ma_l)
    out["index_ma_bullish"] = index_bull
    trend_mult = 1.0
    if not index_bull:
        trend_mult = bear_mult
    out["index_regime_trend_mult"] = float(trend_mult)

    if not need_stress:
        return out

    ratios = _atr_close_series(h, l, c)
    lb = max(20, int(ir.get("stress_lookback_bars", 60)))
    if len(ratios) < 15:
        return out
    hist = ratios[-lb:] if len(ratios) >= lb else ratios
    last_ratio = float(ratios[-1])
    out["index_atr_ratio"] = last_ratio
    p = float(ir.get("stress_atr_percentile", 0.82))
    p = max(0.5, min(0.99, p))
    sorted_h = sorted(hist)
    idx_th = int(round((len(sorted_h) - 1) * p))
    thresh = sorted_h[idx_th]
    stress = last_ratio >= thresh
    out["index_stress_vol"] = bool(stress)
    if stress:
        out["index_regime_breakout_mult"] = float(ir.get("stress_breakout_mult", 0.88))
        out["index_regime_reversion_mult"] = float(ir.get("stress_reversion_mult", 1.06))
    return out
