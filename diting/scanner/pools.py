# [Ref: 02_B模块策略_策略实现规约] 三大策略池 + 可选动量池；每条件 0–50 连续子分，两条件相加 cap 100
# 含优化项：MA60、趋势/突破确认、反转止跌、多池共振；可选趋势强度、急跌阴跌、突破3日确认、动量池、共振二档

import logging
from typing import Any, Dict, List, Optional, Tuple

from diting.scanner import indicators

logger = logging.getLogger(__name__)

POOL_TREND = 1
POOL_REVERSION = 2
POOL_BREAKOUT = 3
POOL_MOMENTUM = 4
POOL_UNSPECIFIED = 0

_DEFAULT_SCORING = {
    "trend": {"macd_hist_scale_ratio": 0.01},
    "reversion": {"rsi_oversold": 20},
    "breakout": {"price_scale_ratio": 0.02, "volume_cap_multiple": 5.0},
}
_DEFAULT_OPTIMIZATION = {
    "trend_confirm_bars": 3,
    "breakout_confirm_bars": 2,
    "reversion_require_above_ma5": True,
    "multi_pool_min_score": 40.0,
    "multi_pool_bonus": 10.0,
    "trend_adx_enabled": True,
    "trend_adx_min": 25.0,
    "trend_adx_penalty_ratio": 0.6,
    "trend_partial_confirm_enabled": True,
    "trend_partial_confirm_ratio": 0.7,
    "trend_position_strength_enabled": False,
    "reversion_volume_bounce_enabled": True,
    "reversion_volume_bounce_ratio": 1.2,
    "reversion_volume_bounce_bonus": 1.1,
    "reversion_acute_slow_enabled": False,
    "reversion_acute_threshold": -0.08,
    "reversion_slow_threshold": -0.03,
    "reversion_acute_weight": 1.2,
    "reversion_slow_weight": 0.6,
    "breakout_hold_days": 0,
    "breakout_hold_ratio": 0.98,
    "pool_4_momentum_enabled": False,
    "momentum_percentile_threshold": 0.80,
    "fusion_mode": "weighted",
    "fusion_weights": [0.7, 0.2, 0.1],
    "multi_pool_tier2_enabled": False,
    "multi_pool_tier2_threshold": 60.0,
    "multi_pool_tier2_bonus": 5.0,
    "output_score_percentile": True,
}


def _last_valid(values: Optional[List[float]]) -> Optional[float]:
    if not values:
        return None
    for v in reversed(values):
        if v is not None and (isinstance(v, float) and (v == v)):
            return float(v)
    return None


def _at(arr: Any, offset: int) -> Optional[float]:
    """取 array-like 从末尾起 offset 位置的值，offset 为负，-1 为最后一条。"""
    if arr is None:
        return None
    try:
        n = len(arr)
        if n < abs(offset):
            return None
        v = arr[offset]
        return float(v) if v is not None and (v == v) else None
    except (TypeError, IndexError):
        return None


def _last_of(arr: Any) -> Optional[float]:
    return _at(arr, -1)


def _get_scoring_params() -> Dict[str, Any]:
    try:
        from diting.scanner.config_loader import get_scoring_params
        return get_scoring_params()
    except Exception:
        return _DEFAULT_SCORING


def _get_optimization_params() -> Dict[str, Any]:
    try:
        from diting.scanner.config_loader import get_optimization_params
        return get_optimization_params()
    except Exception:
        return _DEFAULT_OPTIMIZATION


def evaluate_trend(
    open_: Any, high: Any, low: Any, close: Any, volume: Any,
    opt: Optional[Dict[str, Any]] = None,
) -> float:
    """趋势池：均线子分(0–50) + MACD 子分(0–50)，cap 100。ADX 弱趋势降权；部分确认(2/3)给比例分。"""
    if not indicators.has_talib():
        return 0.0
    if opt is None:
        opt = _get_optimization_params()
    ma5 = indicators.ma(close, 5)
    ma10 = indicators.ma(close, 10)
    ma20 = indicators.ma(close, 20)
    ma60 = indicators.ma(close, 60)
    macd_res = indicators.macd(close, 12, 26, 9)
    if not macd_res or not ma5 or not ma10 or not ma20 or not ma60:
        return 0.0
    macd_line, signal_line, _ = macd_res
    confirm_bars = opt.get("trend_confirm_bars", 3)
    need_confirm = confirm_bars > 1 and len(ma5) >= confirm_bars
    partial_confirm_enabled = opt.get("trend_partial_confirm_enabled", True)
    partial_ratio = opt.get("trend_partial_confirm_ratio", 0.7)

    def _cond_ma_at(off: int) -> bool:
        m5, m10, m20, m60v = _at(ma5, off), _at(ma10, off), _at(ma20, off), _at(ma60, off)
        if None in (m5, m10, m20, m60v):
            return False
        return m5 > m10 > m20 > m60v

    def _cond_macd_at(off: int) -> bool:
        dif, dea = _at(macd_line, off), _at(signal_line, off)
        if dif is None or dea is None:
            return False
        return dif > dea and dif > 0

    satisfied_count = 0
    if need_confirm:
        satisfied_count = sum(1 for off in range(-confirm_bars, 0) if _cond_ma_at(off) and _cond_macd_at(off))
        if satisfied_count < confirm_bars:
            if partial_confirm_enabled and satisfied_count == confirm_bars - 1:
                pass
            else:
                return 0.0

    m5 = _last_valid(ma5)
    m10 = _last_valid(ma10)
    m20 = _last_valid(ma20)
    m60v = _last_valid(ma60)
    dif = _last_valid(macd_line)
    dea = _last_valid(signal_line)
    c_last = _last_of(close)
    if None in (m5, m10, m20, m60v, dif, dea, c_last) or c_last <= 0:
        return 0.0
    count_ma = (1 if m5 > m10 else 0) + (1 if m10 > m20 else 0) + (1 if m20 > m60v else 0)
    ma_sub = (count_ma / 3.0) * 50.0
    params = _get_scoring_params()
    scale = params.get("trend", {}).get("macd_hist_scale_ratio", 0.01)
    if dif > dea and dif > 0:
        hist = dif - dea
        denom = scale * c_last
        macd_sub = 50.0 * min(1.0, hist / denom) if denom > 0 else 50.0
    else:
        macd_sub = 0.0
    raw = min(100.0, ma_sub + macd_sub)
    if need_confirm and satisfied_count == confirm_bars - 1 and partial_confirm_enabled:
        raw = raw * partial_ratio
    if opt.get("trend_adx_enabled", True):
        adx_vals = indicators.adx(high, low, close, 14)
        if adx_vals:
            adx_last = _last_valid(adx_vals)
            adx_min = opt.get("trend_adx_min", 25.0)
            penalty = opt.get("trend_adx_penalty_ratio", 0.6)
            if adx_last is not None and adx_last < adx_min:
                raw = raw * penalty
    if opt.get("trend_position_strength_enabled"):
        min_c = indicators.min_close(close, 60)
        max_h = indicators.max_high(high, 60)
        min_l = indicators.min_low(low, 60)
        if min_c and max_h and min_l and len(min_c) >= 60:
            min60 = _last_valid(min_c)
            max60 = _last_valid(max_h)
            min_l60 = _last_valid(min_l)
            if min60 is not None and max60 is not None and min_l60 is not None and (max60 - min_l60) > 1e-9:
                position_pct = (c_last - min60) / (max60 - min_l60)
                position_pct = max(0.0, min(1.0, position_pct))
                raw = raw * (0.5 + 0.5 * position_pct)
    return min(100.0, raw)


def evaluate_reversion(
    open_: Any, high: Any, low: Any, close: Any, volume: Any,
    opt: Optional[Dict[str, Any]] = None,
) -> float:
    """反转池：RSI 扩展区间 [oversold, soft_ceiling] 连续子分 + 布林下轨；放量止跌加成；止跌 close>MA5。"""
    if not indicators.has_talib():
        return 0.0
    if opt is None:
        opt = _get_optimization_params()
    rsi_vals = indicators.rsi(close, 14)
    bb = indicators.bbands(close, 20, 2.0, 2.0)
    ma5 = indicators.ma(close, 5)
    if not rsi_vals or not bb:
        return 0.0
    r = _last_valid(rsi_vals)
    upper, middle, lower = bb
    c_last = _last_of(close)
    l_last = _last_valid(lower)
    if r is None or c_last is None or l_last is None or l_last <= 0:
        return 0.0
    params = _get_scoring_params()
    rsi_oversold = params.get("reversion", {}).get("rsi_oversold", 20)
    rsi_soft_ceiling = params.get("reversion", {}).get("rsi_soft_ceiling", 35)
    if r < rsi_oversold:
        rsi_sub = 50.0 * (rsi_oversold - r) / float(rsi_oversold)
    elif r <= rsi_soft_ceiling:
        rsi_sub = 50.0 * (rsi_soft_ceiling - r) / max(1, rsi_soft_ceiling - rsi_oversold)
    else:
        rsi_sub = 0.0
    if c_last <= l_last * 1.01:
        depth = (l_last * 1.01 - c_last) / (l_last * 0.01)
        bb_sub = 50.0 * min(1.0, max(0.0, depth))
    else:
        bb_sub = 0.0
    raw = min(100.0, rsi_sub + bb_sub)
    if opt.get("reversion_require_above_ma5", True) and ma5:
        ma5_last = _last_valid(ma5)
        if ma5_last is not None and c_last <= ma5_last:
            return 0.0
    if opt.get("reversion_volume_bounce_enabled", True):
        vol_sma5 = indicators.sma_volume(volume, 5)
        if vol_sma5:
            v_last = _last_of(volume)
            vs5 = _last_valid(vol_sma5)
            ratio = opt.get("reversion_volume_bounce_ratio", 1.2)
            bonus = opt.get("reversion_volume_bounce_bonus", 1.1)
            if v_last is not None and vs5 is not None and vs5 > 1e-9 and v_last > ratio * vs5:
                raw = min(100.0, raw * bonus)
    if opt.get("reversion_acute_slow_enabled"):
        try:
            clen = len(close) if hasattr(close, "__len__") else 0
            if clen >= 6:
                c_prev = _at(close, -6)
                if c_prev is not None and c_prev > 1e-9:
                    ret_5d = (c_last - c_prev) / c_prev
                    ath, slo = opt.get("reversion_acute_threshold", -0.08), opt.get("reversion_slow_threshold", -0.03)
                    w_acute = opt.get("reversion_acute_weight", 1.2)
                    w_slow = opt.get("reversion_slow_weight", 0.6)
                    if ret_5d < ath:
                        raw = min(100.0, raw * w_acute)
                    elif ret_5d > slo:
                        raw = min(100.0, raw * w_slow)
        except Exception:
            pass
    return raw


def evaluate_breakout(
    open_: Any, high: Any, low: Any, close: Any, volume: Any,
    opt: Optional[Dict[str, Any]] = None,
) -> float:
    """突破池：价格突破子分(0–50) 按 ATR 归一化 + 放量子分(0–50)，cap 100；最近 2 根至少 2 根满足。"""
    if not indicators.has_talib():
        return 0.0
    if opt is None:
        opt = _get_optimization_params()
    max_h = indicators.max_high(high, 20)
    vol_sma = indicators.sma_volume(volume, 20)
    if not max_h or not vol_sma or len(max_h) < 22 or len(vol_sma) < 21:
        return 0.0
    confirm_bars = opt.get("breakout_confirm_bars", 2)
    need_confirm = confirm_bars >= 2 and len(max_h) >= confirm_bars

    def _breakout_ok_at(off: int) -> bool:
        c = _at(close, off)
        v = _at(volume, off)
        mh = _at(max_h, off - 1)
        vs = _at(vol_sma, off)
        if None in (c, v, mh, vs) or vs <= 0:
            return False
        return c > mh and v > 2.0 * vs

    if need_confirm:
        ok_count = sum(1 for off in range(-confirm_bars, 0) if _breakout_ok_at(off))
        if ok_count < confirm_bars:
            return 0.0

    hold_days = opt.get("breakout_hold_days", 0)
    if hold_days >= 2:
        ratio = opt.get("breakout_hold_ratio", 0.98)
        found = False
        for t in range(-hold_days - 1, -len(max_h), -1):
            if not _breakout_ok_at(t):
                continue
            level = _at(max_h, t - 1)
            if level is None or level <= 0:
                continue
            ok_hold = all(
                (_at(close, t + i) is not None and _at(close, t + i) >= level * ratio)
                for i in range(1, hold_days + 1)
            )
            if ok_hold:
                found = True
                break
        if not found:
            return 0.0

    max_high_prev = max_h[-2] if len(max_h) >= 2 else None
    c_last = _last_of(close)
    v_last = _last_of(volume)
    vol_sma_last = _last_valid(vol_sma)
    if max_high_prev is None or c_last is None or v_last is None or vol_sma_last is None or vol_sma_last <= 0:
        return 0.0
    params = _get_scoring_params()
    price_ratio = params.get("breakout", {}).get("price_scale_ratio", 0.02)
    vol_cap = params.get("breakout", {}).get("volume_cap_multiple", 5.0)
    atr_scale_enabled = params.get("breakout", {}).get("atr_scale_enabled", True)
    min_atr_multiple = params.get("breakout", {}).get("min_atr_multiple", 0.5)
    if c_last > max_high_prev:
        denom = price_ratio * max_high_prev
        price_sub = 50.0 * min(1.0, (c_last - max_high_prev) / denom) if denom > 0 else 50.0
        if atr_scale_enabled:
            atr_vals = indicators.atr(high, low, close, 14)
            if atr_vals:
                atr_last = _last_valid(atr_vals)
                if atr_last is not None and atr_last > 1e-9:
                    break_dist = c_last - max_high_prev
                    atr_floor = min_atr_multiple * atr_last
                    price_sub = price_sub * min(1.0, break_dist / atr_floor)
    else:
        price_sub = 0.0
    if v_last > 2.0 * vol_sma_last:
        ratio = v_last / vol_sma_last
        span = max(0.01, vol_cap - 2.0)
        vol_sub = 50.0 * min(1.0, (ratio - 2.0) / span)
    else:
        vol_sub = 0.0
    return min(100.0, price_sub + vol_sub)


def evaluate_momentum(
    open_: Any, high: Any, low: Any, close: Any, volume: Any,
    momentum_20d_percentile: Optional[float] = None,
    opt: Optional[Dict[str, Any]] = None,
) -> float:
    """动量池（第四池）：20 日收益率分位子分(0–50) + close>MA20 子分(0–50)，cap 100。"""
    if not indicators.has_talib():
        return 0.0
    if opt is None:
        opt = _get_optimization_params()
    if not opt.get("pool_4_momentum_enabled"):
        return 0.0
    ma20 = indicators.ma(close, 20)
    if not ma20 or len(ma20) < 21:
        return 0.0
    c_last = _last_of(close)
    ma20_last = _last_valid(ma20)
    if c_last is None or ma20_last is None or c_last <= 0:
        return 0.0
    threshold = opt.get("momentum_percentile_threshold", 0.80)
    if momentum_20d_percentile is not None:
        return_sub = 50.0 * min(1.0, momentum_20d_percentile / threshold) if momentum_20d_percentile >= 0 else 0.0
    else:
        c_21 = _at(close, -21)
        if c_21 is None or c_21 <= 1e-9:
            return_sub = 0.0
        else:
            ret_20d = (c_last - c_21) / c_21
            if ret_20d >= 0.08:
                return_sub = 50.0
            elif ret_20d >= 0.04:
                return_sub = 25.0
            else:
                return_sub = 0.0
    ma_sub = 50.0 if c_last > ma20_last else 0.0
    return min(100.0, return_sub + ma_sub)


def _merge_opt(override: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    base = _get_optimization_params()
    if not override:
        return base
    out = dict(base)
    for k, v in override.items():
        out[k] = v
    return out


def evaluate_pools(
    open_: Any, high: Any, low: Any, close: Any, volume: Any,
    momentum_20d_percentile: Optional[float] = None,
    optimization_override: Optional[Dict[str, Any]] = None,
) -> Tuple[float, int, int, float, Dict[int, float]]:
    """多池连续得分；默认加权融合 w1*s1+w2*s2+w3*s3；多池共振加成；返回 technical_score, strategy_source, second_pool_id, second_pool_score, pool_scores。"""
    opt = _merge_opt(optimization_override)
    t = evaluate_trend(open_, high, low, close, volume, opt=opt)
    mult = float(opt.get("index_regime_trend_mult", 1.0))
    if mult != 1.0:
        t = min(100.0, max(0.0, t * mult))
    r = evaluate_reversion(open_, high, low, close, volume, opt=opt)
    b = evaluate_breakout(open_, high, low, close, volume, opt=opt)
    br_m = float(opt.get("index_regime_breakout_mult", 1.0))
    rev_m = float(opt.get("index_regime_reversion_mult", 1.0))
    if br_m != 1.0:
        b = min(100.0, max(0.0, b * br_m))
    if rev_m != 1.0:
        r = min(100.0, max(0.0, r * rev_m))
    scores = [(t, POOL_TREND), (r, POOL_REVERSION), (b, POOL_BREAKOUT)]
    if opt.get("pool_4_momentum_enabled"):
        m = evaluate_momentum(open_, high, low, close, volume, momentum_20d_percentile, opt=opt)
        scores.append((m, POOL_MOMENTUM))
    sorted_scores = sorted(scores, key=lambda x: -x[0])
    best = sorted_scores[0]
    second = sorted_scores[1] if len(sorted_scores) > 1 else (0.0, POOL_UNSPECIFIED)
    third = sorted_scores[2] if len(sorted_scores) > 2 else (0.0, POOL_UNSPECIFIED)
    pool_id = best[1]
    second_pool_id = second[1]
    second_pool_score = second[0]

    fusion_mode = opt.get("fusion_mode", "weighted")
    if fusion_mode == "weighted":
        w = opt.get("fusion_weights", [0.7, 0.2, 0.1])
        if len(w) < 3:
            w = [0.7, 0.2, 0.1]
        score = w[0] * best[0] + w[1] * second[0] + w[2] * third[0]
    else:
        score = best[0]

    if score <= 0:
        pool_id = POOL_UNSPECIFIED
    else:
        min_score = opt.get("multi_pool_min_score", 40.0)
        bonus = opt.get("multi_pool_bonus", 10.0)
        count_above = sum(1 for sc, _ in scores if sc >= min_score)
        if count_above >= 2:
            score = min(100.0, score + bonus)
        if opt.get("multi_pool_tier2_enabled"):
            tier2_threshold = opt.get("multi_pool_tier2_threshold", 60.0)
            tier2_bonus = opt.get("multi_pool_tier2_bonus", 5.0)
            count_tier2 = sum(1 for sc, _ in scores if sc >= tier2_threshold)
            if count_tier2 >= 2:
                score = min(100.0, score + tier2_bonus)
    pool_scores = {pid: sc for sc, pid in scores}
    return (float(min(100.0, score)), pool_id, second_pool_id, float(second_pool_score), pool_scores)
