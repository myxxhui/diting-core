# [Ref: 03_原子目标与规约/_共享规约/09_核心模块架构规约] [Ref: 11_数据采集与输入层规约]
# [Ref: 02_B模块策略_策略实现规约] Module B：TA-Lib + 多策略池 + 可选过滤器与调制；产出 technical_score 及 second_pool 等供下游使用

import logging
from typing import Any, Dict, List, Optional, Tuple

from diting.scanner import indicators
from diting.scanner.config_loader import (
    load_scanner_config,
    get_thresholds,
    get_filters_params,
    get_optimization_params,
    get_pass_tightening_params,
    get_long_term_params,
)
from diting.scanner.ohlcv_feed import get_ohlcv_arrays_for_talib
from diting.scanner.pools import evaluate_pools, POOL_MOMENTUM, POOL_TREND, POOL_REVERSION, POOL_BREAKOUT

logger = logging.getLogger(__name__)


def _percentile_rank_by_index(indexed_values: List[Tuple[int, float]]) -> Dict[int, float]:
    """将 (index, value) 按 value 排序得到分位 [0,1]，返回 index -> 分位。"""
    if not indexed_values:
        return {}
    n = len(indexed_values)
    sorted_ = sorted(indexed_values, key=lambda x: (x[1], x[0]))
    return {idx: (r + 1) / n for r, (idx, _) in enumerate(sorted_)}


def _compute_20d_return(close: Any) -> Optional[float]:
    if close is None or (hasattr(close, "__len__") and len(close) < 21):
        return None
    try:
        c_last = close[-1] if hasattr(close, "__getitem__") else None
        c_21 = close[-21] if hasattr(close, "__getitem__") else None
        if c_last is not None and c_21 is not None and c_21 > 1e-9:
            return (float(c_last) - float(c_21)) / float(c_21)
    except (TypeError, IndexError):
        pass
    return None


def _compute_long_term_return(close: Any, lookback: int) -> Optional[float]:
    """B 轨长期动量：lookback 日收益率 (close[-1]/close[-1-lookback] - 1)。[Ref: 06_B轨需求与实现缺口分析]"""
    if close is None or lookback < 1:
        return None
    n = lookback + 1
    if hasattr(close, "__len__") and len(close) < n:
        return None
    try:
        c_last = close[-1] if hasattr(close, "__getitem__") else None
        c_old = close[-n] if hasattr(close, "__getitem__") else None
        if c_last is not None and c_old is not None and float(c_old) > 1e-9:
            return (float(c_last) - float(c_old)) / float(c_old)
    except (TypeError, IndexError):
        pass
    return None


def _compute_liquidity_20d(close: Any, volume: Any) -> Optional[float]:
    """20 日日均成交额（volume*close）作为流动性代理。"""
    if close is None or volume is None:
        return None
    try:
        n = min(len(close), len(volume), 20)
        if n < 1:
            return None
        total = 0.0
        for i in range(-n, 0):
            c = close[i] if hasattr(close, "__getitem__") else None
            v = volume[i] if hasattr(volume, "__getitem__") else None
            if c is not None and v is not None:
                total += float(c) * float(v)
        return total / n
    except (TypeError, IndexError):
        return None


def _compute_atr_ratio(high: Any, low: Any, close: Any, period: int = 14) -> Optional[float]:
    """最后一根 K 线的 ATR(14)/close，用于波动率分位。"""
    atr_vals = indicators.atr(high, low, close, period)
    if not atr_vals or len(atr_vals) < 1:
        return None
    try:
        atr_last = atr_vals[-1]
        c_last = close[-1] if hasattr(close, "__getitem__") else None
        if atr_last is not None and c_last is not None and float(c_last) > 1e-9:
            return float(atr_last) / float(c_last)
    except (TypeError, IndexError):
        pass
    return None


class QuantScanner:
    """
    量化扫描引擎：对 universe 全量扫描，多策略池得分，可选流动性/波动率/板块调制，输出 technical_score、second_pool 等供下游使用。
    """

    def __init__(self, config_path: Optional[str] = None):
        self._config = load_scanner_config(config_path)
        self._score_threshold, self._sector_threshold = get_thresholds(self._config)
        self._filters = get_filters_params(self._config)
        self._opt = get_optimization_params(self._config)
        self._pass_tightening = get_pass_tightening_params(self._config)
        self._long_term = get_long_term_params(self._config)

    def scan_market(
        self,
        universe: List[str],
        ohlcv_dsn: Optional[str] = None,
        correlation_id: str = "",
        return_all: bool = True,
    ) -> List[Any]:
        """
        扫描全市场：OHLCV → 多池得分（含可选动量）→ 可选过滤器与调制 → 输出 technical_score、second_pool_id、second_pool_score、pool_scores、liquidity_score、volatility_ratio 等。
        """
        logger.info("QuantScanner.scan_market: len(universe)=%s, score_threshold=%s", len(universe), self._score_threshold)
        need_momentum_pct = self._opt.get("pool_4_momentum_enabled", False)
        need_liquidity = self._filters.get("liquidity", {}).get("enabled", False)
        need_volatility = self._filters.get("volatility_regime", {}).get("enabled", False)
        need_long_term = self._long_term.get("enabled", True)
        long_term_lookback = self._long_term.get("lookback_days", 120)
        long_term_threshold = self._long_term.get("score_threshold", 0.6)

        # 第一轮：拉取 OHLCV，计算 20d 收益、流动性、ATR 比、长期收益，用于分位
        symbol_data: List[Tuple[str, List[Any], Optional[float], Optional[float], Optional[float], Optional[float]]] = []
        for sym in universe or []:
            arr = get_ohlcv_arrays_for_talib(sym, period="daily", limit=max(120, long_term_lookback + 10), dsn=ohlcv_dsn)
            if not arr or len(arr[0]) < 60:
                continue
            o, h, l, c, v = arr
            ret_20d = _compute_20d_return(c) if need_momentum_pct else None
            liq = _compute_liquidity_20d(c, v) if need_liquidity else None
            atr_r = _compute_atr_ratio(h, l, c, 14) if need_volatility else None
            ret_lt = _compute_long_term_return(c, long_term_lookback) if need_long_term else None
            symbol_data.append((sym, list(arr), ret_20d, liq, atr_r, ret_lt))

        # 按 symbol_data 下标做分位排名，便于按索引取
        indexed_ret = [(i, x[2]) for i, x in enumerate(symbol_data) if x[2] is not None]
        indexed_liq = [(i, x[3]) for i, x in enumerate(symbol_data) if x[3] is not None and x[3] > 0]
        indexed_atr = [(i, x[4]) for i, x in enumerate(symbol_data) if x[4] is not None and x[4] > 0]
        indexed_lt = [(i, x[5]) for i, x in enumerate(symbol_data) if x[5] is not None]
        rank_ret = _percentile_rank_by_index(indexed_ret)
        rank_liq = _percentile_rank_by_index(indexed_liq)
        rank_atr = _percentile_rank_by_index(indexed_atr)
        rank_long_term = _percentile_rank_by_index(indexed_lt) if need_long_term else {}

        vol_cfg = self._filters.get("volatility_regime", {})
        vol_threshold = vol_cfg.get("percentile_threshold", 0.90)
        vol_penalty = vol_cfg.get("penalty_ratio", 0.8)
        liq_cfg = self._filters.get("liquidity", {})
        liq_min = liq_cfg.get("min_score", 0)
        liq_as_filter = liq_cfg.get("as_filter", True)

        out = []
        for idx, (sym, arr, ret_20d, liq, atr_r, ret_lt) in enumerate(symbol_data):
            o, h, l, c, v = arr
            momentum_pct = rank_ret.get(idx, 0.5) if need_momentum_pct else None
            score, pool_id, second_pool_id, second_pool_score, pool_scores = evaluate_pools(
                o, h, l, c, v, momentum_20d_percentile=momentum_pct
            )
            liquidity_score = (rank_liq.get(idx, 0.5) or 0.5) * 100.0 if need_liquidity else 100.0
            if need_liquidity and liq is None:
                liquidity_score = 0.0
            if liq_as_filter and need_liquidity and liquidity_score < liq_min:
                continue
            atr_pct = rank_atr.get(idx, 0.5) if need_volatility and atr_r is not None else None
            volatility_ratio = vol_penalty if (need_volatility and atr_pct is not None and atr_pct > vol_threshold) else 1.0
            if need_liquidity and not liq_as_filter:
                score = min(100.0, score * (liquidity_score / 100.0))
            if need_volatility and volatility_ratio < 1.0:
                score = min(100.0, score * volatility_ratio)
            sector_strength = 1.0
            if self._filters.get("sector_strength", {}).get("enabled"):
                pass
            passed = score >= self._score_threshold and sector_strength >= self._sector_threshold
            if passed and pool_scores:
                ps_vals = sorted((float(v) for v in pool_scores.values() if v is not None), reverse=True)
                lead_min = self._pass_tightening.get("pass_require_lead_pool_min")
                second_min = self._pass_tightening.get("pass_require_second_pool_min")
                if lead_min is not None and ps_vals and ps_vals[0] < lead_min:
                    passed = False
                if passed and second_min is not None:
                    second_score = ps_vals[1] if len(ps_vals) >= 2 else 0.0
                    if second_score < second_min:
                        passed = False
            long_term_score = rank_long_term.get(idx) if need_long_term else None
            long_term_candidate = (
                long_term_score is not None and float(long_term_score) >= long_term_threshold
            ) if need_long_term else False
            item = {
                "symbol": sym,
                "technical_score": float(score),
                "strategy_source": pool_id,
                "sector_strength": sector_strength,
                "correlation_id": correlation_id,
                "passed": passed,
                "second_pool_id": second_pool_id,
                "second_pool_score": float(second_pool_score),
                "pool_scores": dict(pool_scores) if pool_scores else {},
                "liquidity_score": liquidity_score,
                "volatility_ratio": volatility_ratio,
            }
            if need_long_term:
                item["long_term_score"] = float(long_term_score) if long_term_score is not None else None
                item["long_term_candidate"] = long_term_candidate
            else:
                item["long_term_score"] = None
                item["long_term_candidate"] = False
            out.append(item)
        if out and self._opt.get("output_score_percentile", True):
            order = sorted(range(len(out)), key=lambda i: out[i]["technical_score"])
            n_out = len(out)
            for r, i in enumerate(order):
                out[i]["technical_score_percentile"] = (r + 1) / n_out
        else:
            for item in out:
                item.setdefault("technical_score_percentile", None)
        n_passed = sum(1 for x in out if x.get("passed"))
        logger.info("QuantScanner.scan_market: 全量=%s, 通过阈值=%s", len(out), n_passed)
        if return_all:
            return out
        return [x for x in out if x.get("passed")]

    @classmethod
    def run_full(
        cls,
        universe: Optional[List[str]] = None,
        ohlcv_dsn: Optional[str] = None,
        correlation_id: str = "",
    ) -> List[Any]:
        """执行入口：获取标的池后全量扫描。"""
        if universe is None:
            from diting.universe import get_current_a_share_universe, parse_symbol_list_from_env
            universe = parse_symbol_list_from_env("DITING_SYMBOLS") or parse_symbol_list_from_env("MODULE_AB_SYMBOLS")
            if not universe:
                universe = get_current_a_share_universe()
        logger.info("QuantScanner.run_full: len(universe)=%s", len(universe))
        return cls().scan_market(universe, ohlcv_dsn=ohlcv_dsn, correlation_id=correlation_id)
