# [Ref: 03_原子目标与规约/_共享规约/09_核心模块架构规约] [Ref: 11_数据采集与输入层规约]
# [Ref: 02_B模块策略_策略实现规约] Module B：TA-Lib + 多策略池 + 可选过滤器与调制；产出 technical_score 及 second_pool 等供下游使用

import logging
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from diting.scanner import indicators
from diting.scanner.config_fingerprint import compute_scanner_rules_fingerprint
from diting.scanner.config_loader import (
    load_scanner_config,
    get_filters_params,
    get_optimization_params,
    get_pass_tightening_params,
    get_long_term_params,
    get_a_track_short_params,
    get_scanner_performance_params,
    get_product_signals_params,
)
from diting.scanner.industry_map import fetch_symbol_industry_map
from diting.scanner.ohlcv_feed import get_ohlcv_arrays_for_talib, get_ohlcv_batch_arrays_for_talib
from diting.scanner.classifier_gate import allowed_symbols_by_classifier, resolve_scanner_classifier_batch_id
from diting.scanner.index_regime import compute_index_regime_modifiers
from diting.scanner.pools import evaluate_pools
from diting.scanner.risk_levels import compute_a_track_risk_levels
from diting.scanner.scanner_metrics import ScannerRunMetrics
from diting.scanner.scan_input_fingerprint import fetch_l1_ohlcv_max_ts_batch, fetch_l2_news_max_ts_batch
from diting.scanner.signal_cooldown import symbols_in_signal_cooldown

logger = logging.getLogger(__name__)


def _merge_pass_tightening(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k in ("pass_require_lead_pool_min", "pass_require_second_pool_min"):
        v = override.get(k)
        if v is not None:
            out[k] = v
    return out


def _tightening_ok(pool_scores: Dict[int, float], pass_tightening: Dict[str, Any]) -> bool:
    ps_vals = sorted((float(v) for v in (pool_scores or {}).values() if v is not None), reverse=True)
    lead_min = pass_tightening.get("pass_require_lead_pool_min")
    second_min = pass_tightening.get("pass_require_second_pool_min")
    if lead_min is not None and ps_vals and ps_vals[0] < lead_min:
        return False
    if second_min is not None:
        second_score = ps_vals[1] if len(ps_vals) >= 2 else 0.0
        if second_score < second_min:
            return False
    return True


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


def _sector_strength_ratios(
    technical_scores: List[float],
    symbols: List[str],
    industry_by_symbol: Dict[str, str],
    enabled: bool,
    unmapped_sector_strength: float = 1.0,
) -> Tuple[List[float], List[Optional[bool]]]:
    """
    同行业截面：sector_strength = technical_score / max(同行业 technical_score 均值, ε)。
    无行业映射时使用 unmapped_sector_strength（默认 1.0，与历史一致）；并返回 industry_mapped 供下游展示。
    """
    n = len(technical_scores)
    if not enabled or n == 0:
        return [1.0] * n, [None] * n
    umap = float(unmapped_sector_strength)
    by_ind: Dict[str, List[int]] = defaultdict(list)
    for i, sym in enumerate(symbols):
        ind = (industry_by_symbol.get(str(sym).strip().upper(), "") or "").strip()
        if ind:
            by_ind[ind].append(i)
    means: Dict[str, float] = {}
    for ind, idxs in by_ind.items():
        means[ind] = sum(technical_scores[j] for j in idxs) / len(idxs)
    out: List[float] = []
    mapped_flags: List[Optional[bool]] = []
    for i, sym in enumerate(symbols):
        ts = float(technical_scores[i])
        ind = (industry_by_symbol.get(str(sym).strip().upper(), "") or "").strip()
        if not ind:
            out.append(umap)
            mapped_flags.append(False)
            continue
        m = means.get(ind, ts)
        out.append(ts / max(m, 1e-6))
        mapped_flags.append(True)
    return out, mapped_flags


class QuantScanner:
    """
    量化扫描引擎：对 universe 全量扫描，多策略池得分，可选流动性/波动率/板块调制，输出 technical_score、second_pool 等供下游使用。
    """

    def __init__(self, config_path: Optional[str] = None):
        self._config = load_scanner_config(config_path)
        self._scanner_rules_fingerprint = compute_scanner_rules_fingerprint(
            Path(config_path) if config_path else None
        )
        self._a_track = get_a_track_short_params(self._config)
        self._score_threshold = int(self._a_track["confirmed_threshold"])
        self._sector_threshold = float(self._a_track["sector_strength_threshold"])
        self._filters = get_filters_params(self._config)
        self._opt = get_optimization_params(self._config)
        self._pass_tightening = _merge_pass_tightening(
            get_pass_tightening_params(self._config),
            self._a_track.get("pass_tightening_override") or {},
        )
        self._long_term = get_long_term_params(self._config)
        self._opt_override = self._a_track.get("optimization_override") or {}
        _risk = self._a_track.get("risk") or {}
        self._vol_tier_for_risk = _risk.get("volatility_tier") or {}
        self._need_atr_for_risk = bool(self._vol_tier_for_risk.get("enabled", False))
        self._perf = get_scanner_performance_params(self._config)
        self._product = get_product_signals_params(self._config)
        self.last_scan_pipeline: Optional[Dict[str, Any]] = None
        self.last_scan_metrics: Optional[Dict[str, Any]] = None

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
        cs_cfg = self._opt.get("coarse_screen") or {}
        coarse_on = bool(cs_cfg.get("enabled", False))
        min_liq_coarse = float(cs_cfg.get("min_liquidity_percentile", 0.0) or 0.0)
        need_20d_rank = bool(need_momentum_pct or coarse_on)
        need_liquidity = self._filters.get("liquidity", {}).get("enabled", False)
        need_liq_for_coarse = bool(need_liquidity or (coarse_on and min_liq_coarse > 0))
        need_volatility = self._filters.get("volatility_regime", {}).get("enabled", False)
        need_long_term = self._long_term.get("enabled", True)
        long_term_lookback = self._long_term.get("lookback_days", 120)
        long_term_threshold = self._long_term.get("score_threshold", 0.6)

        metrics = ScannerRunMetrics()
        metrics.universe_in = len(universe or [])
        t_total0 = time.perf_counter()

        # 第一轮：拉取 OHLCV（有 DSN 时优先批量 SQL，减少往返），计算 20d 收益、流动性、ATR 比、长期收益，用于分位
        batch_limit = max(120, long_term_lookback + 10)
        batch_ohlcv: Dict[str, Any] = {}
        t_fetch0 = time.perf_counter()
        if ohlcv_dsn:
            batch_ohlcv = get_ohlcv_batch_arrays_for_talib(
                list(universe or []), period="daily", limit=batch_limit, dsn=ohlcv_dsn
            )
        symbol_data: List[Tuple[str, List[Any], Optional[float], Optional[float], Optional[float], Optional[float]]] = []
        for sym in universe or []:
            key = str(sym).strip().upper()
            arr = batch_ohlcv.get(key) if batch_ohlcv else None
            if arr is None:
                arr = get_ohlcv_arrays_for_talib(sym, period="daily", limit=batch_limit, dsn=ohlcv_dsn)
            if not arr or len(arr[0]) < 60:
                continue
            o, h, l, c, v = arr
            ret_20d = _compute_20d_return(c) if need_20d_rank else None
            liq = _compute_liquidity_20d(c, v) if need_liq_for_coarse else None
            need_atr_ratio = need_volatility or self._need_atr_for_risk
            atr_r = _compute_atr_ratio(h, l, c, 14) if need_atr_ratio else None
            ret_lt = _compute_long_term_return(c, long_term_lookback) if need_long_term else None
            symbol_data.append((sym, list(arr), ret_20d, liq, atr_r, ret_lt))
        metrics.ms_fetch_batch_ohlcv = (time.perf_counter() - t_fetch0) * 1000.0

        l2_dsn = (os.environ.get("PG_L2_DSN") or "").strip() or (ohlcv_dsn or "")
        _sym_fp = [s[0] for s in symbol_data]
        _fp_ohlcv = fetch_l1_ohlcv_max_ts_batch(_sym_fp, ohlcv_dsn, period="daily") if ohlcv_dsn else None
        _fp_news = fetch_l2_news_max_ts_batch(_sym_fp, l2_dsn) if l2_dsn else {}

        # 按 symbol_data 下标做分位排名，便于按索引取
        t_rank0 = time.perf_counter()
        indexed_ret = [(i, x[2]) for i, x in enumerate(symbol_data) if x[2] is not None]
        indexed_liq = [(i, x[3]) for i, x in enumerate(symbol_data) if x[3] is not None and x[3] > 0]
        indexed_atr = [(i, x[4]) for i, x in enumerate(symbol_data) if x[4] is not None and x[4] > 0]
        indexed_lt = [(i, x[5]) for i, x in enumerate(symbol_data) if x[5] is not None]
        rank_ret = _percentile_rank_by_index(indexed_ret)
        rank_liq = _percentile_rank_by_index(indexed_liq)
        rank_atr = _percentile_rank_by_index(indexed_atr)
        rank_long_term = _percentile_rank_by_index(indexed_lt) if need_long_term else {}

        eligible = [True] * len(symbol_data)
        if coarse_on:
            min_m = float(cs_cfg.get("min_momentum_percentile", 0.0) or 0.0)
            for idx in range(len(symbol_data)):
                ok = True
                if min_m > 0:
                    ok = ok and (float(rank_ret.get(idx, 0.0)) >= min_m)
                if min_liq_coarse > 0:
                    ok = ok and (float(rank_liq.get(idx, 0.0)) >= min_liq_coarse)
                eligible[idx] = ok
        metrics.ms_percentile_ranks = (time.perf_counter() - t_rank0) * 1000.0

        ir = self._filters.get("index_regime") or {}
        reg = compute_index_regime_modifiers(ir, ohlcv_dsn)
        opt_pool = dict(self._opt_override)
        opt_pool["index_regime_trend_mult"] = float(reg.get("index_regime_trend_mult", 1.0))
        opt_pool["index_regime_breakout_mult"] = float(reg.get("index_regime_breakout_mult", 1.0))
        opt_pool["index_regime_reversion_mult"] = float(reg.get("index_regime_reversion_mult", 1.0))
        index_bull = reg.get("index_ma_bullish")
        trend_mult = float(reg.get("index_regime_trend_mult", 1.0))
        market_regime_row = {
            "benchmark": str(ir.get("benchmark_symbol") or "000300.SH"),
            "ma_bullish": index_bull,
            "trend_pool_mult": reg.get("index_regime_trend_mult"),
            "stress_vol": reg.get("index_stress_vol"),
            "index_atr_ratio": reg.get("index_atr_ratio"),
            "breakout_mult": reg.get("index_regime_breakout_mult"),
            "reversion_mult": reg.get("index_regime_reversion_mult"),
        }

        t_l2pre0 = time.perf_counter()
        cd_days = int(self._opt.get("signal_cooldown_days", 0) or 0)
        cd_confirmed = bool(self._opt.get("signal_cooldown_confirmed_only", True))
        cd_set = (
            symbols_in_signal_cooldown(
                [s[0] for s in symbol_data],
                l2_dsn,
                cd_days,
                confirmed_only=cd_confirmed,
                current_ohlcv_max_ts=_fp_ohlcv,
                current_news_max_ts=_fp_news if _fp_ohlcv is not None else None,
            )
            if cd_days > 0
            else set()
        )

        cg = self._filters.get("classifier_gate") or {}
        allowed_class = None
        cg_batch = resolve_scanner_classifier_batch_id(cg.get("batch_id"))
        cg_mode = str(cg.get("match_mode") or "domain_or_primary").strip()
        if bool(cg.get("enabled", False)) and l2_dsn:
            allowed_class = allowed_symbols_by_classifier(
                [s[0] for s in symbol_data],
                l2_dsn,
                cg.get("allowed_primary_tags") or [],
                match_mode=cg_mode,
                batch_id=cg_batch,
            )
        metrics.ms_l2_precheck = (time.perf_counter() - t_l2pre0) * 1000.0

        vol_cfg = self._filters.get("volatility_regime", {})
        vol_threshold = vol_cfg.get("percentile_threshold", 0.90)
        vol_penalty = vol_cfg.get("penalty_ratio", 0.8)
        liq_cfg = self._filters.get("liquidity", {})
        liq_min = liq_cfg.get("min_score", 0)
        liq_as_filter = liq_cfg.get("as_filter", True)

        sector_strength_enabled = bool(self._filters.get("sector_strength", {}).get("enabled", False))
        ats = self._a_track
        alert_t = int(ats["alert_threshold"])
        conf_t = int(ats["confirmed_threshold"])
        dual = bool(ats["dual_tier"])

        def _score_row(idx: int) -> Optional[Dict[str, Any]]:
            sym, arr, _ret_20d, liq, atr_r, ret_lt = symbol_data[idx]
            o, h, l, c, v = arr
            momentum_pct = rank_ret.get(idx, 0.5) if need_momentum_pct else None
            score, pool_id, second_pool_id, second_pool_score, pool_scores = evaluate_pools(
                o, h, l, c, v,
                momentum_20d_percentile=momentum_pct,
                optimization_override=opt_pool,
            )
            liquidity_score = (rank_liq.get(idx, 0.5) or 0.5) * 100.0 if need_liquidity else 100.0
            if need_liquidity and liq is None:
                liquidity_score = 0.0
            if liq_as_filter and need_liquidity and liquidity_score < liq_min:
                return None
            atr_pct = rank_atr.get(idx, 0.5) if need_volatility and atr_r is not None else None
            volatility_ratio = vol_penalty if (need_volatility and atr_pct is not None and atr_pct > vol_threshold) else 1.0
            if need_liquidity and not liq_as_filter:
                score = min(100.0, score * (liquidity_score / 100.0))
            if need_volatility and volatility_ratio < 1.0:
                score = min(100.0, score * volatility_ratio)
            long_term_score = rank_long_term.get(idx) if need_long_term else None
            long_term_candidate = (
                long_term_score is not None and float(long_term_score) >= long_term_threshold
            ) if need_long_term else False
            atr_pct_for_risk = rank_atr.get(idx, 0.5) if self._need_atr_for_risk else None
            return {
                "symbol": sym,
                "o": o,
                "h": h,
                "l": l,
                "c": c,
                "technical_score": float(score),
                "strategy_source": pool_id,
                "second_pool_id": second_pool_id,
                "second_pool_score": float(second_pool_score),
                "pool_scores": dict(pool_scores) if pool_scores else {},
                "liquidity_score": liquidity_score,
                "volatility_ratio": volatility_ratio,
                "long_term_score": float(long_term_score) if long_term_score is not None else None,
                "long_term_candidate": long_term_candidate,
                "atr_pct_for_risk": atr_pct_for_risk,
            }

        work_indices: List[int] = []
        cooldown_skipped_symbols: List[str] = []
        skip_coarse = skip_cooldown = skip_classifier = 0
        for idx in range(len(symbol_data)):
            if not eligible[idx]:
                skip_coarse += 1
                continue
            sym_key = str(symbol_data[idx][0]).strip().upper()
            if sym_key in cd_set:
                skip_cooldown += 1
                cooldown_skipped_symbols.append(sym_key)
                continue
            if allowed_class is not None and sym_key not in allowed_class:
                skip_classifier += 1
                continue
            work_indices.append(idx)

        t_eval0 = time.perf_counter()
        pw = int(self._perf.get("parallel_workers", 0) or 0)
        min_par = int(self._perf.get("min_symbols_for_parallel", 48) or 48)
        pending: List[Dict[str, Any]] = []
        if pw > 0 and len(work_indices) >= min_par:
            max_w = min(pw, 32)
            with ThreadPoolExecutor(max_workers=max_w) as pool:
                results = list(pool.map(_score_row, work_indices))
            pending = [r for r in results if r is not None]
            metrics.parallel_workers_used = max_w
        else:
            for idx in work_indices:
                r = _score_row(idx)
                if r is not None:
                    pending.append(r)
        metrics.ms_evaluate_pools = (time.perf_counter() - t_eval0) * 1000.0

        t_sec0 = time.perf_counter()
        ind_map: Dict[str, str] = {}
        ss_cfg = self._filters.get("sector_strength") or {}
        unmapped_ss = float(ss_cfg.get("unmapped_sector_strength", 1.0) or 1.0)
        if sector_strength_enabled and pending:
            # L2 industry_revenue_summary：与冷却同源 DSN（PG_L2_DSN，无则回退 L1）
            ind_map = fetch_symbol_industry_map([p["symbol"] for p in pending], l2_dsn)
        ss_list, industry_mapped_flags = _sector_strength_ratios(
            [p["technical_score"] for p in pending],
            [p["symbol"] for p in pending],
            ind_map,
            sector_strength_enabled,
            unmapped_sector_strength=unmapped_ss,
        )
        metrics.ms_sector_strength = (time.perf_counter() - t_sec0) * 1000.0

        emit_regime = bool(self._product.get("emit_market_regime_per_row", True))
        t_build0 = time.perf_counter()
        out = []
        for p, sector_strength, industry_mapped in zip(pending, ss_list, industry_mapped_flags):
            o, h, l, c = p["o"], p["h"], p["l"], p["c"]
            score = p["technical_score"]
            pool_scores = p["pool_scores"]
            pool_id = p["strategy_source"]
            sector_ok = sector_strength >= self._sector_threshold
            tight_ok = _tightening_ok(pool_scores, self._pass_tightening) if pool_scores else True
            confirmed_passed = bool(sector_ok and score >= float(conf_t) and tight_ok)
            alert_passed = bool(
                dual and sector_ok and score >= float(alert_t) and not confirmed_passed
            )
            if confirmed_passed:
                signal_tier = "CONFIRMED"
            elif alert_passed:
                signal_tier = "ALERT"
            else:
                signal_tier = "NONE"
            passed = confirmed_passed
            risk_fields = compute_a_track_risk_levels(
                o,
                h,
                l,
                c,
                ats.get("risk") or {},
                strategy_source=int(pool_id),
                atr_percentile=p["atr_pct_for_risk"],
                signal_tier=signal_tier,
            )
            item = {
                "symbol": p["symbol"],
                "technical_score": score,
                "strategy_source": pool_id,
                "sector_strength": float(sector_strength),
                "correlation_id": correlation_id,
                "passed": passed,
                "alert_passed": bool(alert_passed),
                "confirmed_passed": bool(confirmed_passed),
                "signal_tier": signal_tier,
                "signal_profile": ats.get("signal_profile", "balanced"),
                "second_pool_id": p["second_pool_id"],
                "second_pool_score": p["second_pool_score"],
                "pool_scores": pool_scores,
                "liquidity_score": p["liquidity_score"],
                "volatility_ratio": p["volatility_ratio"],
                "entry_reference_price": risk_fields.get("entry_reference_price"),
                "stop_loss_price": risk_fields.get("stop_loss_price"),
                "take_profit_prices": risk_fields.get("take_profit_prices") or [],
                "risk_rules_json": risk_fields.get("risk_rules_json"),
                "stop_rule_id": risk_fields.get("stop_rule_id"),
                "tp_rule_id": risk_fields.get("tp_rule_id"),
            }
            if emit_regime:
                item["market_regime"] = dict(market_regime_row)
            if self._product.get("emit_win_rate_payoff", False):
                item["win_rate_prediction"] = float(self._product.get("win_rate_prediction", 0.7))
                item["payoff_ratio"] = float(self._product.get("payoff_ratio", 2.0))
            item["scanner_rules_fingerprint"] = self._scanner_rules_fingerprint
            item["evaluation_source"] = "FRESH"
            item["industry_mapped"] = industry_mapped
            _sk = str(p["symbol"]).strip().upper()
            item["scan_input_ohlcv_max_ts"] = _fp_ohlcv.get(_sk) if _fp_ohlcv else None
            item["scan_input_news_max_ts"] = _fp_news.get(_sk)
            if need_long_term:
                item["long_term_score"] = p["long_term_score"]
                item["long_term_candidate"] = p["long_term_candidate"]
            else:
                item["long_term_score"] = None
                item["long_term_candidate"] = False
            out.append(item)
        metrics.ms_build_output = (time.perf_counter() - t_build0) * 1000.0
        metrics.symbols_ohlcv_ok = len(symbol_data)
        metrics.symbols_scored = len(pending)
        metrics.skipped_coarse = skip_coarse
        metrics.skipped_cooldown = skip_cooldown
        metrics.skipped_classifier = skip_classifier
        if out and self._opt.get("output_score_percentile", True):
            order = sorted(range(len(out)), key=lambda i: out[i]["technical_score"])
            n_out = len(out)
            for r, i in enumerate(order):
                out[i]["technical_score_percentile"] = (r + 1) / n_out
        else:
            for item in out:
                item.setdefault("technical_score_percentile", None)
        metrics.symbols_out = len(out)
        metrics.ms_total = (time.perf_counter() - t_total0) * 1000.0
        _um = sum(1 for x in out if x.get("industry_mapped") is False)
        _mp = sum(1 for x in out if x.get("industry_mapped") is True)
        metrics.extra = {
            "n_passed_threshold": sum(1 for x in out if x.get("passed")),
            "correlation_id": correlation_id,
            "scanner_rules_fingerprint": self._scanner_rules_fingerprint,
            "sector_strength_unmapped_count": _um,
            "sector_strength_mapped_count": _mp,
        }
        self.last_scan_metrics = metrics.to_dict()
        if self._product.get("emit_scanner_metrics_log", True) and self._perf.get("metrics_log_json", True):
            logger.info("scanner_run_metrics %s", metrics.to_json())

        n_passed = sum(1 for x in out if x.get("passed"))
        logger.info("QuantScanner.scan_market: 全量=%s, 通过阈值=%s", len(out), n_passed)
        self.last_scan_pipeline = {
            "coarse_screen_enabled": coarse_on,
            "coarse_min_momentum_pct": float(cs_cfg.get("min_momentum_percentile", 0.0) or 0.0),
            "coarse_min_liquidity_pct": float(cs_cfg.get("min_liquidity_percentile", 0.0) or 0.0),
            "skipped_after_coarse": skip_coarse,
            "skipped_cooldown": skip_cooldown,
            "skipped_classifier_gate": skip_classifier,
            "index_regime_enabled": bool(ir.get("enabled", False)),
            "index_benchmark": str(ir.get("benchmark_symbol") or "000300.SH"),
            "index_ma_bullish": index_bull,
            "index_regime_trend_mult": float(trend_mult),
            "index_stress_vol": bool(reg.get("index_stress_vol")),
            "index_atr_ratio": reg.get("index_atr_ratio"),
            "scanner_metrics": self.last_scan_metrics,
            "signal_cooldown_days": cd_days,
            "cooldown_skipped_symbols": cooldown_skipped_symbols,
            "classifier_gate_enabled": bool(cg.get("enabled", False)),
            "allowed_primary_tags": list(cg.get("allowed_primary_tags") or []),
            "classifier_gate_match_mode": str(cg.get("match_mode") or "domain_or_primary"),
            "classifier_gate_batch_id": cg_batch,
            "symbols_with_ohlcv_ok": len(symbol_data),
            "symbols_talib_scored": len(pending),
            "scanner_rules_fingerprint": self._scanner_rules_fingerprint,
            "sector_strength_unmapped_count": _um,
            "sector_strength_mapped_count": _mp,
            "unmapped_sector_strength": unmapped_ss if sector_strength_enabled else None,
        }
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
