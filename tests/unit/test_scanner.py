# [Ref: 02_量化扫描引擎_实践] Module B 量化扫描引擎单测：配置加载、OHLCV 馈送、三池判定、QuantScanner

import pytest

from diting.scanner.config_loader import (
    load_scanner_config,
    get_thresholds,
    get_scoring_params,
    get_optimization_params,
    get_filters_params,
    get_pass_tightening_params,
    get_long_term_params,
    get_a_track_short_params,
    get_scanner_performance_params,
    get_product_signals_params,
)
from diting.scanner.ohlcv_feed import get_ohlcv_for_symbol, get_ohlcv_arrays_for_talib
from diting.scanner.pools import evaluate_trend, evaluate_reversion, evaluate_breakout, evaluate_pools
from diting.scanner.config_fingerprint import compute_scanner_rules_fingerprint
from diting.scanner.quant import QuantScanner


def test_scanner_rules_fingerprint_stable():
    a = compute_scanner_rules_fingerprint()
    b = compute_scanner_rules_fingerprint()
    assert len(a) == 16 and a == b
    assert all(c in "0123456789abcdef" for c in a)


def test_load_scanner_config():
    config = load_scanner_config()
    t, s = get_thresholds(config)
    assert t >= 0 and s >= 0
    assert isinstance(config, dict)
    ats = get_a_track_short_params(config)
    assert ats["signal_profile"] in ("alert_sensitive", "balanced", "strict")
    assert "alert_threshold" in ats and "confirmed_threshold" in ats


def test_get_scoring_and_optimization_params():
    config = load_scanner_config()
    scoring = get_scoring_params(config)
    opt = get_optimization_params(config)
    filters = get_filters_params(config)
    assert "trend" in scoring and "reversion" in scoring and "breakout" in scoring
    assert scoring["trend"]["macd_hist_scale_ratio"] > 0
    assert scoring["reversion"]["rsi_oversold"] in (20, 30)
    assert opt["trend_confirm_bars"] >= 1
    assert opt["multi_pool_min_score"] >= 0
    assert opt["multi_pool_bonus"] >= 0
    assert "sector_strength" in filters and "liquidity" in filters and "volatility_regime" in filters
    assert filters["sector_strength"].get("unmapped_sector_strength") == 1.0
    assert "index_regime" in filters and "classifier_gate" in filters
    assert "stress_vol_enabled" in filters["index_regime"]
    assert "coarse_screen" in opt and "signal_cooldown_days" in opt
    perf = get_scanner_performance_params(config)
    assert "parallel_workers" in perf
    prod = get_product_signals_params(config)
    assert "emit_market_regime_per_row" in prod


def test_get_pass_tightening_params():
    config = load_scanner_config()
    tight = get_pass_tightening_params(config)
    assert "pass_require_lead_pool_min" in tight and "pass_require_second_pool_min" in tight
    assert tight["pass_require_lead_pool_min"] is None or isinstance(tight["pass_require_lead_pool_min"], (int, float))
    assert tight["pass_require_second_pool_min"] is None or isinstance(tight["pass_require_second_pool_min"], (int, float))


def test_get_long_term_params():
    config = load_scanner_config()
    lt = get_long_term_params(config)
    assert "enabled" in lt and "lookback_days" in lt and "score_threshold" in lt
    assert isinstance(lt["enabled"], bool)
    assert lt["lookback_days"] >= 60
    assert 0 <= lt["score_threshold"] <= 1


def test_get_ohlcv_mock():
    raw = get_ohlcv_for_symbol("000001.SZ", limit=80, dsn=None)
    assert raw is not None
    o, h, l, c, v = raw
    assert len(o) >= 20 and len(c) == len(v)


def test_get_ohlcv_arrays_for_talib():
    arr = get_ohlcv_arrays_for_talib("600000.SH", limit=60, dsn=None)
    assert arr is not None
    assert len(arr) == 5
    assert len(arr[0]) >= 60


def test_evaluate_pools_returns_tuple():
    arr = get_ohlcv_arrays_for_talib("000001.SZ", limit=120, dsn=None)
    assert arr is not None
    o, h, l, c, v = arr
    score, pool_id, second_pool_id, second_pool_score, pool_scores = evaluate_pools(o, h, l, c, v)
    assert isinstance(score, (int, float)) and 0 <= score <= 100
    assert pool_id in (0, 1, 2, 3, 4)
    assert second_pool_id in (0, 1, 2, 3, 4)
    assert isinstance(second_pool_score, (int, float)) and 0 <= second_pool_score <= 100
    assert isinstance(pool_scores, dict) and all(k in (1, 2, 3, 4) for k in pool_scores)


def test_evaluate_pools_index_regime_trend_mult():
    arr = get_ohlcv_arrays_for_talib("000001.SZ", limit=120, dsn=None)
    assert arr is not None
    o, h, l, c, v = arr
    base, *_ = evaluate_pools(o, h, l, c, v)
    scaled, *_ = evaluate_pools(
        o, h, l, c, v, optimization_override={"index_regime_trend_mult": 0.5}
    )
    assert 0 <= float(scaled) <= 100
    assert float(scaled) <= float(base) + 1e-6


def test_evaluate_pools_index_regime_breakout_reversion_mult():
    arr = get_ohlcv_arrays_for_talib("000001.SZ", limit=120, dsn=None)
    assert arr is not None
    o, h, l, c, v = arr
    base, *_ = evaluate_pools(o, h, l, c, v)
    br, *_ = evaluate_pools(
        o, h, l, c, v,
        optimization_override={"index_regime_breakout_mult": 0.5, "index_regime_reversion_mult": 1.0},
    )
    assert 0 <= float(br) <= 100


def test_quant_scanner_scan_market_returns_list():
    scanner = QuantScanner()
    out = scanner.scan_market(["000001.SZ", "600000.SH"], ohlcv_dsn=None, return_all=True)
    assert isinstance(out, list)
    assert getattr(scanner, "last_scan_metrics", None) is not None
    assert "ms_total" in (scanner.last_scan_metrics or {})
    for s in out:
        assert "symbol" in s and "technical_score" in s and "strategy_source" in s and "sector_strength" in s and "passed" in s
        assert "second_pool_id" in s and "second_pool_score" in s and "pool_scores" in s
        assert "liquidity_score" in s and "volatility_ratio" in s
        assert "long_term_score" in s and "long_term_candidate" in s
        assert "technical_score_percentile" in s
        assert "alert_passed" in s and "confirmed_passed" in s and "signal_tier" in s
        assert "entry_reference_price" in s and "stop_loss_price" in s
        assert "market_regime" in s
        assert "industry_mapped" in s
        assert s.get("evaluation_source") == "FRESH"
        assert len(s.get("scanner_rules_fingerprint") or "") == 16


def test_quant_scanner_respects_threshold():
    scanner = QuantScanner()
    out = scanner.scan_market(["000001.SZ"], ohlcv_dsn=None, return_all=True)
    for s in out:
        assert s.get("evaluation_source") == "FRESH"
        assert len(s.get("scanner_rules_fingerprint") or "") == 16
        if s.get("passed"):
            assert s["technical_score"] >= scanner._score_threshold
            assert s["sector_strength"] >= scanner._sector_threshold


def test_l2_cooldown_carryover_row_to_signal_dict():
    from diting.scanner.l2_cooldown_carryover import _row_to_signal_dict

    row = {
        "symbol": "300750.SZ",
        "symbol_name": "宁德时代",
        "technical_score": 100.0,
        "strategy_source": "TREND",
        "sector_strength": 1.0,
        "trend_score": 100.0,
        "reversion_score": 0.0,
        "breakout_score": 0.0,
        "momentum_score": 100.0,
        "technical_score_percentile": 0.97,
        "alert_passed": False,
        "confirmed_passed": True,
        "signal_tier": "CONFIRMED",
        "entry_reference_price": 400.0,
        "stop_loss_price": 380.0,
        "take_profit_json": "[420.0, 440.0]",
        "risk_rules_json": "{}",
        "batch_id": "old",
    }
    d = _row_to_signal_dict(
        row,
        batch_id="new",
        correlation_id="new",
        use_scan_all_passed=False,
        scanner_rules_fingerprint="a" * 16,
    )
    assert d["evaluation_source"] == "CARRYOVER"
    assert d["scanner_rules_fingerprint"] == "a" * 16
    assert d["symbol"] == "300750.SZ"
    assert d["pool_scores"][1] == 100.0 and d["pool_scores"][4] == 100.0
    assert d["take_profit_prices"] == [420.0, 440.0]
    assert d["passed"] is True
    assert d.get("industry_mapped") is None
