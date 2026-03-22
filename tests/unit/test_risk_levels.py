# [Ref: 02_B模块策略_策略实现规约] A 轨风控：合并、波动分档、ALERT 止盈

import json

import pytest

from diting.scanner.risk_levels import compute_a_track_risk_levels, _volatility_tier_scales


def _ohlc_flat(close_val: float, n: int = 80):
    c = [close_val] * n
    o = h = l = c
    v = [1e6] * n
    return o, h, l, c, v


def test_volatility_tier_scales():
    name, ams, fps = _volatility_tier_scales(0.1, {"low_atr_percentile_max": 0.33, "high_atr_percentile_min": 0.66, "low_volatility": {"atr_stop_multiple_scale": 0.9, "fixed_stop_pct_scale": 0.9}})
    assert name == "low"
    assert ams == 0.9 and fps == 0.9
    name2, _, _ = _volatility_tier_scales(0.9, {"low_atr_percentile_max": 0.33, "high_atr_percentile_min": 0.66, "high_volatility": {"atr_stop_multiple_scale": 1.1, "fixed_stop_pct_scale": 1.05}})
    assert name2 == "high"


def test_compute_risk_strategy_override_changes_stop_mode():
    o, h, l, c, _ = _ohlc_flat(100.0)
    base = {
        "enabled": True,
        "stop_mode": "fixed_pct",
        "fixed_stop_pct": 0.02,
        "strategy_risk_overrides": {"2": {"fixed_stop_pct": 0.04}},
    }
    r = compute_a_track_risk_levels(o, h, l, c, base, strategy_source=2)
    data = json.loads(r["risk_rules_json"])
    assert data["fixed_stop_pct"] == pytest.approx(0.04)
    assert r["stop_loss_price"] == pytest.approx(96.0)


def test_compute_risk_volatility_tier_scales_atr():
    o, h, l, c, _ = _ohlc_flat(100.0)
    base = {
        "enabled": True,
        "stop_mode": "fixed_pct",
        "fixed_stop_pct": 0.02,
        "volatility_tier": {
            "enabled": True,
            "low_atr_percentile_max": 0.33,
            "high_atr_percentile_min": 0.66,
            "high_volatility": {"atr_stop_multiple_scale": 1.0, "fixed_stop_pct_scale": 1.2},
            "low_volatility": {"atr_stop_multiple_scale": 1.0, "fixed_stop_pct_scale": 0.9},
            "mid_volatility": {"atr_stop_multiple_scale": 1.0, "fixed_stop_pct_scale": 1.0},
        },
    }
    r = compute_a_track_risk_levels(o, h, l, c, base, strategy_source=1, atr_percentile=0.95)
    data = json.loads(r["risk_rules_json"])
    assert data["volatility_tier"] == "high"
    assert data["fixed_stop_pct"] == pytest.approx(0.024)
    assert r["stop_loss_price"] == pytest.approx(100 * (1 - 0.024))


def test_alert_tier_tp_multiples():
    o, h, l, c, _ = _ohlc_flat(100.0)
    base = {
        "enabled": True,
        "fixed_stop_pct": 0.02,
        "take_profit_r_multiples": [1.0, 2.0],
        "alert_tier": {"take_profit_r_multiples": [0.5, 1.0]},
    }
    r_alert = compute_a_track_risk_levels(o, h, l, c, base, signal_tier="ALERT")
    r_conf = compute_a_track_risk_levels(o, h, l, c, base, signal_tier="CONFIRMED")
    assert len(r_alert["take_profit_prices"]) == 2
    assert r_alert["take_profit_prices"][0] < r_conf["take_profit_prices"][0]
