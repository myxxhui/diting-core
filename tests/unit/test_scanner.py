# [Ref: 02_量化扫描引擎_实践] Module B 量化扫描引擎单测：配置加载、OHLCV 馈送、三池判定、QuantScanner

import pytest

from diting.scanner.config_loader import load_scanner_config, get_thresholds
from diting.scanner.ohlcv_feed import get_ohlcv_for_symbol, get_ohlcv_arrays_for_talib
from diting.scanner.pools import evaluate_trend, evaluate_reversion, evaluate_breakout, evaluate_pools
from diting.scanner.quant import QuantScanner


def test_load_scanner_config():
    config = load_scanner_config()
    t, s = get_thresholds(config)
    assert t >= 0 and s >= 0
    assert isinstance(config, dict)


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
    arr = get_ohlcv_arrays_for_talib("000001.SZ", limit=80, dsn=None)
    assert arr is not None
    o, h, l, c, v = arr
    score, pool_id = evaluate_pools(o, h, l, c, v)
    assert isinstance(score, int) and 0 <= score <= 100
    assert pool_id in (0, 1, 2, 3)


def test_quant_scanner_scan_market_returns_list():
    scanner = QuantScanner()
    out = scanner.scan_market(["000001.SZ", "600000.SH"], ohlcv_dsn=None, return_all=True)
    assert isinstance(out, list)
    for s in out:
        assert "symbol" in s and "technical_score" in s and "strategy_source" in s and "sector_strength" in s and "passed" in s


def test_quant_scanner_respects_threshold():
    scanner = QuantScanner()
    out = scanner.scan_market(["000001.SZ"], ohlcv_dsn=None, return_all=True)
    for s in out:
        if s.get("passed"):
            assert s["technical_score"] >= scanner._score_threshold
            assert s["sector_strength"] >= scanner._sector_threshold
