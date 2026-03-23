# [Ref: 06_B轨_信号层生产级数据采集_实践] 信号层单测

import pytest

from diting.signal_layer.models import RefreshSegmentSignalsResult
from diting.signal_layer.understanding.engine import understand_signal, _rule_tag, _validate_schema


def test_refresh_result_dataclass():
    r = RefreshSegmentSignalsResult(
        symbols_without_segments=["X"],
        segments_written=["seg_bp_abc"],
        summary={"total_symbols": 2},
    )
    assert r.symbols_without_segments == ["X"]
    assert r.segments_written == ["seg_bp_abc"]
    assert r.summary["total_symbols"] == 2


def test_rule_tag_bullish():
    text = "政策支持新能源扩产，订单增长明显，需求旺盛。"
    out = _rule_tag(text)
    assert out["direction"] == "bullish"
    assert 0.5 <= out["strength"] <= 1.0
    assert out["type"] == "policy"
    assert "summary_cn" in out


def test_rule_tag_bearish():
    text = "限产令下达，公司面临处罚，业绩下滑。"
    out = _rule_tag(text)
    assert out["direction"] == "bearish"
    assert 0.5 <= out["strength"] <= 1.0


def test_rule_tag_neutral():
    text = "今日天气晴朗，无重大消息。"
    out = _rule_tag(text)
    assert out["direction"] == "neutral"


def test_validate_schema():
    assert _validate_schema({"direction": "bullish", "strength": 0.7, "summary_cn": "利好"}) is True
    assert _validate_schema({"direction": "x", "strength": 0.5, "summary_cn": "x"}) is False
    assert _validate_schema({"direction": "bullish", "strength": 1.5, "summary_cn": "x"}) is False
    assert _validate_schema({"direction": "bullish", "strength": 0.5}) is False  # missing summary_cn


def test_understand_signal_rule_path():
    out = understand_signal("政策支持扩产，订单增长。", "seg_bp_x", {"mode": "rule_only"})
    assert out is not None
    assert out["direction"] in ("bullish", "bearish", "neutral")
    assert 0.0 <= out["strength"] <= 1.0
    assert out["summary_cn"]


def test_understand_signal_empty_returns_none():
    assert understand_signal("", "x", {}) is None
    assert understand_signal("a", "x", {}) is None
