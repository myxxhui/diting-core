# [Ref: 06_B轨_信号层生产级数据采集_实践] 信号层单测

import pytest

from diting.signal_layer.models import RefreshSegmentSignalsResult
from diting.signal_layer.understanding.engine import understand_signal, _validate_schema


def test_refresh_result_dataclass():
    r = RefreshSegmentSignalsResult(
        symbols_without_segments=["X"],
        segments_written=["seg_bp_abc"],
        summary={"total_symbols": 2},
    )
    assert r.symbols_without_segments == ["X"]
    assert r.segments_written == ["seg_bp_abc"]
    assert r.summary["total_symbols"] == 2


def test_validate_schema():
    assert _validate_schema({"direction": "bullish", "strength": 0.7, "summary_cn": "利好"}) is True
    assert _validate_schema({"direction": "x", "strength": 0.5, "summary_cn": "x"}) is False
    assert _validate_schema({"direction": "bullish", "strength": 1.5, "summary_cn": "x"}) is False
    assert _validate_schema({"direction": "bullish", "strength": 0.5}) is False  # missing summary_cn


def test_understand_signal_empty_returns_none():
    assert understand_signal("", "x", {}) is None
    assert understand_signal("a", "x", {}) is None


def test_understand_signal_no_llm_returns_none():
    text = "政策支持扩产，订单增长签约合作，需求旺盛景气。" * 2
    assert len(text) >= 5
    assert understand_signal(text, "seg_x", {}) is None
    assert understand_signal(text, "seg_x", {"mode": "ai_only"}) is None


def test_understand_signal_ai_only_path(monkeypatch):
    """已配置 key+model 时仅走 _ai_tag。"""
    def _fake_ai(raw_text, segment_id, config, audit_callback=None):
        return {
            "type": "policy",
            "direction": "bullish",
            "strength": 0.72,
            "summary_cn": "模型摘要示例",
            "risk_tags": [],
            "signal_source": "llm",
        }

    monkeypatch.setattr(
        "diting.signal_layer.understanding.engine._ai_tag",
        _fake_ai,
    )
    text = "这是一条足够长的输入文本用于信号理解测试。"
    out = understand_signal(
        text,
        "seg_test",
        {"api_key": "sk-test", "model_id": "gpt-4o-mini"},
    )
    assert out is not None
    assert out["direction"] == "bullish"
    assert out["summary_cn"] == "模型摘要示例"
    assert out.get("signal_source") == "llm"
