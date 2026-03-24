# A 轨信号合并到 Module C segment_signals

from diting.moe.a_track_signal_reader import merge_a_track_into_segment_signals


def test_merge_fills_primary_when_empty():
    sl = [{"segment_id": "seg_bp_abc", "is_primary": True, "revenue_share": 0.8}]
    ss = {}
    sym_sig = {"direction": "bullish", "strength": 0.7, "summary_cn": "x", "risk_tags": []}
    sl2, ss2 = merge_a_track_into_segment_signals(
        "a", sl, ss, sym_sig, None
    )
    assert ss2.get("seg_bp_abc") is sym_sig


def test_merge_industry_appends_segment():
    sl = [{"segment_id": "seg_bp_abc", "is_primary": True, "revenue_share": 0.8}]
    ss = {}
    ind_sig = {"direction": "neutral", "strength": 0.5, "summary_cn": "行业", "risk_tags": []}
    sl2, ss2 = merge_a_track_into_segment_signals(
        "a", sl, ss, None, ind_sig
    )
    assert "a_track_industry" in ss2
    assert any(s.get("segment_id") == "a_track_industry" for s in sl2)


def test_merge_noop_for_b_track():
    sl, ss = merge_a_track_into_segment_signals("b", [], {}, {"direction": "bullish"}, None)
    assert ss == {}
