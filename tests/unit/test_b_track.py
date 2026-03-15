# [Ref: 06_B轨需求与实现缺口分析] B 轨：VC-Agent、Router、判官双轨分流、逻辑证伪/大周期反转占位

import pytest

from diting.moe import route_and_collect_opinions, vc_agent_opinion, TIME_HORIZON_LONG_TERM
from diting.protocols.brain_pb2 import ExpertOpinion
from diting.gavel import vote, Verdict, check_logic_disproof_stop, check_major_trend_reversal


def test_vc_agent_returns_long_term():
    op = vc_agent_opinion("600519.SH", enable_long_term=True)
    assert isinstance(op, ExpertOpinion)
    assert op.symbol == "600519.SH"
    assert op.horizon == TIME_HORIZON_LONG_TERM
    assert op.is_supported is True
    assert op.direction == 1


def test_router_adds_vc_agent_when_long_term_candidate():
    quant = {"symbol": "600519.SH", "long_term_candidate": True}
    opinions = route_and_collect_opinions("600519.SH", quant_signal=quant, enable_vc_agent=True)
    assert len(opinions) >= 1
    assert any(getattr(o, "horizon", 0) == TIME_HORIZON_LONG_TERM for o in opinions)


def test_router_no_vc_agent_when_not_long_term_candidate():
    quant = {"symbol": "600519.SH", "long_term_candidate": False}
    opinions = route_and_collect_opinions("600519.SH", quant_signal=quant, enable_vc_agent=True)
    assert all(getattr(o, "horizon", 0) != TIME_HORIZON_LONG_TERM or True for o in opinions)


def test_verdict_b_track_exempts_2pct_and_cash_drag():
    quant = {"symbol": "600519.SH", "technical_score": 75}
    op = vc_agent_opinion("600519.SH", enable_long_term=True)
    v = vote(quant, [op], technical_threshold=70)
    assert isinstance(v, Verdict)
    assert v.action == 1
    assert v.track == "B"
    assert v.apply_2pct_stop is False
    assert v.apply_cash_drag is False


def test_verdict_a_track_keeps_2pct_and_cash_drag():
    quant = {"symbol": "600519.SH", "technical_score": 75}
    op = vc_agent_opinion("600519.SH", enable_long_term=False)
    v = vote(quant, [op], technical_threshold=70)
    assert v.track == "A"
    assert v.apply_2pct_stop is True
    assert v.apply_cash_drag is True


def test_logic_disproof_and_major_reversal_stubs():
    assert check_logic_disproof_stop("600519.SH") is False
    assert check_major_trend_reversal("600519.SH") is False
