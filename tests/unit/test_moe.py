# [Ref: 04_A轨_MoE议会_设计, 04_A轨_MoE议会_实践] Module C 单测：信号解析、对齐、聚合、Router、专家

import pytest

from diting.moe.signal_parse import parse_segment_signal
from diting.moe.alignment import (
    build_structured_summary,
    compute_alignment_and_aggregate,
    should_reject_by_cognitive_boundary,
)
from diting.moe.experts import unified_opinion, trash_bin_opinion
from diting.moe.router import resolve_router_domain_tag, route_and_collect_opinions
from diting.protocols.brain_pb2 import ExpertOpinion, TIME_HORIZON_SHORT_TERM


# ----- 契约 100% -----


def test_expert_opinion_has_required_fields():
    op = ExpertOpinion(
        symbol="000001.SZ",
        domain=1,
        is_supported=True,
        direction=1,
        confidence=0.8,
        reasoning_summary="test",
        risk_factors=[],
    )
    assert hasattr(op, "symbol")
    assert hasattr(op, "is_supported")
    assert hasattr(op, "confidence")
    assert hasattr(op, "reasoning_summary")
    assert hasattr(op, "risk_factors")


# ----- signal_parse -----


def test_parse_segment_signal_json_bullish():
    s = '{"type":"policy","direction":"bullish","strength":0.9,"summary_cn":"利好","risk_tags":[]}'
    out = parse_segment_signal(s)
    assert out["direction"] == "bullish"
    assert out["strength"] == 0.9


def test_parse_segment_signal_json_bearish():
    s = '{"direction":"bearish","strength":0.2}'
    out = parse_segment_signal(s)
    assert out["direction"] == "bearish"
    assert out["strength"] == 0.2


def test_parse_segment_signal_fallback_bullish():
    out = parse_segment_signal("近期政策利好行业")
    assert out["direction"] == "bullish"
    assert out["strength"] == 0.5


def test_parse_segment_signal_fallback_bearish():
    out = parse_segment_signal("需求下跌风险")
    assert out["direction"] == "bearish"


def test_parse_segment_signal_empty():
    out = parse_segment_signal("")
    assert out["direction"] == "neutral"
    assert out["strength"] == 0.5


# ----- alignment -----


def test_alignment_empty_segment_list():
    out = compute_alignment_and_aggregate([], {})
    score, veto, conf, risk, parts, bull, boom, rlevel = out
    assert score == 0.0
    assert veto is True
    assert "无主营构成" in " ".join(parts)


def test_alignment_primary_no_signal_veto():
    seg_list = [{"segment_id": "agri_pork", "revenue_share": 0.8, "is_primary": True}]
    out = compute_alignment_and_aggregate(seg_list, {}, primary_veto=True)
    score, veto, conf, risk, parts, bull, boom, rlevel = out
    assert veto is True
    assert "主营" in " ".join(parts)


def test_alignment_primary_bullish_pass():
    seg_list = [{"segment_id": "agri_pork", "revenue_share": 0.8, "is_primary": True}]
    signals = {"agri_pork": {"direction": "bullish", "strength": 0.9, "risk_tags": []}}
    out = compute_alignment_and_aggregate(seg_list, signals)
    score, veto, conf, risk, parts, bull_strength, boom_strength, risk_level = out
    assert veto is False
    assert score >= 0.5
    assert conf >= 0.5
    assert bull_strength >= 0.5
    assert boom_strength >= 0.5
    assert risk_level in ("高", "中", "低")


def test_alignment_score_below_threshold():
    seg_list = [{"segment_id": "x", "revenue_share": 0.3, "is_primary": True}]
    signals = {"x": {"direction": "neutral", "strength": 0.3, "risk_tags": []}}
    out = compute_alignment_and_aggregate(seg_list, signals, veto_threshold=0.3)
    score, veto, conf, risk, parts, bull, boom, rlevel = out
    assert score < 0.3 or veto
    reject, reason = should_reject_by_cognitive_boundary(seg_list, signals, 0.2, False, 0.3)
    assert reject is True
    assert "利好与主营未对齐" in reason or "未对齐" in reason


def test_build_structured_summary():
    s = build_structured_summary(0.85, 0.7, "低", 0.8)
    assert "对齐得分=0.85" in s
    assert "景气强度=0.70" in s
    assert "风险等级=低" in s
    assert "利好强度=0.80" in s


def test_cognitive_boundary_no_segment_list():
    reject, reason = should_reject_by_cognitive_boundary([], {}, 0.0, False, 0.3)
    assert reject is True
    assert "无主营构成" in reason


# ----- experts（统一分析入口）-----


def test_unified_opinion_no_segment_list_is_supported_false():
    op = unified_opinion("000998.SZ", {}, [], {}, {}, domain_tag="农业")
    assert op.is_supported is False
    assert op.domain == 1
    assert "无主营构成" in op.reasoning_summary or "细分" in op.reasoning_summary


def test_unified_opinion_primary_bullish_is_supported_true():
    seg_list = [{"segment_id": "agri_pork", "revenue_share": 0.9, "is_primary": True}]
    signals = {"agri_pork": {"direction": "bullish", "strength": 0.85, "risk_tags": []}}
    op = unified_opinion("000998.SZ", {}, seg_list, signals, {}, domain_tag="农业")
    assert op.is_supported is True
    assert op.confidence >= 0.5
    assert op.domain == 1
    assert op.horizon == TIME_HORIZON_SHORT_TERM
    assert "对齐得分=" in op.reasoning_summary
    assert "景气强度=" in op.reasoning_summary
    assert "风险等级=" in op.reasoning_summary
    assert "利好强度=" in op.reasoning_summary


def test_unified_opinion_require_quant_passed_blocks():
    seg_list = [{"segment_id": "agri_pork", "revenue_share": 0.9, "is_primary": True}]
    signals = {"agri_pork": {"direction": "bullish", "strength": 0.85, "risk_tags": []}}
    cfg = {
        "moe_router": {
            "require_quant_passed": True,
            "alignment": {},
            "multi_segment": {},
            "signal_parse": {},
        }
    }
    op = unified_opinion(
        "000998.SZ",
        {"confirmed_passed": False, "passed": False, "alert_passed": False},
        seg_list,
        signals,
        cfg,
        domain_tag="农业",
    )
    assert op.is_supported is False
    assert "量化" in op.reasoning_summary or "门控" in op.reasoning_summary


def test_unified_opinion_require_quant_passed_allows_alert_tier():
    """与 B snapshot 一致：仅预警档也应通过量化门。"""
    seg_list = [{"segment_id": "agri_pork", "revenue_share": 0.9, "is_primary": True}]
    signals = {"agri_pork": {"direction": "bullish", "strength": 0.85, "risk_tags": []}}
    cfg = {
        "moe_router": {
            "require_quant_passed": True,
            "alignment": {},
            "multi_segment": {},
            "signal_parse": {},
        }
    }
    op = unified_opinion(
        "000998.SZ",
        {"confirmed_passed": False, "passed": False, "alert_passed": True},
        seg_list,
        signals,
        cfg,
        domain_tag="农业",
    )
    assert op.is_supported is True


def test_trash_bin_opinion():
    op = trash_bin_opinion("999999.SZ", "未知标签")
    assert op.is_supported is False
    assert "未知" in op.reasoning_summary or "无法归类" in op.reasoning_summary


# ----- Router 分发 -----


def test_router_distributes_agriculture():
    opinions = route_and_collect_opinions(
        "000998.SZ",
        quant_signal={},
        domain_tags=["农业"],
        segment_list=[{"segment_id": "agri_pork", "revenue_share": 0.9, "is_primary": True}],
        segment_signals={"agri_pork": {"direction": "bullish", "strength": 0.8}},
        enable_vc_agent=False,
    )
    assert len(opinions) == 1
    assert opinions[0].domain == 1
    assert opinions[0].is_supported is True


def test_router_distributes_tech_and_geo():
    opinions = route_and_collect_opinions(
        "688981.SH",
        quant_signal={},
        domain_tags=["科技"],
        segment_list=[{"segment_id": "tech_semi", "revenue_share": 0.9, "is_primary": True}],
        segment_signals={"tech_semi": {"direction": "bullish", "strength": 0.7}},
        enable_vc_agent=False,
    )
    assert len(opinions) >= 1
    assert opinions[0].domain == 2

    opinions2 = route_and_collect_opinions(
        "601899.SH",
        quant_signal={},
        domain_tags=["宏观"],
        segment_list=[{"segment_id": "geo_copper", "revenue_share": 0.6, "is_primary": True}],
        segment_signals={"geo_copper": {"direction": "bullish", "strength": 0.75}},
        enable_vc_agent=False,
    )
    assert len(opinions2) >= 1
    assert opinions2[0].domain == 3


def test_router_unknown_tag_trash_bin():
    opinions = route_and_collect_opinions(
        "000001.SZ",
        quant_signal={},
        domain_tags=["未知"],
        segment_list=[],
        segment_signals={},
        enable_vc_agent=False,
    )
    assert len(opinions) == 1
    assert opinions[0].is_supported is False
    assert "无法归类" in opinions[0].reasoning_summary or "未映射" in opinions[0].reasoning_summary


def test_resolve_router_domain_tag_matches_router():
    assert resolve_router_domain_tag(["半导体"], None) == "科技"
    assert resolve_router_domain_tag(["宏观"], None) == "宏观"
    cfg = {"moe_router": {"supported_tags": ["农业", "科技", "宏观"], "tag_to_router_domain": {}, "unmapped_router_domain": "科技"}}
    assert resolve_router_domain_tag(["不存在行业"], cfg) == "科技"


def test_router_tag_to_router_domain_maps_custom_label():
    """config/moe_router.yaml 将「半导体」等映射到 科技/宏观 等。"""
    opinions = route_and_collect_opinions(
        "688012.SH",
        quant_signal={},
        domain_tags=["半导体"],
        segment_list=[{"segment_id": "semi", "revenue_share": 0.9, "is_primary": True}],
        segment_signals={"semi": {"direction": "bullish", "strength": 0.7}},
        enable_vc_agent=False,
    )
    assert len(opinions) == 1
    assert opinions[0].is_supported is True
    assert opinions[0].domain == 2


def test_router_unmapped_router_domain_in_config():
    cfg = {
        "moe_router": {
            "supported_tags": ["农业", "科技", "宏观"],
            "tag_to_router_domain": {},
            "unmapped_router_domain": "科技",
            "risk_factor_templates": {"科技": ["研发不及预期"]},
            "alignment": {"primary_weight": 0.6, "other_weight": 0.4, "veto_threshold": 0.3},
            "multi_segment": {"primary_veto": True, "risk_discount": 0.5},
        }
    }
    opinions = route_and_collect_opinions(
        "000001.SZ",
        quant_signal={},
        domain_tags=["未在表中出现的行业名"],
        segment_list=[{"segment_id": "x", "revenue_share": 0.9, "is_primary": True}],
        segment_signals={"x": {"direction": "bullish", "strength": 0.7}},
        enable_vc_agent=False,
        config=cfg,
    )
    assert len(opinions) == 1
    assert opinions[0].is_supported is True


def test_router_no_domain_tags_returns_one_unsupported():
    opinions = route_and_collect_opinions(
        "000001.SZ",
        quant_signal={},
        domain_tags=[],
        enable_vc_agent=False,
    )
    assert len(opinions) == 1
    assert opinions[0].is_supported is False


def test_router_primary_veto_yields_unsupported():
    opinions = route_and_collect_opinions(
        "000998.SZ",
        quant_signal={},
        domain_tags=["农业"],
        segment_list=[{"segment_id": "agri_pork", "revenue_share": 0.9, "is_primary": True}],
        segment_signals={},  # 主营无信号 -> 一票否决
        enable_vc_agent=False,
    )
    assert len(opinions) == 1
    assert opinions[0].is_supported is False
    assert "主营" in opinions[0].reasoning_summary or "无" in opinions[0].reasoning_summary


def test_router_primary_bearish_veto():
    opinions = route_and_collect_opinions(
        "000998.SZ",
        quant_signal={},
        domain_tags=["农业"],
        segment_list=[{"segment_id": "agri_pork", "revenue_share": 0.9, "is_primary": True}],
        segment_signals={"agri_pork": {"direction": "bearish", "strength": 0.8}},
        enable_vc_agent=False,
    )
    assert len(opinions) == 1
    assert opinions[0].is_supported is False


def test_risk_level_high_discounts_confidence():
    """风险等级高时确信度乘 0.5。"""
    seg_list = [{"segment_id": "agri_pork", "revenue_share": 0.9, "is_primary": True}]
    signals = {"agri_pork": {"direction": "bullish", "strength": 0.9, "risk_tags": ["高风险"]}}
    op = unified_opinion("000998.SZ", {}, seg_list, signals, {}, domain_tag="农业")
    assert op.is_supported is True
    assert "风险等级=高" in op.reasoning_summary
    assert op.confidence <= 0.5
    assert op.confidence >= 0.4
