# [Ref: 04_A轨_MoE议会_设计] 按股配置 + 统一分析，单管道输出一条 ExpertOpinion

import logging
import time
from typing import Any, Dict, List, Optional

from diting.protocols.brain_pb2 import (
    ExpertOpinion,
    TIME_HORIZON_MEDIUM_TERM,
    TIME_HORIZON_SHORT_TERM,
)

from diting.moe.alignment import (
    RISK_LEVEL_DISCOUNT,
    build_structured_summary,
    compute_alignment_and_aggregate,
    should_reject_by_cognitive_boundary,
)
from diting.moe.signal_parse import parse_segment_signal

logger = logging.getLogger(__name__)

SIGNAL_NEUTRAL = 0
SIGNAL_BULLISH = 1
SIGNAL_BEARISH = 2
DOMAIN_AGRI = 1
DOMAIN_TECH = 2
DOMAIN_MACRO = 3

_DOMAIN_TAG_TO_ENUM = {"农业": DOMAIN_AGRI, "科技": DOMAIN_TECH, "宏观": DOMAIN_MACRO}

# 默认风险提示模板（config 未提供时使用）
_DEFAULT_RISK_FACTOR_TEMPLATES = {
    "农业": ["政策兑现风险", "存栏供给波动", "气候异常"],
    "科技": ["研发不及预期", "技术路线替代", "大基金减持"],
    "宏观": ["汇率波动", "供需反转"],
}


def _parse_signals_map(
    segment_signals_raw: Dict[str, Any],
    keywords_bullish: list = None,
    keywords_bearish: list = None,
) -> Dict[str, Dict[str, Any]]:
    """segment_id -> 解析后信号。"""
    out = {}
    for seg_id, raw in (segment_signals_raw or {}).items():
        if isinstance(raw, dict) and "direction" in raw:
            out[seg_id] = raw
        elif isinstance(raw, str):
            out[seg_id] = parse_segment_signal(raw, keywords_bullish, keywords_bearish)
        else:
            out[seg_id] = {"direction": "neutral", "strength": 0.5, "risk_tags": []}
    return out


def _risk_factors_by_level(
    risk_level: str, from_signals: List[str], domain_defaults: List[str]
) -> List[str]:
    """按风险等级填充风险提示；无风险等级时返回空，不虚构。"""
    if not risk_level or not str(risk_level).strip():
        return []
    if risk_level == "高":
        return list(from_signals)[:3] if from_signals else domain_defaults[:2]
    if risk_level == "中":
        return list(from_signals)[:2] if from_signals else (domain_defaults[:1] or ["景气波动"])
    return ["无重大风险"] if not from_signals else list(from_signals)[:2]


def _build_opinion(
    symbol: str,
    domain: int,
    is_supported: bool,
    direction: int,
    confidence: float,
    reasoning_summary: str,
    risk_factors: List[str],
    horizon: int = None,
) -> ExpertOpinion:
    if horizon is None:
        horizon = TIME_HORIZON_SHORT_TERM
    return ExpertOpinion(
        symbol=symbol,
        domain=domain,
        is_supported=is_supported,
        direction=direction,
        confidence=max(0.0, min(1.0, confidence)),
        reasoning_summary=reasoning_summary or "",
        risk_factors=risk_factors or [],
        timestamp=int(time.time()),
        horizon=horizon,
    )


def unified_opinion(
    symbol: str,
    quant_signal: Dict[str, Any],
    segment_list: List[Dict[str, Any]],
    segment_signals_raw: Dict[str, Any],
    config: Dict[str, Any],
    domain_tag: str = "农业",
    horizon: Optional[int] = None,
) -> ExpertOpinion:
    """
    统一分析入口：按设计文档管道步骤输出一条 ExpertOpinion。
    domain_tag 仅用于选用 risk_factor_templates 与 domain 枚举，不改变计算逻辑。
    """
    config = config or {}
    horizon = horizon if horizon is not None else TIME_HORIZON_SHORT_TERM
    routing = config.get("moe_router") or config
    # 与 B 侧写入 quant_signal_snapshot 一致：确认档或预警档即视为左脑量化门可用（非仅 confirmed）
    if routing.get("require_quant_passed") and quant_signal:
        qp = (
            bool(quant_signal.get("confirmed_passed"))
            or bool(quant_signal.get("passed"))
            or bool(quant_signal.get("alert_passed"))
        )
        if not qp:
            return _build_opinion(
                symbol,
                _DOMAIN_TAG_TO_ENUM.get(domain_tag, DOMAIN_AGRI),
                False,
                SIGNAL_NEUTRAL,
                0.0,
                "量化侧未进入确认档或预警档（require_quant_passed=true）；右脑不予支持",
                ["量化门控"],
                horizon=horizon,
            )
    align_cfg = routing.get("alignment", {})
    multi_cfg = routing.get("multi_segment", {})
    parse_cfg = routing.get("signal_parse", {})
    templates = (routing.get("risk_factor_templates") or config.get("risk_factor_templates") or _DEFAULT_RISK_FACTOR_TEMPLATES)
    domain_defaults = templates.get(domain_tag, _DEFAULT_RISK_FACTOR_TEMPLATES["农业"])
    domain_enum = _DOMAIN_TAG_TO_ENUM.get(domain_tag, DOMAIN_AGRI)

    # ① 解析信号
    segment_signals = _parse_signals_map(
        segment_signals_raw,
        parse_cfg.get("fallback_keywords_bullish"),
        parse_cfg.get("fallback_keywords_bearish"),
    )
    # ② 对齐与主营否决 → ③ 认知边界
    (
        alignment_score,
        primary_veto,
        weighted_conf,
        has_high_risk,
        reasoning_parts,
        bull_strength,
        boom_strength,
        risk_level,
    ) = compute_alignment_and_aggregate(
        segment_list,
        segment_signals,
        primary_weight=align_cfg.get("primary_weight", 0.6),
        other_weight=align_cfg.get("other_weight", 0.4),
        veto_threshold=align_cfg.get("veto_threshold", 0.3),
        primary_veto=multi_cfg.get("primary_veto", True),
        risk_discount=multi_cfg.get("risk_discount", 0.5),
    )
    primary_veto_reason = "; ".join(reasoning_parts).strip() if primary_veto and reasoning_parts else None
    reject, reject_reason = should_reject_by_cognitive_boundary(
        segment_list,
        segment_signals,
        alignment_score,
        primary_veto,
        veto_threshold=align_cfg.get("veto_threshold", 0.3),
        primary_veto_reason=primary_veto_reason,
    )
    if reject:
        reason = build_structured_summary(
            0.0, 0.0, risk_level, 0.0, reject_reason or "; ".join(reasoning_parts)
        )
        return _build_opinion(
            symbol,
            domain_enum,
            False,
            SIGNAL_NEUTRAL,
            0.0,
            reason,
            _risk_factors_by_level(risk_level, [], domain_defaults),
            horizon=horizon,
        )
    # ④ 多细分加权与结构化维度 ⑤ 风险等级降权 ⑥ 拼结构化摘要
    discount = RISK_LEVEL_DISCOUNT.get(risk_level, 1.0)
    final_conf = weighted_conf * discount
    blend_cfg = (routing.get("short_confidence_blend") or {}) if isinstance(routing, dict) else {}
    if blend_cfg.get("enabled") and isinstance(quant_signal, dict):
        w = float(blend_cfg.get("weight", 0.25))
        w = max(0.0, min(1.0, w))
        pct = quant_signal.get("technical_score_percentile")
        if pct is not None and isinstance(pct, (int, float)):
            p = max(0.0, min(1.0, float(pct)))
            final_conf = (1.0 - w) * final_conf + w * p
            final_conf = max(0.0, min(1.0, final_conf))
    direction = SIGNAL_BULLISH if final_conf >= 0.5 else SIGNAL_NEUTRAL
    from_sigs = []
    for sig in segment_signals.values():
        from_sigs.extend(sig.get("risk_tags") or [])
    risk_factors = _risk_factors_by_level(risk_level, from_sigs, domain_defaults)
    summary = build_structured_summary(alignment_score, boom_strength, risk_level, bull_strength)
    if reasoning_parts:
        summary += "；" + "; ".join(reasoning_parts)
    h = horizon if horizon is not None else TIME_HORIZON_SHORT_TERM
    return _build_opinion(
        symbol, domain_enum, True, direction, final_conf, summary, risk_factors[:3],
        horizon=h,
    )


def trash_bin_opinion(symbol: str, reason: str = "无法归类，逻辑不清") -> ExpertOpinion:
    """无法归类或未在 supported_tags 时，输出一条不支持意见。"""
    return _build_opinion(symbol, 0, False, SIGNAL_NEUTRAL, 0.0, reason, [], horizon=TIME_HORIZON_SHORT_TERM)
