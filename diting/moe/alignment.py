# [Ref: 04_A轨_MoE议会_设计] 利好与主营对齐 + 多细分聚合 + 结构化维度（利好强度、景气强度、风险分级）

import logging
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)

# 与 config/DNA 一致，可被覆盖
DEFAULT_PRIMARY_WEIGHT = 0.6
DEFAULT_OTHER_WEIGHT = 0.4
DEFAULT_VETO_THRESHOLD = 0.3
HIGH_RISK_TAG = "高风险"
# 风险等级降权：高 0.5，中 0.9，低 1.0（在 experts 中应用，此处只算等级）
RISK_LEVEL_HIGH = "高"
RISK_LEVEL_MID = "中"
RISK_LEVEL_LOW = "低"


def compute_alignment_and_aggregate(
    segment_list: List[Dict[str, Any]],
    segment_signals: Dict[str, Dict[str, Any]],
    primary_weight: float = DEFAULT_PRIMARY_WEIGHT,
    other_weight: float = DEFAULT_OTHER_WEIGHT,
    veto_threshold: float = DEFAULT_VETO_THRESHOLD,
    primary_veto: bool = True,
    risk_discount: float = 0.5,
) -> Tuple[float, bool, float, bool, List[str], float, float, str]:
    """
    计算对齐得分、是否主营一票否决、加权置信度（未乘风险等级系数）、是否含高风险标签、
    理由片段、利好强度、景气强度、风险等级（高/中/低）。
    :return: (alignment_score, primary_veto_triggered, weighted_confidence, has_high_risk,
              reasoning_parts, bull_strength, boom_strength, risk_level)
    """
    reasoning_parts: List[str] = []
    bull_strength = 0.5
    boom_strength = 0.0
    risk_level = RISK_LEVEL_MID

    if not segment_list:
        reasoning_parts.append("无主营构成或细分列表为空")
        return 0.0, True, 0.0, False, reasoning_parts, 0.0, 0.0, RISK_LEVEL_MID

    primary = next((s for s in segment_list if s.get("is_primary")), None)
    others = [s for s in segment_list if not s.get("is_primary")]

    # 主营对齐
    alignment_primary = 0.0
    if primary:
        seg_id = primary.get("segment_id")
        sig = segment_signals.get(seg_id) if seg_id else None
        if sig and (sig.get("direction") == "bullish"):
            alignment_primary = sig.get("strength") if isinstance(sig.get("strength"), (int, float)) else 1.0
        elif primary_veto and (not sig or sig.get("direction") != "bullish"):
            reasoning_parts.append("主营细分无利好或利空")
            return 0.0, True, 0.0, False, reasoning_parts, 0.0, 0.0, RISK_LEVEL_MID

    alignment_other = 0.0
    for s in others:
        seg_id = s.get("segment_id")
        rev = float(s.get("revenue_share") or 0)
        sig = segment_signals.get(seg_id) if seg_id else None
        if sig and sig.get("direction") == "bullish":
            alignment_other += rev
    alignment_other = min(alignment_other, 1.0)

    alignment_score = primary_weight * alignment_primary + other_weight * alignment_other

    if alignment_score < veto_threshold:
        reasoning_parts.append("利好与主营未对齐")

    # 加权置信度与利好强度：仅对有 bullish 信号的细分按营收加权 strength
    total_rev = 0.0
    weighted_sum = 0.0
    boom_rev_sum = 0.0  # 有利好信号的细分营收占比之和 → 景气强度
    for s in segment_list:
        seg_id = s.get("segment_id")
        rev = float(s.get("revenue_share") or 0)
        sig = segment_signals.get(seg_id) if seg_id else None
        if sig and sig.get("direction") == "bullish":
            strength = sig.get("strength")
            if isinstance(strength, (int, float)):
                strength = max(0, min(1, float(strength)))
            else:
                strength = 0.5
            weighted_sum += strength * rev
            total_rev += rev
            boom_rev_sum += rev
    if total_rev > 0:
        weighted_confidence = weighted_sum / total_rev
        bull_strength = weighted_confidence
    else:
        weighted_confidence = 0.5
        bull_strength = 0.5
    boom_strength = min(1.0, boom_rev_sum)

    has_high_risk = False
    for sig in segment_signals.values():
        tags = sig.get("risk_tags") or []
        if any(HIGH_RISK_TAG in str(t) for t in tags):
            has_high_risk = True
            break

    # 风险等级：高=任一细分高风险；中=主营利空或默认；低=多数利好且无高风险
    if has_high_risk:
        risk_level = RISK_LEVEL_HIGH
    elif primary and (segment_signals.get(primary.get("segment_id")) or {}).get("direction") == "bearish":
        risk_level = RISK_LEVEL_MID
    elif boom_strength >= 0.5 and not has_high_risk:
        risk_level = RISK_LEVEL_LOW
    else:
        risk_level = RISK_LEVEL_MID

    return alignment_score, False, weighted_confidence, has_high_risk, reasoning_parts, bull_strength, boom_strength, risk_level


def build_structured_summary(
    alignment_score: float,
    boom_strength: float,
    risk_level: str,
    bull_strength: float,
    extra: str = "",
) -> str:
    """
    理由摘要须含可解析维度：对齐得分=xx 景气强度=xx 风险等级=高|中|低 利好强度=xx。
    """
    parts = [
        f"对齐得分={alignment_score:.2f}",
        f"景气强度={boom_strength:.2f}",
        f"风险等级={risk_level}",
        f"利好强度={bull_strength:.2f}",
    ]
    if extra:
        parts.append(extra)
    return " ".join(parts)


# 风险等级对应确信度系数（判官前由 C 应用）
RISK_LEVEL_DISCOUNT = {RISK_LEVEL_HIGH: 0.5, RISK_LEVEL_MID: 0.9, RISK_LEVEL_LOW: 1.0}


def should_reject_by_cognitive_boundary(
    segment_list: List[Dict[str, Any]],
    segment_signals: Dict[str, Dict[str, Any]],
    alignment_score: float,
    primary_veto_triggered: bool,
    veto_threshold: float = DEFAULT_VETO_THRESHOLD,
) -> Tuple[bool, str]:
    """
    认知边界：是否应强制 is_supported=False 及对应原因文案。
    """
    if not segment_list:
        return True, "无主营构成或细分列表为空"
    if primary_veto_triggered:
        return True, "主营细分无利好或利空"
    if alignment_score < veto_threshold:
        return True, "利好与主营未对齐"
    has_any_signal = any(seg.get("segment_id") in segment_signals for seg in segment_list)
    if not has_any_signal:
        return True, "全部细分无垂直信号"
    return False, ""
