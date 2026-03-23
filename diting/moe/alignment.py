# [Ref: 04_A轨_MoE议会_设计] 利好与主营对齐 + 多细分聚合 + 结构化维度（利好强度、景气强度、风险分级）

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# 与 config/DNA 一致，可被覆盖
DEFAULT_PRIMARY_WEIGHT = 0.6
DEFAULT_OTHER_WEIGHT = 0.4
DEFAULT_VETO_THRESHOLD = 0.3
HIGH_RISK_TAG = "高风险"
# 风险等级降权：高 0.5，中 0.9，低 1.0（在 experts 中应用）；无则 None，禁止虚构兜底
RISK_LEVEL_HIGH = "高"
RISK_LEVEL_MID = "中"
RISK_LEVEL_LOW = "低"
RISK_LEVEL_NONE = ""  # 无法判断时不虚构，暴露实际问题


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
    risk_level = RISK_LEVEL_NONE  # 无则空，不虚构兜底

    if not segment_list:
        reasoning_parts.append(
            "上游无数据：标的主营构成为空；缺失依赖：symbol_business_profile 或 Module A segment_shares"
        )
        return 0.0, True, 0.0, False, reasoning_parts, 0.0, 0.0, RISK_LEVEL_NONE

    primary = next((s for s in segment_list if s.get("is_primary")), None)
    others = [s for s in segment_list if not s.get("is_primary")]

    # 主营对齐：独立判断无细分/利空/无信号
    alignment_primary = 0.0
    if primary:
        seg_id = primary.get("segment_id")
        sig = segment_signals.get(seg_id) if seg_id else None
        if sig and (sig.get("direction") == "bullish"):
            alignment_primary = sig.get("strength") if isinstance(sig.get("strength"), (int, float)) else 1.0
        elif primary_veto and (not sig or sig.get("direction") != "bullish"):
            if not sig or seg_id not in segment_signals:
                reasoning_parts.append(
                    "主营细分无信号（认知边界）；缺失依赖：segment_signal_cache 需先写入该标的 segment_id 的条目"
                )
            elif sig.get("direction") == "bearish":
                reasoning_parts.append("主营细分利空（认知边界）")
            else:
                reasoning_parts.append(
                    "主营细分无信号（认知边界）；缺失依赖：segment_signal_cache 需先写入"
                )
            return 0.0, True, 0.0, False, reasoning_parts, 0.0, 0.0, RISK_LEVEL_NONE

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
        reasoning_parts.append("利好与主营未对齐（alignment_score < 0.3）")

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

    # 风险等级：高=任一细分高风险；低=多数利好且无高风险；其他可判断时为中；无法判断时 None
    if has_high_risk:
        risk_level = RISK_LEVEL_HIGH
    elif primary and (segment_signals.get(primary.get("segment_id")) or {}).get("direction") == "bearish":
        risk_level = RISK_LEVEL_MID
    elif boom_strength >= 0.5 and not has_high_risk:
        risk_level = RISK_LEVEL_LOW
    elif total_rev > 0:
        # 有信号数据可判断，仅未达「低」条件
        risk_level = RISK_LEVEL_MID
    else:
        # 无利好信号、无法判断，不虚构，并注明缺失依赖
        risk_level = RISK_LEVEL_NONE
        if not segment_signals:
            reasoning_parts.append("风险等级无法判断：缺失 segment_signal_cache")
        elif total_rev <= 0:
            reasoning_parts.append("风险等级无法判断：主营无利好信号")

    return alignment_score, False, weighted_confidence, has_high_risk, reasoning_parts, bull_strength, boom_strength, risk_level


def build_structured_summary(
    alignment_score: float,
    boom_strength: float,
    risk_level: str,
    bull_strength: float,
    extra: str = "",
) -> str:
    """
    理由摘要须含可解析维度：对齐得分 景气强度 利好强度；风险等级仅在可判断时输出，不虚构。
    """
    parts = [
        f"对齐得分={alignment_score:.2f}",
        f"景气强度={boom_strength:.2f}",
        f"利好强度={bull_strength:.2f}",
    ]
    if risk_level and risk_level.strip():
        parts.insert(2, f"风险等级={risk_level}")
    if extra:
        parts.append(extra)
    return " ".join(parts)


# 风险等级对应确信度系数（判官前由 C 应用）；无时不降权，用 1.0
RISK_LEVEL_DISCOUNT = {
    RISK_LEVEL_HIGH: 0.5,
    RISK_LEVEL_MID: 0.9,
    RISK_LEVEL_LOW: 1.0,
    RISK_LEVEL_NONE: 1.0,
}


def should_reject_by_cognitive_boundary(
    segment_list: List[Dict[str, Any]],
    segment_signals: Dict[str, Dict[str, Any]],
    alignment_score: float,
    primary_veto_triggered: bool,
    veto_threshold: float = DEFAULT_VETO_THRESHOLD,
    primary_veto_reason: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    认知边界：是否应强制 is_supported=False 及对应原因文案。
    独立判断：无细分 / 主营利空 / 主营无信号 / 全部无垂直信号。
    primary_veto_reason：由 compute 传入，避免与 should_reject 重复计算。
    """
    if not segment_list:
        return True, "上游无数据：标的主营构成为空"
    if primary_veto_triggered and primary_veto_reason:
        return True, primary_veto_reason
    if primary_veto_triggered:
        # 兜底：无法从调用方传入时，按 segment_signals 独立判断
        primary = next((s for s in segment_list if s.get("is_primary")), None)
        if primary:
            sig = segment_signals.get(primary.get("segment_id")) if primary.get("segment_id") else None
            if sig and sig.get("direction") == "bearish":
                return True, "主营细分利空（认知边界）"
        return True, "主营细分无信号（认知边界）"
    if alignment_score < veto_threshold:
        return True, "利好与主营未对齐（alignment_score < 0.3）"
    has_any_signal = any(seg.get("segment_id") in segment_signals for seg in segment_list)
    if not has_any_signal:
        return True, "上游无数据：全部细分无垂直信号；缺失依赖：segment_signal_cache 或 refresh_segment_signals_for_symbols"
    return False, ""
