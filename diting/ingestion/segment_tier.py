# [Ref: 12_右脑数据支撑与Segment规约] segment 层级：1=domain 2=sector 3=business
from __future__ import annotations

from typing import Optional

# 与规约、segment_registry.segment_tier 数值一致
TIER_DOMAIN = 1
TIER_SECTOR = 2
TIER_BUSINESS = 3

# 主营披露哈希 seg_bp_* 行缺省为「具体业务层」
DISCLOSURE_DEFAULT_TIER = TIER_BUSINESS


def tier_int_to_signal_key(tier: Optional[int], segment_id: str) -> str:
    """
    将 DB 中的 segment_tier 转为 signal_understanding.model_override_by_tier 使用的键：
    domain | sector | business。
    """
    tid = segment_id or ""
    if tier == TIER_DOMAIN:
        return "domain"
    if tier == TIER_SECTOR:
        return "sector"
    if tier == TIER_BUSINESS:
        return "business"
    if tid.startswith("seg_bp_"):
        return "business"
    # 命名型 segment_id（非哈希）：按下划线段数粗分层级
    segs = [x for x in tid.split("_") if x]
    if len(segs) <= 1:
        return "domain"
    if len(segs) == 2:
        return "sector"
    return "business"


def tier_int_to_label_cn(tier: Optional[int]) -> str:
    """终端/报表用短标签。"""
    if tier == TIER_DOMAIN:
        return "L1·领域"
    if tier == TIER_SECTOR:
        return "L2·板块"
    if tier == TIER_BUSINESS:
        return "L3·业务"
    return "—"
