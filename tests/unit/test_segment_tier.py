# [Ref: 12_右脑数据支撑与Segment规约] segment_tier 与 signal 层 tier 键

from diting.ingestion.segment_tier import (
    DISCLOSURE_DEFAULT_TIER,
    TIER_BUSINESS,
    TIER_DOMAIN,
    TIER_SECTOR,
    tier_int_to_label_cn,
    tier_int_to_signal_key,
)


def test_tier_int_to_signal_key_from_db():
    assert tier_int_to_signal_key(TIER_DOMAIN, "tech") == "domain"
    assert tier_int_to_signal_key(TIER_SECTOR, "tech_ai") == "sector"
    assert tier_int_to_signal_key(TIER_BUSINESS, "seg_bp_abc") == "business"


def test_tier_int_to_signal_key_seg_bp_default():
    assert tier_int_to_signal_key(None, "seg_bp_0123456789abcdef") == "business"


def test_tier_int_to_label_cn():
    assert tier_int_to_label_cn(TIER_DOMAIN) == "L1·领域"
    assert tier_int_to_label_cn(TIER_SECTOR) == "L2·板块"
    assert tier_int_to_label_cn(TIER_BUSINESS) == "L3·业务"
    assert tier_int_to_label_cn(None) == "—"


def test_disclosure_default_tier_constant():
    assert DISCLOSURE_DEFAULT_TIER == TIER_BUSINESS == 3
