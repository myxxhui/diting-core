# [Ref: 02_B模块策略] classifier_gate：门控匹配模式与领域桶

from diting.scanner.classifier_gate import matches_classifier_allowed


def test_matches_exact_primary_only():
    tags = [{"domain_tag": 1, "confidence": 0.9, "domain_label": ""}]
    assert matches_classifier_allowed("农业", tags, ["农业"], match_mode="exact_primary")
    assert not matches_classifier_allowed("自定义水电", tags, ["农业"], match_mode="exact_primary")


def test_domain_or_primary_matches_bucket_when_primary_differs():
    """primary_tag 为长自定义串时，仍可按 tags_json 领域桶命中「农业」。"""
    tags = [{"domain_tag": 1, "confidence": 0.9, "domain_label": ""}]
    assert matches_classifier_allowed("某长自定义标签", tags, ["农业"], match_mode="domain_or_primary")


def test_domain_or_primary_matches_primary_string():
    tags = [{"domain_tag": 5, "confidence": 0.9, "domain_label": "水电"}]
    assert matches_classifier_allowed("水电", tags, ["水电"], match_mode="domain_or_primary")


def test_domain_bucket_custom():
    tags = [{"domain_tag": 5, "confidence": 0.9, "domain_label": "x"}]
    assert matches_classifier_allowed("任意展示名", tags, ["自定义"], match_mode="domain_or_primary")


def test_empty_allowed_always_true():
    assert matches_classifier_allowed("x", [], [], match_mode="domain_or_primary")
