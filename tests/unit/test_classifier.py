# [Ref: 01_语义分类器] Module A 语义分类器单测：接口、结构、逻辑、配置变更
# 四项 100%：接口 100%、结构 100%、逻辑功能 100%、代码测试 100%

import os
import tempfile
from pathlib import Path

import pytest

from diting.classifier import SemanticClassifier
from diting.classifier.semantic import load_rules
from diting.protocols.classifier_pb2 import (
    ClassifierOutput,
    DomainTag,
    TagWithConfidence,
)


# ----- 接口 100%：ClassifierOutput 契约、可被 B/D 消费 -----


def test_classifier_output_structure():
    """输出符合 ClassifierOutput.proto：symbol、tags、correlation_id。"""
    clf = SemanticClassifier(rules={})
    out = clf.classify("000998.SZ", correlation_id="req-1")
    assert isinstance(out, ClassifierOutput)
    assert out.symbol == "000998.SZ"
    assert out.correlation_id == "req-1"
    assert isinstance(out.tags, list)
    for t in out.tags:
        assert isinstance(t, TagWithConfidence)
        assert hasattr(t, "domain_tag")
        assert hasattr(t, "confidence")
        assert 0 <= t.confidence <= 1.0


def test_domain_tag_enum_values():
    """DomainTag 枚举与 Proto 一致，供 B/D 消费。"""
    assert DomainTag.DOMAIN_UNSPECIFIED == 0
    assert DomainTag.AGRI == 1
    assert DomainTag.TECH == 2
    assert DomainTag.GEO == 3
    assert DomainTag.UNKNOWN == 4


# ----- 结构 100%：目录与 config 存在、YAML 可加载 -----


def test_classifier_package_and_config_exist():
    """diting/classifier 存在；config/classifier_rules.yaml 存在且可加载。"""
    root = Path(__file__).resolve().parents[2]
    assert (root / "diting" / "classifier").is_dir()
    rules_path = root / "config" / "classifier_rules.yaml"
    assert rules_path.is_file(), "config/classifier_rules.yaml 应存在"
    rules = load_rules(rules_path)
    assert "agri" in rules
    assert "tech" in rules
    assert "geo" in rules
    assert "unknown" in rules


# ----- 逻辑功能 100%：AGRI/TECH/GEO/UNKNOWN、Mock 标的 -----


def test_agri_tag_for_longping():
    """000998.SZ（隆平高科）预期为 AGRI。"""
    clf = SemanticClassifier(rules=load_rules())
    out = clf.classify("000998.SZ")
    tags = [t.domain_tag for t in out.tags]
    assert DomainTag.AGRI in tags


def test_tech_tag_for_smic():
    """688981.SH（中芯国际）预期为 TECH。"""
    clf = SemanticClassifier(rules=load_rules())
    out = clf.classify("688981.SH")
    tags = [t.domain_tag for t in out.tags]
    assert DomainTag.TECH in tags


def test_geo_tag_for_zijin():
    """601899.SH（紫金矿业）预期为 GEO。"""
    clf = SemanticClassifier(rules=load_rules())
    out = clf.classify("601899.SH")
    tags = [t.domain_tag for t in out.tags]
    assert DomainTag.GEO in tags


def test_unknown_for_unlisted_symbol():
    """未在 Mock 中的标的归为 UNKNOWN。"""
    clf = SemanticClassifier(rules=load_rules())
    out = clf.classify("999999.SZ")
    assert len(out.tags) >= 1
    assert out.tags[0].domain_tag == DomainTag.UNKNOWN
    assert 0 <= out.tags[0].confidence <= 1.0


def test_confidence_in_range():
    """所有 tag 置信度在 0.0–1.0。"""
    clf = SemanticClassifier(rules=load_rules())
    for symbol in ["000998.SZ", "688981.SH", "601899.SH", "999999.SZ"]:
        out = clf.classify(symbol)
        for t in out.tags:
            assert 0.0 <= t.confidence <= 1.0


# ----- 逻辑功能 100%：规则由 YAML 驱动，配置变更生效 -----


def test_rules_driven_by_yaml():
    """变更 YAML 后分类结果随之变化。"""
    rules = {
        "agri": {"industry_keywords": ["农林牧渔"], "revenue_ratio_threshold": 0.5},
        "tech": {"industry_keywords": ["电子"], "rnd_ratio_threshold": 0.1},
        "geo": {"industry_keywords": ["有色金属"], "commodity_revenue_ratio_threshold": 0.5},
        "unknown": {"default_confidence": 0.5},
    }
    clf = SemanticClassifier(rules=rules)
    out = clf.classify("000998.SZ")
    assert any(t.domain_tag == DomainTag.AGRI for t in out.tags)

    # 提高 AGRI 阈值使 000998 不再匹配（仅靠行业关键词仍会匹配）
    rules["agri"]["industry_keywords"] = []
    rules["agri"]["revenue_ratio_threshold"] = 0.99
    clf2 = SemanticClassifier(rules=rules)
    # Mock 中 000998 营收占比 0.85 < 0.99，且无关键词 -> UNKNOWN
    out2 = clf2.classify("000998.SZ")
    tags2 = [t.domain_tag for t in out2.tags]
    assert DomainTag.UNKNOWN in tags2 or DomainTag.AGRI not in tags2


# ----- 边界：空规则、空 symbol -----


def test_empty_rules_falls_to_unknown():
    """无规则且无匹配数据时归为 UNKNOWN。"""
    def no_match_provider(_symbol):
        return ("未知", 0.0, 0.0, 0.0)

    clf = SemanticClassifier(rules={}, industry_revenue_provider=no_match_provider)
    out = clf.classify("ANY.SZ")
    assert len(out.tags) >= 1
    assert out.tags[0].domain_tag == DomainTag.UNKNOWN
    assert out.symbol == "ANY.SZ"


def test_custom_provider():
    """自定义 industry_revenue_provider 可注入。"""
    def provider(symbol: str):
        if symbol == "CUSTOM.AGRI":
            return ("农林牧渔", 0.6, 0.0, 0.0)
        return ("未知", 0.0, 0.0, 0.0)

    clf = SemanticClassifier(rules=load_rules(), industry_revenue_provider=provider)
    out = clf.classify("CUSTOM.AGRI")
    assert any(t.domain_tag == DomainTag.AGRI for t in out.tags)


def test_load_rules_from_path():
    """load_rules 可从指定路径加载。"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("agri:\n  industry_keywords: [农业]\nunknown:\n  default_confidence: 0.6\n")
        path = f.name
    try:
        r = load_rules(path)
        assert r.get("agri", {}).get("industry_keywords") == ["农业"]
        assert r.get("unknown", {}).get("default_confidence") == 0.6
    finally:
        os.unlink(path)
