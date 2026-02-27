# [Ref: 01_语义分类器] [Ref: 09_核心模块架构规约] Module A 语义分类器实现
# 输入：标的代码、申万行业、营收占比（来自 L1/L2 或 MarketDataFeed/约定表）；输出：ClassifierOutput

import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import yaml

from diting.protocols.classifier_pb2 import (
    ClassifierOutput,
    DomainTag,
    TagWithConfidence,
)

# 默认规则路径：与 DNA delivery_scope、实践文档约定一致
DEFAULT_RULES_PATH = "config/classifier_rules.yaml"


def _find_rules_path() -> Path:
    """解析 config/classifier_rules.yaml 路径：优先项目根，其次当前工作目录。"""
    root = Path(os.environ.get("DITING_CORE_ROOT", "."))
    if not root.is_absolute():
        root = Path.cwd() / root
    p = root / "config" / "classifier_rules.yaml"
    if p.exists():
        return p
    p = Path.cwd() / "config" / "classifier_rules.yaml"
    if p.exists():
        return p
    return root / "config" / "classifier_rules.yaml"


def load_rules(path: Optional[os.PathLike] = None) -> Dict[str, Any]:
    """从 YAML 加载分类规则；规则不硬编码。"""
    path = path or _find_rules_path()
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _tag_for_domain(domain: str) -> int:
    """规则键 agri/tech/geo -> DomainTag 枚举值。"""
    mapping = {"agri": DomainTag.AGRI, "tech": DomainTag.TECH, "geo": DomainTag.GEO}
    return mapping.get(domain.lower(), DomainTag.UNKNOWN)


class SemanticClassifier:
    """
    语义分类器：基于申万行业与营收占比分类，不做预测。
    输出符合 ClassifierOutput.proto；可被 Module B/D 消费。
    """

    def __init__(
        self,
        rules: Optional[Dict[str, Any]] = None,
        rules_path: Optional[os.PathLike] = None,
        industry_revenue_provider: Optional[
            Callable[[str], Tuple[str, float, float, float]]
        ] = None,
    ):
        """
        :param rules: 若提供则直接使用，否则从 rules_path 或默认路径加载
        :param rules_path: YAML 规则文件路径
        :param industry_revenue_provider: (symbol) -> (industry_name, revenue_ratio, rnd_ratio, commodity_revenue_ratio)
            不提供时使用内置 Mock（数据不可用时 Mock，见 01_语义分类器_实践）
        """
        if rules is not None:
            self._rules = rules
        else:
            self._rules = load_rules(rules_path)
        self._provider = industry_revenue_provider or _default_mock_provider()

    def classify(
        self,
        symbol: str,
        correlation_id: str = "",
    ) -> ClassifierOutput:
        """
        对标的进行分类，返回 ClassifierOutput（Domain Tag 列表 + 置信度）。
        """
        industry, revenue_ratio, rnd_ratio, commodity_ratio = self._provider(symbol)
        tags_with_conf: List[TagWithConfidence] = []

        # AGRI
        agri_cfg = self._rules.get("agri") or {}
        keywords = agri_cfg.get("industry_keywords") or []
        rev_th = agri_cfg.get("revenue_ratio_threshold") or 0.5
        if any(k in industry for k in keywords) or revenue_ratio >= rev_th:
            tags_with_conf.append(
                TagWithConfidence(domain_tag=DomainTag.AGRI, confidence=0.95)
            )

        # TECH
        tech_cfg = self._rules.get("tech") or {}
        keywords = tech_cfg.get("industry_keywords") or []
        rnd_th = tech_cfg.get("rnd_ratio_threshold") or 0.1
        if any(k in industry for k in keywords) or rnd_ratio >= rnd_th:
            tags_with_conf.append(
                TagWithConfidence(domain_tag=DomainTag.TECH, confidence=0.95)
            )

        # GEO
        geo_cfg = self._rules.get("geo") or {}
        keywords = geo_cfg.get("industry_keywords") or []
        comm_th = geo_cfg.get("commodity_revenue_ratio_threshold") or 0.5
        if any(k in industry for k in keywords) or commodity_ratio >= comm_th:
            tags_with_conf.append(
                TagWithConfidence(domain_tag=DomainTag.GEO, confidence=0.95)
            )

        # 若未匹配任何领域，归为 UNKNOWN
        unknown_cfg = self._rules.get("unknown") or {}
        default_conf = unknown_cfg.get("default_confidence") or 0.5
        if not tags_with_conf:
            tags_with_conf.append(
                TagWithConfidence(domain_tag=DomainTag.UNKNOWN, confidence=default_conf)
            )

        return ClassifierOutput(
            symbol=symbol,
            tags=tags_with_conf,
            correlation_id=correlation_id,
        )


def _default_mock_provider() -> Callable[[str], Tuple[str, float, float, float]]:
    """
    默认 Mock：真实数据不可用时使用（见 01_语义分类器_实践）。
    约定：000998.SZ 农林牧渔；688981.SH 电子/半导体；紫金矿业 有色金属。
    """

    # 示例标的 -> (行业名, 主营营收占比, 研发投入占比, 大宗商品营收占比)
    _mock = {
        "000998.SZ": ("农林牧渔", 0.85, 0.02, 0.0),
        "688981.SH": ("电子", 0.90, 0.15, 0.0),
        "601899.SH": ("有色金属", 0.10, 0.01, 0.85),  # 紫金矿业
    }

    def provider(symbol: str) -> Tuple[str, float, float, float]:
        s = symbol.strip().upper()
        if s in _mock:
            return _mock[s]
        return ("未知", 0.0, 0.0, 0.0)

    return provider
