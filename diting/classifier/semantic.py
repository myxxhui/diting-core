# [Ref: 01_语义分类器] [Ref: 09_核心模块架构规约] [Ref: 11_数据采集与输入层规约]
# Module A 语义分类器：标的池由 get_current_a_share_universe() 或调用方传入；对全部 N 只全量分类

import logging
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import yaml

from diting.protocols.classifier_pb2 import (
    ClassifierOutput,
    DomainTag,
    SegmentShare,
    TagWithConfidence,
)

# 规则键 -> DomainTag（农业/科技/宏观 或 兼容 agri/tech/geo，其余用 DOMAIN_CUSTOM + label）
_DOMAIN_TAG_BY_ID = {
    "农业": DomainTag.AGRI, "科技": DomainTag.TECH, "宏观": DomainTag.GEO,
    "agri": DomainTag.AGRI, "tech": DomainTag.TECH, "geo": DomainTag.GEO,
}

logger = logging.getLogger(__name__)


def _is_bare_power_industry(industry: str) -> bool:
    """申万二级常为单独「电力」；此类不做 YAML 粗标签兜底，只走主营披露映射。"""
    s = (industry or "").strip().replace(" ", "")
    if not s:
        return False
    return s in ("电力", "电力行业")


def refine_power_label_from_disclosure(name_cn: str) -> Optional[str]:
    """
    申万行业名仅为「电力」时，用主营披露分部名称映射为运营子类（无映射规则时由调用方使用披露原文）。
    与 classifier_rules 中 火电/水电/风电运营/新能源发电 等展示名对齐；可扩展售电/配电/电网等垂直口径。
    """
    s = (name_cn or "").strip()
    if len(s) < 2:
        return None
    # 更具体优先；映射结果需贴切、可区分（避免过分精简）
    if any(k in s for k in ("抽水蓄能", "蓄能电站")):
        return "水力发电"
    if any(k in s for k in ("水力", "水电", "水利发电")):
        return "水力发电"
    if any(k in s for k in ("火力", "燃煤", "煤电", "热电联产", "热电", "火电")):
        return "火力发电"
    if any(k in s for k in ("核电", "核能", "核力")):
        return "核电"
    if any(k in s for k in ("风电", "风力发电", "风力", "风电运营", "风电场", "风力发电运营")):
        return "风力发电"
    if any(k in s for k in ("太阳能", "光伏发电", "光伏", "光热发电", "垃圾发电", "生物质", "新能源发电")):
        return "新能源发电"
    if any(k in s for k in ("清洁能源", "绿电", "可再生能源")):
        return "新能源发电"
    if "燃气" in s:
        return "燃气供应"
    if any(k in s for k in ("热力", "供热", "供暖", "蒸汽销售", "蒸汽")):
        return "热力供应"
    if "售电" in s or any(k in s for k in ("电力销售", "供电业务", "供电服务", "销售电力", "售电收入", "电力产品销售")):
        return "售电业务"
    if any(k in s for k in ("配电", "输配电", "配电网", "电网配电")):
        return "配电运营"
    if any(k in s for k in ("输电", "电网运营", "电网建设")):
        return "电网运营"
    if "综合能源" in s:
        return "综合能源服务"
    if any(k in s for k in ("电力销售", "电力供应", "售电业务", "供电")):
        return "售电业务"
    if any(k in s for k in ("电力生产", "发电业务", "发电量", "发电-电力", "发电", "电力生产销售")):
        return "发电与供电业务"
    if any(k in s for k in ("储能", "储能系统")):
        return "储能业务"
    if any(k in s for k in ("电力分部", "电力板块", "电力业务")):
        return "发电与供电业务"
    # 裸「电力」兜底（申万仅电力且披露无细分时）
    if s == "电力":
        return "发电与供电业务"
    return None


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
        business_segment_provider: Optional[Callable[[str], List[SegmentShare]]] = None,
        segment_top1_name_provider: Optional[Callable[[str], Optional[str]]] = None,
        segment_disclosure_names_provider: Optional[Callable[[str], List[str]]] = None,
    ):
        """
        :param rules: 若提供则直接使用，否则从 rules_path 或默认路径加载
        :param rules_path: YAML 规则文件路径
        :param industry_revenue_provider: (symbol) -> (industry_name, revenue_ratio, rnd_ratio, commodity_revenue_ratio)
            不提供时使用内置 Mock（数据不可用时 Mock，见 01_语义分类器_实践）
        :param business_segment_provider: (symbol) -> segment_shares；有 L2 symbol_business_profile 时由采集写入；
            无表数据时 segment_shares 为 seg_no_disclosure（见 _fallback_segment_shares_no_disclosure）。
        :param segment_top1_name_provider: (symbol) -> 主营披露 Top1 中文名；申万仅为「电力」且无 names 时用。
        :param segment_disclosure_names_provider: (symbol) -> 主营披露分部名列表（营收降序）；
            申万「电力」时逐条映射为运营子类；无映射规则时用披露原文作垂直标签。
        """
        if rules is not None:
            self._rules = rules
        else:
            self._rules = load_rules(rules_path)
        self._provider = industry_revenue_provider or _default_mock_provider()
        self._business_segment_provider = business_segment_provider
        self._segment_top1_name_provider = segment_top1_name_provider
        self._segment_disclosure_names_provider = segment_disclosure_names_provider

    def classify(
        self,
        symbol: str,
        correlation_id: str = "",
    ) -> ClassifierOutput:
        """
        对标的进行分类，返回 ClassifierOutput（Domain Tag 列表 + 置信度）。
        按 YAML 中 categories 顺序匹配；agri/tech/geo 对应枚举，其余为 DOMAIN_CUSTOM + domain_label。
        """
        industry, revenue_ratio, rnd_ratio, commodity_ratio = self._provider(symbol)
        tags_with_conf: List[TagWithConfidence] = []
        categories = self._rules.get("categories") or []
        # 兼容旧版 YAML：仅有 农业/科技/宏观 或 agri/tech/geo 时按原逻辑
        if not categories and (self._rules.get("农业") or self._rules.get("agri") or self._rules.get("科技") or self._rules.get("tech") or self._rules.get("宏观") or self._rules.get("geo")):
            tags_with_conf = self._classify_legacy(industry, revenue_ratio, rnd_ratio, commodity_ratio)
        else:
            confidence_matched = 0.95
            for cat in categories:
                raw_id = (cat.get("id") or "").strip()
                cat_id = raw_id.lower() if raw_id.isascii() else raw_id  # 中文 id 不转小写
                label = (cat.get("label") or cat.get("id") or "").strip() or cat_id
                keywords = cat.get("industry_keywords") or []
                keyword_match = any(k in industry for k in keywords)
                rev_th = cat.get("revenue_ratio_threshold")
                rnd_th = cat.get("rnd_ratio_threshold")
                comm_th = cat.get("commodity_revenue_ratio_threshold")
                ratio_match = (
                    (rev_th is not None and revenue_ratio >= rev_th)
                    or (rnd_th is not None and rnd_ratio >= rnd_th)
                    or (comm_th is not None and commodity_ratio >= comm_th)
                )
                if not keyword_match and not ratio_match:
                    continue
                if cat_id in _DOMAIN_TAG_BY_ID:
                    domain_tag = _DOMAIN_TAG_BY_ID[cat_id]
                    tags_with_conf.append(
                        TagWithConfidence(domain_tag=domain_tag, confidence=confidence_matched)
                    )
                else:
                    tags_with_conf.append(
                        TagWithConfidence(
                            domain_tag=DomainTag.DOMAIN_CUSTOM,
                            confidence=confidence_matched,
                            domain_label=label,
                        )
                    )
                break  # 按顺序只取第一个匹配

        unknown_cfg = self._rules.get("unknown") or {}
        default_conf = unknown_cfg.get("default_confidence") or 0.5
        if not tags_with_conf and _is_bare_power_industry(industry):
            tags_with_conf = self._tags_for_bare_power_industry(symbol, default_conf)
        if not tags_with_conf:
            tags_with_conf.append(
                TagWithConfidence(domain_tag=DomainTag.UNKNOWN, confidence=default_conf)
            )

        segment_shares: List[SegmentShare] = []
        if self._business_segment_provider:
            try:
                segment_shares = self._business_segment_provider(symbol) or []
            except Exception as e:
                logger.debug("business_segment_provider(%s): %s", symbol, e)
                segment_shares = []
        if not segment_shares:
            segment_shares = self._fallback_segment_shares_no_disclosure()
        return ClassifierOutput(
            symbol=symbol,
            tags=tags_with_conf,
            correlation_id=correlation_id,
            segment_shares=segment_shares,
        )

    def _tags_for_bare_power_industry(
        self, symbol: str, default_conf: float
    ) -> List[TagWithConfidence]:
        """
        申万二级仅为「电力」：无 YAML 粗标签；必须有 L2 主营披露才可分类。
        优先映射为水电/火电等规范子类；否则用披露分部原文；无披露则未知。
        """
        names: List[str] = []
        if self._segment_disclosure_names_provider:
            try:
                names = self._segment_disclosure_names_provider(symbol) or []
            except Exception as e:
                logger.debug("segment_disclosure_names_provider(%s): %s", symbol, e)
                names = []
        if not names and self._segment_top1_name_provider:
            try:
                one = self._segment_top1_name_provider(symbol)
                if one and str(one).strip():
                    names = [str(one).strip()]
            except Exception as e:
                logger.debug("segment_top1_name_provider(%s): %s", symbol, e)
        refined: List[str] = []
        seen: set = set()
        for n in names:
            r = refine_power_label_from_disclosure(n)
            if r and r not in seen:
                seen.add(r)
                refined.append(r)
        if refined:
            c0 = 0.88
            tags: List[TagWithConfidence] = [
                TagWithConfidence(
                    domain_tag=DomainTag.DOMAIN_CUSTOM,
                    confidence=c0,
                    domain_label=refined[0],
                )
            ]
            sec = min(0.82, c0)
            for lab in refined[1:4]:
                tags.append(
                    TagWithConfidence(
                        domain_tag=DomainTag.DOMAIN_CUSTOM,
                        confidence=sec,
                        domain_label=lab,
                    )
                )
            return tags
        if names:
            raw = (names[0] or "").strip()[:48]
            if raw:
                return [
                    TagWithConfidence(
                        domain_tag=DomainTag.DOMAIN_CUSTOM,
                        confidence=0.78,
                        domain_label=raw,
                    )
                ]
        return [
            TagWithConfidence(
                domain_tag=DomainTag.DOMAIN_CUSTOM,
                confidence=default_conf,
                domain_label="无披露",
            )
        ]

    def _fallback_segment_shares_no_disclosure(self) -> List[SegmentShare]:
        """
        无 L2 symbol_business_profile 行时：不伪造与行业/主 Tag 绑定的分部 ID，统一标注无披露。
        """
        return [SegmentShare(segment_id="seg_no_disclosure", revenue_share=1.0, is_primary=True)]

    def _classify_legacy(
        self, industry: str, revenue_ratio: float, rnd_ratio: float, commodity_ratio: float
    ) -> List[TagWithConfidence]:
        """兼容旧版 YAML（仅有 agri/tech/geo 顶层键）。"""
        tags: List[TagWithConfidence] = []
        agri_cfg = self._rules.get("agri") or {}
        if any(k in industry for k in (agri_cfg.get("industry_keywords") or [])) or revenue_ratio >= (agri_cfg.get("revenue_ratio_threshold") or 0.5):
            tags.append(TagWithConfidence(domain_tag=DomainTag.AGRI, confidence=0.95))
        tech_cfg = self._rules.get("tech") or {}
        if any(k in industry for k in (tech_cfg.get("industry_keywords") or [])) or rnd_ratio >= (tech_cfg.get("rnd_ratio_threshold") or 0.1):
            tags.append(TagWithConfidence(domain_tag=DomainTag.TECH, confidence=0.95))
        geo_cfg = self._rules.get("geo") or {}
        if any(k in industry for k in (geo_cfg.get("industry_keywords") or [])) or commodity_ratio >= (geo_cfg.get("commodity_revenue_ratio_threshold") or 0.5):
            tags.append(TagWithConfidence(domain_tag=DomainTag.GEO, confidence=0.95))
        return tags

    def classify_batch(
        self,
        universe: List[str],
        correlation_id: str = "",
    ) -> List[ClassifierOutput]:
        """
        对标的池全量分类，与 09_/11_ 约定一致；同批与 Module B 使用同一 universe 时由调度保证。
        """
        logger.info("本批分类标的数量: %s", len(universe))
        return [self.classify(s, correlation_id=correlation_id) for s in universe]

    @classmethod
    def run_full(
        cls,
        universe: Optional[List[str]] = None,
        correlation_id: str = "",
        **classifier_kwargs,
    ) -> List[ClassifierOutput]:
        """
        执行入口：先通过 get_current_a_share_universe() 获取标的池（或使用调用方传入的 universe），
        再对全部 N 只全量分类；日志输出 len(universe)。与 11_/09_ 同批一致约定一致。
        """
        if universe is None:
            from diting.universe import get_current_a_share_universe, parse_symbol_list_from_env
            universe = parse_symbol_list_from_env("DITING_SYMBOLS") or parse_symbol_list_from_env("MODULE_AB_SYMBOLS")
            if not universe:
                universe = get_current_a_share_universe()
        logger.info("语义分类器 run_full：本批 universe 数量 %s", len(universe))
        inst = cls(**classifier_kwargs)
        return inst.classify_batch(universe, correlation_id=correlation_id)


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
