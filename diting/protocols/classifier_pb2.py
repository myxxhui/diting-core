# [Ref: 01_语义分类器] [Ref: 09_核心模块架构规约]
# 占位/契约：与 design/protocols/classifier/classifier_output.proto 一致
# 正式流程：design/protocols/classifier/classifier_output.proto -> protoc -> 本模块

class DomainTag:
    """DomainTag enum: 与 ClassifierOutput.proto 一致，供 Module B/D 消费。
    存储与展示以中文为主：1=农业 2=科技 3=宏观 4=未知 5=自定义（见 l2_snapshot_writer._DOMAIN_TAG_TO_STR）。"""
    DOMAIN_UNSPECIFIED = 0  # 未指定
    AGRI = 1   # 农业
    TECH = 2   # 科技
    GEO = 3    # 宏观
    UNKNOWN = 4  # 未知
    DOMAIN_CUSTOM = 5  # 自定义，展示名由 domain_label 提供


class TagWithConfidence:
    """单条 Tag + 置信度 (0.0-1.0)；DOMAIN_CUSTOM 时用 domain_label 展示。"""

    def __init__(self, domain_tag=0, confidence=0.0, domain_label=""):
        self.domain_tag = domain_tag  # DomainTag 枚举值
        self.confidence = float(confidence)
        self.domain_label = domain_label or ""  # 自定义类别展示名


class SegmentShare:
    """主营/细分占比，供 Module C 对齐与加权（[Ref: 12_右脑数据支撑与Segment规约]）。"""

    def __init__(self, segment_id="", revenue_share=0.0, is_primary=False):
        self.segment_id = str(segment_id or "").strip()
        self.revenue_share = float(revenue_share)
        self.is_primary = bool(is_primary)


class ClassifierOutput:
    """Module A 输出：symbol + tags + segment_shares（细分列表），可被 B/C/D 消费。"""

    def __init__(self, symbol="", tags=None, correlation_id="", segment_shares=None):
        self.symbol = symbol
        self.tags = list(tags) if tags else []
        self.correlation_id = correlation_id or ""
        self.segment_shares = list(segment_shares) if segment_shares else []
