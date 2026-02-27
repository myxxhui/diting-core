# [Ref: 01_语义分类器] [Ref: 09_核心模块架构规约]
# 占位/契约：与 design/protocols/classifier/classifier_output.proto 一致
# 正式流程：design/protocols/classifier/classifier_output.proto -> protoc -> 本模块

class DomainTag:
    """DomainTag enum: 与 ClassifierOutput.proto 一致，供 Module B/D 消费。"""
    DOMAIN_UNSPECIFIED = 0
    AGRI = 1
    TECH = 2
    GEO = 3
    UNKNOWN = 4


class TagWithConfidence:
    """单条 Tag + 置信度 (0.0-1.0)。"""

    def __init__(self, domain_tag=0, confidence=0.0):
        self.domain_tag = domain_tag  # DomainTag 枚举值
        self.confidence = float(confidence)


class ClassifierOutput:
    """Module A 输出：symbol + tags（Domain Tag 列表 + 置信度），可被 B/D 消费。"""

    def __init__(self, symbol="", tags=None, correlation_id=""):
        self.symbol = symbol
        self.tags = list(tags) if tags else []
        self.correlation_id = correlation_id or ""
