# [Ref: 03_原子目标与规约/_共享规约/04_全链路通信协议矩阵] 占位，替代 protoc 生成
# 正式流程：design/protocols/brain/expert.proto -> protoc -> 本模块

class ExpertOpinion:
    def __init__(self, symbol="", domain=0, is_supported=False, direction=0,
                 confidence=0.0, reasoning_summary="", risk_factors=None, timestamp=0, horizon=0):
        self.symbol = symbol
        self.domain = domain
        self.is_supported = is_supported
        self.direction = direction
        self.confidence = confidence
        self.reasoning_summary = reasoning_summary
        self.risk_factors = risk_factors or []
        self.timestamp = timestamp
        self.horizon = horizon
