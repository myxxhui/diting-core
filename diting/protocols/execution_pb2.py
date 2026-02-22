# [Ref: 03_原子目标与规约/_共享规约/04_全链路通信协议矩阵] 占位，替代 protoc 生成
# 正式流程：design/protocols/execution/order.proto -> protoc -> 本模块

class OrderType:
    MARKET = 0
    LIMIT = 1

class AuditStatus:
    PENDING_APPROVAL = 0
    APPROVED = 1
    REJECTED = 2
    AUTO_EXECUTED = 3

class TradeOrder:
    def __init__(self, order_id="", symbol="", type=0, price=0.0, quantity=0,
                 audit_status=0, strategy_source=""):
        self.order_id = order_id
        self.symbol = symbol
        self.type = type
        self.price = price
        self.quantity = quantity
        self.audit_status = audit_status
        self.strategy_source = strategy_source

class OrderStatus:
    def __init__(self, order_id="", status=0):
        self.order_id = order_id
        self.status = status  # int 或兼容字符串 "PENDING"/"FILLED"/"CANCELLED"/"UNKNOWN"
