# 交易执行抽象 [Ref: 05_接口抽象层规约 A]
# 与 04_全链路通信协议矩阵 execution 对齐；骨架期使用占位类型，逻辑填充期接入 execution_pb2
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict

# 占位类型，替代 execution_pb2.TradeOrder / OrderStatus，使「导入 + 最小调用」可运行
@dataclass
class OrderPlaceholder:
    """占位订单类型 [Ref: 04_全链路通信协议矩阵 execution]"""
    symbol: str
    quantity: int
    price: float
    order_id: str = ""


@dataclass
class OrderStatusPlaceholder:
    """占位订单状态 [Ref: 04_全链路通信协议矩阵]"""
    order_id: str
    status: str  # e.g. PENDING, FILLED, CANCELLED


class BrokerDriver(ABC):
    """
    交易网关的标准接口 [Ref: 05_接口抽象层规约]。
    无论是 MiniQMT, PTrade 还是回测引擎，都必须实现此接口。
    """

    @abstractmethod
    def get_cash_balance(self) -> float:
        """获取当前可用资金"""
        pass

    @abstractmethod
    def get_positions(self) -> Dict[str, int]:
        """获取当前持仓 :return: {symbol: quantity}"""
        pass

    @abstractmethod
    def place_order(self, order: OrderPlaceholder) -> str:
        """下单接口 :param order: 订单对象 :return: order_id"""
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """撤单接口"""
        pass

    @abstractmethod
    def get_order_status(self, order_id: str) -> OrderStatusPlaceholder:
        """查询订单状态"""
        pass
