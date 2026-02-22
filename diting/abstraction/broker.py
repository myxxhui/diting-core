# [Ref: 03_原子目标与规约/_共享规约/05_接口抽象层规约] 交易执行抽象
from abc import ABC, abstractmethod
from typing import Dict

# 占位阶段使用本地 stub；正式由 design/protocols/execution/order.proto 生成
from diting.protocols.execution_pb2 import TradeOrder, OrderStatus


class BrokerDriver(ABC):
    """交易网关的标准接口。研产同构、经纪商解耦。"""

    @abstractmethod
    def get_cash_balance(self) -> float:
        """获取当前可用资金"""
        pass

    @abstractmethod
    def get_positions(self) -> Dict[str, int]:
        """获取当前持仓。返回 {symbol: quantity}"""
        pass

    @abstractmethod
    def place_order(self, order: TradeOrder) -> str:
        """下单接口。返回 order_id（系统内部ID）"""
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """撤单接口"""
        pass

    @abstractmethod
    def get_order_status(self, order_id: str) -> OrderStatus:
        """查询订单状态（用于异步轮询）"""
        pass
