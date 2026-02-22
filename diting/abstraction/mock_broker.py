# 占位 Broker 实现 [Ref: 05_接口抽象层规约] 使「导入 + 最小调用」可运行
from diting.abstraction.broker import BrokerDriver
from diting.protocols.execution_pb2 import TradeOrder, OrderStatus


class MockBroker(BrokerDriver):
    """回测/测试用占位实现，无业务逻辑"""

    def __init__(self, initial_cash: float = 0.0):
        self._cash = initial_cash
        self._positions: dict[str, int] = {}
        self._orders: dict[str, OrderStatus] = {}
        self._next_id = 0

    def get_cash_balance(self) -> float:
        return self._cash

    def get_positions(self) -> dict[str, int]:
        return dict(self._positions)

    def place_order(self, order: TradeOrder) -> str:
        order_id = f"mock_{self._next_id}"
        self._next_id += 1
        st = OrderStatus(order_id=order_id, status=0)
        st.status = "PENDING"  # 占位兼容字符串
        self._orders[order_id] = st
        return order_id

    def cancel_order(self, order_id: str) -> bool:
        if order_id in self._orders:
            self._orders[order_id].status = "CANCELLED"
            return True
        return False

    def get_order_status(self, order_id: str) -> OrderStatus:
        o = self._orders.get(order_id)
        if o is None:
            o = OrderStatus(order_id=order_id, status=0)
            o.status = "UNKNOWN"
            return o
        return o
