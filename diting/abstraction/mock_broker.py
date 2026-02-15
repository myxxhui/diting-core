# 占位 Broker 实现 [Ref: 05_接口抽象层规约] 使「导入 + 最小调用」可运行
from diting.abstraction.broker import (
    BrokerDriver,
    OrderPlaceholder,
    OrderStatusPlaceholder,
)


class MockBroker(BrokerDriver):
    """回测/测试用占位实现，无业务逻辑"""

    def __init__(self, initial_cash: float = 0.0):
        self._cash = initial_cash
        self._positions: dict[str, int] = {}
        self._orders: dict[str, OrderStatusPlaceholder] = {}
        self._next_id = 0

    def get_cash_balance(self) -> float:
        return self._cash

    def get_positions(self) -> dict[str, int]:
        return dict(self._positions)

    def place_order(self, order: OrderPlaceholder) -> str:
        order_id = f"mock_{self._next_id}"
        self._next_id += 1
        self._orders[order_id] = OrderStatusPlaceholder(order_id=order_id, status="PENDING")
        return order_id

    def cancel_order(self, order_id: str) -> bool:
        if order_id in self._orders:
            self._orders[order_id] = OrderStatusPlaceholder(order_id=order_id, status="CANCELLED")
            return True
        return False

    def get_order_status(self, order_id: str) -> OrderStatusPlaceholder:
        return self._orders.get(
            order_id, OrderStatusPlaceholder(order_id=order_id, status="UNKNOWN")
        )
