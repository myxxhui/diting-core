# Table-Driven Tests 覆盖接口与占位 [Ref: 03_架构设计共识与协作元规则]
# 与 04_/05_ 规约对齐；骨架期验证「导入 + 最小调用」可运行
import pytest

from diting.abstraction.broker import BrokerDriver
from diting.protocols.execution_pb2 import TradeOrder, OrderStatus
from diting.abstraction.brain import CognitiveEngine
from diting.abstraction.feed import MarketDataFeed
from diting.abstraction.mock_broker import MockBroker
from diting.abstraction.mock_brain import MockBrain
from diting.abstraction.mock_feed import MockFeed


# ----- BrokerDriver + MockBroker -----

@pytest.mark.parametrize("initial_cash,expected", [
    (0.0, 0.0),
    (100000.0, 100000.0),
])
def test_mock_broker_get_cash_balance(initial_cash: float, expected: float) -> None:
    broker: BrokerDriver = MockBroker(initial_cash=initial_cash)
    assert broker.get_cash_balance() == expected


@pytest.mark.parametrize("order_symbol,order_qty,order_price", [
    ("000001.SZ", 100, 10.5),
    ("600000.SH", 200, 0.0),
])
def test_mock_broker_place_order(order_symbol: str, order_qty: int, order_price: float) -> None:
    broker = MockBroker(initial_cash=10000.0)
    order = TradeOrder(symbol=order_symbol, quantity=order_qty, price=order_price)
    order_id = broker.place_order(order)
    assert order_id.startswith("mock_")
    status = broker.get_order_status(order_id)
    assert status.order_id == order_id
    assert status.status in ("PENDING", "FILLED", "CANCELLED", "UNKNOWN")


def test_mock_broker_cancel_order() -> None:
    broker = MockBroker()
    order = TradeOrder(symbol="000001.SZ", quantity=100, price=10.0)
    order_id = broker.place_order(order)
    assert broker.cancel_order(order_id) is True
    assert broker.get_order_status(order_id).status == "CANCELLED"


def test_mock_broker_get_positions() -> None:
    broker = MockBroker()
    assert broker.get_positions() == {}


# ----- CognitiveEngine + MockBrain -----

@pytest.mark.parametrize("context,schema_key", [
    ("news summary", "type"),
    ("", "properties"),
])
def test_mock_brain_reason(context: str, schema_key: str) -> None:
    engine: CognitiveEngine = MockBrain()
    schema = {schema_key: "object"}
    out = engine.reason(context, schema)
    assert "is_supported" in out
    assert "confidence" in out
    assert "reasoning_summary" in out


@pytest.mark.parametrize("session_id", ["s1", "session-2"])
def test_mock_brain_audit_thought_process(session_id: str) -> None:
    engine = MockBrain()
    cot = engine.audit_thought_process(session_id)
    assert session_id in cot


# ----- MarketDataFeed + MockFeed -----

@pytest.mark.parametrize("symbol,period,limit", [
    ("000001.SZ", "1d", 10),
    ("600000.SH", "1h", 1),
])
def test_mock_feed_get_history(symbol: str, period: str, limit: int) -> None:
    feed: MarketDataFeed = MockFeed()
    result = feed.get_history(symbol, period, limit)
    assert result == []


@pytest.mark.parametrize("symbols", [
    [],
    ["000001.SZ"],
    ["000001.SZ", "600000.SH"],
])
def test_mock_feed_get_snapshot(symbols: list) -> None:
    feed = MockFeed()
    result = feed.get_snapshot(symbols)
    assert result == []
