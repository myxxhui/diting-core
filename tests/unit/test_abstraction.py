# [Ref: 03_原子目标与规约/_共享规约/05_接口抽象层规约] 接口与 Proto 占位可导入、最小调用可运行
"""Stage1-02 验收：核心接口与 Proto 占位；make test 通过。"""
import sys
sys.path.insert(0, ".")

def test_import_broker_and_proto():
    from diting.abstraction.broker import BrokerDriver
    from diting.protocols.execution_pb2 import TradeOrder, OrderStatus
    o = TradeOrder(symbol="000001.SZ", quantity=100, order_id="test-1")
    assert o.symbol == "000001.SZ"
    assert o.quantity == 100

def test_import_cognitive_engine():
    from diting.abstraction.brain import CognitiveEngine
    assert CognitiveEngine is not None

def test_import_market_data_feed():
    from diting.abstraction.feed import MarketDataFeed
    assert MarketDataFeed is not None

def test_proto_files_exist():
    import os
    base = os.path.join(os.path.dirname(__file__), "..", "..", "design", "protocols")
    assert os.path.isdir(base)
    for path in ["brain/expert.proto", "brain/verdict.proto", "execution/order.proto",
                 "classifier/classifier_output.proto", "quant/quant_signal.proto",
                 "risk/telemetry.proto", "trade_signal.proto"]:
        assert os.path.isfile(os.path.join(base, path)), path

if __name__ == "__main__":
    test_import_broker_and_proto()
    test_import_cognitive_engine()
    test_import_market_data_feed()
    test_proto_files_exist()
    print("All abstraction and proto placeholder tests passed.")
