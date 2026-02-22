# [Ref: 03_原子目标与规约/_共享规约/05_接口抽象层规约] 数据源抽象
from abc import ABC, abstractmethod
from typing import Any, List

class MarketDataFeed(ABC):
    """行情数据源标准接口。冷热分离、数据校验。"""

    @abstractmethod
    def get_history(self, symbol: str, period: str, limit: int) -> Any:
        """获取历史 K 线。返回 [datetime, open, high, low, close, volume]。"""
        pass

    @abstractmethod
    def get_snapshot(self, symbols: List[str]) -> Any:
        """获取最新一笔 Tick 快照。返回 [symbol, price, volume, timestamp]。"""
        pass
