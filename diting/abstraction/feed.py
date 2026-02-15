# 数据源抽象 [Ref: 05_接口抽象层规约 C]
from abc import ABC, abstractmethod
from typing import Any, List


class MarketDataFeed(ABC):
    """
    行情数据源标准接口 [Ref: 05_接口抽象层规约]。
    屏蔽底层 SQL, CSV, API 差异。
    """

    @abstractmethod
    def get_history(self, symbol: str, period: str, limit: int) -> Any:
        """
        获取历史 K 线
        :param symbol: 标的代码
        :param period: 周期 (1d, 1h, 1m)
        :param limit: 条数限制
        :return: DataFrame [datetime, open, high, low, close, volume]
        """
        pass

    @abstractmethod
    def get_snapshot(self, symbols: List[str]) -> Any:
        """
        获取最新一笔 Tick 快照
        :param symbols: 标的代码列表
        :return: DataFrame [symbol, price, volume, timestamp]
        """
        pass
