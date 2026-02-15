# 占位 Feed 实现 [Ref: 05_接口抽象层规约 C]
from typing import List

from diting.abstraction.feed import MarketDataFeed


class MockFeed(MarketDataFeed):
    """占位行情数据源，返回空结构，无业务逻辑"""

    def get_history(self, symbol: str, period: str, limit: int):  # noqa: ANN201
        # 骨架期不依赖 pandas，返回空 list；逻辑填充期可改为 pd.DataFrame
        return []

    def get_snapshot(self, symbols: List[str]):  # noqa: ANN201
        return []
