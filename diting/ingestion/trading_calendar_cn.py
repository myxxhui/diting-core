# [Ref: diting.ingestion] A 股交易日口径（与采集脚本增量「是否过期」一致）
"""中国 A 股交易日：用于 K 线增量判定（相对自然日/周末误判）。

数据源：AkShare ``tool_trade_date_hist_sina()``（与行情源一致的交易日序列），进程内缓存。
若 AkShare 不可用则回退为「仅剔除周末」，长假可能偏差。"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from functools import lru_cache
from typing import FrozenSet

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _cn_trade_dates() -> FrozenSet[date]:
    import akshare as ak
    import pandas as pd

    df = ak.tool_trade_date_hist_sina()
    return frozenset(pd.to_datetime(df["trade_date"]).dt.date)


def as_of_trading_session_eod(today: date) -> date:
    """不大于 *today* 的最后一个交易日（日 K 口径：截至该日收盘应有数据）。"""
    try:
        td = _cn_trade_dates()
        x = today
        for _ in range(400):
            if x in td:
                return x
            x -= timedelta(days=1)
        logger.warning(
            "as_of_trading_session_eod: 回溯 400 天未命中交易日历，使用仅周末规则 (today=%s)",
            today,
        )
    except Exception as e:
        logger.warning("AkShare 交易日历不可用，使用仅周末规则: %s", e)
    return _as_of_weekday_only(today)


def _as_of_weekday_only(today: date) -> date:
    x = today
    for _ in range(14):
        if x.weekday() < 5:
            return x
        x -= timedelta(days=1)
    return today


def trading_sessions_gap_after(latest: date, as_of: date) -> int:
    """严格在 *latest* 之后、且不超过 *as_of* 的交易日个数，即 (latest, as_of] 上的交易日数。

    *as_of* 宜为 `as_of_trading_session_eod(today)`，与「应补到哪一天」对齐。
    """
    if latest >= as_of:
        return 0
    try:
        td = _cn_trade_dates()
        gap = 0
        cur = latest + timedelta(days=1)
        while cur <= as_of:
            if cur in td:
                gap += 1
            cur += timedelta(days=1)
        return gap
    except Exception as e:
        logger.warning("交易日缺口计算失败，使用仅周末: %s", e)
        return _trading_gap_weekday_only(latest, as_of)


def _trading_gap_weekday_only(latest: date, as_of: date) -> int:
    if latest >= as_of:
        return 0
    gap = 0
    cur = latest + timedelta(days=1)
    while cur <= as_of:
        if cur.weekday() < 5:
            gap += 1
        cur += timedelta(days=1)
    return gap
