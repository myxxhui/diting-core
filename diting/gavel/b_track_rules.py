# [Ref: 03_双轨制与VC-Agent] B 轨仍施加：逻辑证伪止损、大周期反转止盈（占位接口，待 L3 细化后实现）

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def check_logic_disproof_stop(symbol: str, context: Optional[Dict[str, Any]] = None) -> bool:
    """
    逻辑证伪止损：产业趋势或基本面逻辑被证伪时返回 True，判官生成平仓指令。
    当前占位：始终返回 False。后续输入：基本面事件、政策/技术路线颠覆、VC-Agent 或独立模块判定。
    """
    return False


def check_major_trend_reversal(symbol: str, context: Optional[Dict[str, Any]] = None) -> bool:
    """
    大周期反转止盈：标的或行业进入长期下行周期时返回 True，判官生成止盈/减仓信号。
    当前占位：始终返回 False。后续输入：长期均线死叉、行业指数跌破关键支撑、波动率 regime 等。
    """
    return False
