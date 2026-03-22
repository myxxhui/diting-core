# [Ref: 09_核心模块架构规约 Module C VC-Agent] [Ref: 03_双轨制与VC-Agent]
# B 轨信仰专家占位：输出 horizon=LONG_TERM，供判官双轨分流（豁免 2% 硬止损与现金拖累）

import logging
from typing import Any, Dict, Optional

from diting.protocols.brain_pb2 import ExpertOpinion, TIME_HORIZON_LONG_TERM

logger = logging.getLogger(__name__)

SIGNAL_BULLISH = 1


def vc_agent_opinion(
    symbol: str,
    quant_signal: Optional[Dict[str, Any]] = None,
    enable_long_term: bool = True,
) -> ExpertOpinion:
    """
    VC-Agent 占位：对标的给出长期视野意见；当前占位固定返回 LONG_TERM、is_supported=True、BULLISH。
    后续可接入基本面数据（财报、营收增速、研发占比）做真实判定。
    """
    return ExpertOpinion(
        symbol=symbol,
        domain=0,
        is_supported=True,
        direction=SIGNAL_BULLISH,
        confidence=0.7,
        reasoning_summary="VC-Agent 占位：长期价值候选，待接入基本面与逻辑证伪",
        risk_factors=[],
        timestamp=0,
        horizon=TIME_HORIZON_LONG_TERM if enable_long_term else 1,  # noqa: short=1
    )
