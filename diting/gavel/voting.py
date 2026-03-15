# [Ref: 09_核心模块架构规约 Module D] 判官投票 + 双轨分流（LONG_TERM 豁免 2% 与现金拖累）

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# expert.proto TimeHorizon
TIME_HORIZON_LONG_TERM = 2

# 协议 Action 与 verdict.proto CouncilVerdict 对齐
ACTION_PASS = 0
ACTION_BUY = 1
ACTION_SELL = 2
ACTION_CUT = 3


@dataclass
class Verdict:
    """判官裁决；B 轨时 apply_2pct_stop=False, apply_cash_drag=False。"""
    action: int
    symbol: str
    win_rate_prediction: float
    suggested_position_ratio: float
    primary_reasoning: str
    is_defensive_mode: bool = False
    # 双轨分流 [Ref: 03_双轨制与VC-Agent]
    apply_2pct_stop: bool = True
    apply_cash_drag: bool = True
    track: str = "A"  # "A" 现金奶牛 | "B" 长期捕手


def vote(
    quant_signal: Dict[str, Any],
    expert_opinions: List[Any],
    technical_threshold: float = 70.0,
) -> Verdict:
    """
    投票：Quant Pass + 至少一名专家支持且 BULLISH => 有效信号。
    若任一专家 horizon == LONG_TERM，走 B 轨分支：豁免 2% 硬止损与现金拖累。
    """
    symbol = quant_signal.get("symbol", "")
    score = float(quant_signal.get("technical_score", 0))
    quant_vote = score >= technical_threshold

    expert_votes = []
    is_long_term = False
    for op in expert_opinions or []:
        horizon = getattr(op, "horizon", 0) or 0
        if int(horizon) == TIME_HORIZON_LONG_TERM:
            is_long_term = True
        if getattr(op, "is_supported", False) and getattr(op, "direction", 0) == 1:
            expert_votes.append(op)

    expert_ok = len(expert_votes) > 0
    if quant_vote and expert_ok:
        win_rate = 0.7
        reasoning = getattr(expert_votes[0], "reasoning_summary", "") or "Quant + Expert 通过"
        if is_long_term:
            return Verdict(
                action=ACTION_BUY,
                symbol=symbol,
                win_rate_prediction=win_rate,
                suggested_position_ratio=0.0,
                primary_reasoning=reasoning,
                is_defensive_mode=False,
                apply_2pct_stop=False,
                apply_cash_drag=False,
                track="B",
            )
        return Verdict(
            action=ACTION_BUY,
            symbol=symbol,
            win_rate_prediction=win_rate,
            suggested_position_ratio=0.0,
            primary_reasoning=reasoning,
            is_defensive_mode=False,
            apply_2pct_stop=True,
            apply_cash_drag=True,
            track="A",
        )
    return Verdict(
        action=ACTION_PASS,
        symbol=symbol,
        win_rate_prediction=0.0,
        suggested_position_ratio=0.0,
        primary_reasoning="Quant 或 Expert 未通过",
        apply_2pct_stop=True,
        apply_cash_drag=True,
        track="A",
    )
