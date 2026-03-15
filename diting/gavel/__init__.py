# [Ref: 09_核心模块架构规约 Module D] 判官：投票 + 双轨分流（TimeHorizon）
from diting.gavel.voting import vote, Verdict, TIME_HORIZON_LONG_TERM
from diting.gavel.b_track_rules import check_logic_disproof_stop, check_major_trend_reversal

__all__ = [
    "vote",
    "Verdict",
    "TIME_HORIZON_LONG_TERM",
    "check_logic_disproof_stop",
    "check_major_trend_reversal",
]
