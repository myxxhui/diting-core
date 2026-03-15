# MoE 议会 [Ref: 04_A轨_MoE议会_设计] 按股配置 + 统一分析
from diting.moe.router import route_and_collect_opinions
from diting.moe.vc_agent import vc_agent_opinion, TIME_HORIZON_LONG_TERM
from diting.moe.signal_parse import parse_segment_signal
from diting.moe.alignment import compute_alignment_and_aggregate, should_reject_by_cognitive_boundary
from diting.moe.experts import unified_opinion, trash_bin_opinion

__all__ = [
    "route_and_collect_opinions",
    "vc_agent_opinion",
    "TIME_HORIZON_LONG_TERM",
    "parse_segment_signal",
    "compute_alignment_and_aggregate",
    "should_reject_by_cognitive_boundary",
    "unified_opinion",
    "trash_bin_opinion",
]
