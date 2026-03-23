# [Ref: 06_B轨_信号层生产级数据采集_设计] 返回结构定义

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class RefreshSegmentSignalsResult:
    """refresh_segment_signals_for_symbols 返回值；便于排查缺数据与运维。"""

    symbols_without_segments: List[str] = field(default_factory=list)
    segments_without_adapter: List[str] = field(default_factory=list)
    segments_skipped_ttl: List[str] = field(default_factory=list)
    segments_written: List[str] = field(default_factory=list)
    segments_failed: Dict[str, str] = field(default_factory=dict)
    summary: Dict[str, int] = field(default_factory=dict)
