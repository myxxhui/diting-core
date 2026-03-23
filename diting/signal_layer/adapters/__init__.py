# [Ref: 06_B轨_信号层生产级数据采集_设计] 生产级信号适配器

from typing import Optional

from diting.signal_layer.adapters.base import BaseSignalAdapter
from diting.signal_layer.adapters.seg_bp_news_adapter import SegBpNewsAdapter

__all__ = ["BaseSignalAdapter", "SegBpNewsAdapter", "get_adapter_for_segment"]

_ADAPTERS = {
    "seg_bp_news": SegBpNewsAdapter(),
}


def get_adapter_for_segment(segment_id: str, config: dict) -> Optional[BaseSignalAdapter]:
    """按 segment_id 选适配器。seg_bp_* 默认 seg_bp_news。"""
    adapter_map = config.get("adapter_by_prefix") or {}
    for prefix, name in adapter_map.items():
        if segment_id.startswith(prefix):
            return _ADAPTERS.get(name) or _ADAPTERS["seg_bp_news"]
    if segment_id.startswith("seg_bp_"):
        return _ADAPTERS["seg_bp_news"]
    return None
