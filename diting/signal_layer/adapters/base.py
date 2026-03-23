# [Ref: 06_B轨_信号层生产级数据采集_设计] 信号适配器基类

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class BaseSignalAdapter(ABC):
    """生产级信号适配器：按 segment_id 从真实数据源拉取原始摘要。禁止返回 Mock 数据。"""

    @abstractmethod
    def fetch_raw(self, segment_id: str, context: Dict[str, Any]) -> Optional[str]:
        """
        拉取该 segment 的原始摘要/文本。
        :param segment_id: 细分标识
        :param context: 如 symbols/symbol, name_cn, domain 等
        :return: 原始文本；无数据或失败返回 None
        """
        pass
