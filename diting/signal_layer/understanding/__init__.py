# [Ref: 06_B轨_信号层生产级数据采集_设计] 信号理解：仅大模型；未配置 api_key+model_id 时不打标

from diting.signal_layer.understanding.engine import is_llm_configured, understand_signal

__all__ = ["understand_signal", "is_llm_configured"]
