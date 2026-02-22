# [Ref: 03_原子目标与规约/_共享规约/05_接口抽象层规约] 大脑认知抽象
from abc import ABC, abstractmethod
from typing import Any, Dict

class CognitiveEngine(ABC):
    """认知引擎的标准接口。模型切换、Mock 测试。"""

    @abstractmethod
    def reason(self, context_text: str, schema: dict) -> dict:
        """通用推理接口。返回符合 Schema 的结构化数据。"""
        pass

    @abstractmethod
    def audit_thought_process(self, session_id: str) -> str:
        """获取思维链 (CoT) 日志，用于 LangFuse 审计。"""
        pass
