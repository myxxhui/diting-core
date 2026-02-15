# 大脑认知抽象 [Ref: 05_接口抽象层规约 B]
# MoE Router 与 Sub-Agents 依赖此接口
from abc import ABC, abstractmethod
from typing import Any, Dict


class CognitiveEngine(ABC):
    """
    认知引擎的标准接口 [Ref: 05_接口抽象层规约]。
    MoE 的 Router 和 Sub-Agents 依赖此接口，而不是具体的 HTTP Client。
    """

    @abstractmethod
    def reason(self, context_text: str, schema: dict) -> Dict[str, Any]:
        """
        通用推理接口
        :param context_text: 新闻/公告/行情摘要
        :param schema: 期望输出的 JSON Schema
        :return: 符合 Schema 的结构化数据
        """
        pass

    @abstractmethod
    def audit_thought_process(self, session_id: str) -> str:
        """
        获取思维链 (CoT) 日志，用于审计
        :param session_id: 推理会话 ID
        :return: CoT 日志文本
        """
        pass
