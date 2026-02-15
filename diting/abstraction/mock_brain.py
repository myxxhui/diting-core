# 占位 Brain 实现 [Ref: 05_接口抽象层规约 B] 使「导入 + 最小调用」可运行
from typing import Any, Dict

from diting.abstraction.brain import CognitiveEngine


class MockBrain(CognitiveEngine):
    """Mock 实现，用于单元测试，不消耗 Token"""

    def reason(self, context_text: str, schema: dict) -> Dict[str, Any]:
        return {
            "is_supported": True,
            "confidence": 0.8,
            "reasoning_summary": "Mock reasoning",
        }

    def audit_thought_process(self, session_id: str) -> str:
        return f"Mock CoT for session {session_id}"
