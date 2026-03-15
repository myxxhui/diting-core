# [Ref: 04_A轨_MoE议会_设计#design-stage3-04-strategy] 细分信号解析约定
# 规范 JSON：type, direction, strength, summary_cn, risk_tags；纯文本则关键词回退

import json
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

# 默认关键词与 DNA/config 一致
DEFAULT_KEYWORDS_BULLISH = ["利好", "上涨", "支持", "政策"]
DEFAULT_KEYWORDS_BEARISH = ["利空", "下跌", "风险"]


def parse_segment_signal(
    signal_summary: str,
    keywords_bullish: list = None,
    keywords_bearish: list = None,
) -> Dict[str, Any]:
    """
    解析细分信号：JSON 则取 type/direction/strength/summary_cn/risk_tags；
    否则按纯文本关键词判断 direction，strength 默认 0.5。
    :return: dict 至少含 direction (bullish|bearish|neutral), strength (0~1), risk_tags (list)
    """
    keywords_bullish = keywords_bullish or DEFAULT_KEYWORDS_BULLISH
    keywords_bearish = keywords_bearish or DEFAULT_KEYWORDS_BEARISH
    out = {"direction": "neutral", "strength": 0.5, "risk_tags": [], "type": "", "summary_cn": ""}

    if not (signal_summary and isinstance(signal_summary, str)):
        return out

    s = signal_summary.strip()
    # 尝试 JSON
    if s.startswith("{") and "}" in s:
        try:
            data = json.loads(s)
            if isinstance(data, dict):
                out["type"] = str(data.get("type") or "")
                out["summary_cn"] = str(data.get("summary_cn") or "")
                dr = (data.get("direction") or "").lower()
                if dr in ("bullish", "bearish", "neutral"):
                    out["direction"] = dr
                else:
                    _apply_fallback_direction(out, s, keywords_bullish, keywords_bearish)
                strength = data.get("strength")
                if isinstance(strength, (int, float)) and 0 <= strength <= 1:
                    out["strength"] = float(strength)
                risk = data.get("risk_tags")
                if isinstance(risk, list):
                    out["risk_tags"] = [str(x) for x in risk]
                elif isinstance(risk, str):
                    out["risk_tags"] = [risk]
                return out
        except (json.JSONDecodeError, TypeError):
            pass

    _apply_fallback_direction(out, s, keywords_bullish, keywords_bearish)
    return out


def _apply_fallback_direction(
    out: Dict[str, Any],
    text: str,
    keywords_bullish: list,
    keywords_bearish: list,
) -> None:
    for k in keywords_bearish:
        if k in text:
            out["direction"] = "bearish"
            return
    for k in keywords_bullish:
        if k in text:
            out["direction"] = "bullish"
            return
    out["direction"] = "neutral"
