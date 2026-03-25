# [Ref: 06_B轨_信号层生产级数据采集_设计] 信号理解：仅大模型打标与摘要（固定 JSON schema），无关键词规则路径。

import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

# schema: type, direction, strength, summary_cn, risk_tags
TYPE_ENUM = {"policy", "price", "order", "rnd"}
DIRECTION_ENUM = {"bullish", "bearish", "neutral"}
SCHEMA_INSTRUCTION = '{"type":"policy|price|order|rnd","direction":"bullish|bearish|neutral","strength":0.0~1.0,"summary_cn":"中文摘要","risk_tags":[]}'


def _load_prompt_template(prompt_path: Optional[str], root: Optional[Path] = None) -> str:
    """从 prompt_path 加载模板；不存在则返回内置默认。"""
    if not prompt_path or not prompt_path.strip():
        return (
            "对以下与细分领域相关的一手信息做利好/利空/中性判断。"
            "仅输出 JSON：{schema}。禁止预测价格、输出买卖建议。\n\n{{raw_text}}"
        ).replace("{schema}", SCHEMA_INSTRUCTION)
    root = root or Path(__file__).resolve().parents[3]
    path = Path(prompt_path)
    if not path.is_absolute():
        path = root / path
    if path.exists():
        try:
            return path.read_text(encoding="utf-8")
        except Exception as e:
            logger.warning("加载 prompt 失败 %s: %s", path, e)
    return (
        "对以下与细分领域相关的一手信息做利好/利空/中性判断。"
        "仅输出 JSON：{schema}。禁止预测价格、输出买卖建议。\n\n{{raw_text}}"
    ).replace("{schema}", SCHEMA_INSTRUCTION)


def _render_prompt(template: str, segment_id: str, raw_text: str) -> str:
    """渲染 prompt 模板占位符。"""
    return template.replace("{{segment_id}}", segment_id).replace(
        "{{raw_text}}", raw_text
    ).replace("{{schema_instruction}}", SCHEMA_INSTRUCTION)


def is_llm_configured(config: Optional[Dict[str, Any]]) -> bool:
    """是否已配置可调用的大模型（与 _ai_tag 门槛一致：api_key + model_id/provider 均非空）。"""
    if not config:
        return False
    api_key = (config.get("api_key") or "").strip()
    model_id = (config.get("model_id") or config.get("provider") or "").strip()
    return bool(api_key and model_id)


def _validate_schema(d: Dict[str, Any]) -> bool:
    """校验产出符合固定 schema。"""
    if not d or not isinstance(d, dict):
        return False
    if d.get("type") not in TYPE_ENUM:
        d["type"] = "policy"
    if d.get("direction") not in DIRECTION_ENUM:
        return False
    s = d.get("strength")
    if s is None or not isinstance(s, (int, float)):
        return False
    if not (0.0 <= float(s) <= 1.0):
        return False
    if not isinstance(d.get("summary_cn"), str) or not d["summary_cn"].strip():
        return False
    if not isinstance(d.get("risk_tags"), list):
        d["risk_tags"] = []
    return True


def _ai_tag(
    raw_text: str,
    segment_id: str,
    config: Dict[str, Any],
    audit_callback: Optional[Callable[[str, str, Optional[str], Optional[str]], None]] = None,
) -> Optional[Dict[str, Any]]:
    """大模型打标。需 model_id + api_key；返回合法 schema 或 None。"""
    model_id = (config.get("model_id") or config.get("provider") or "").strip()
    if not model_id:
        return None
    api_key = (config.get("api_key") or "").strip()
    if not api_key:
        return None
    base_url = (config.get("base_url") or "").strip() or None
    max_output_tokens = int(config.get("max_output_tokens") or 256)
    retry_times = max(0, int(config.get("retry_times") or 1))
    retry_backoff_sec = max(0, float(config.get("retry_backoff_sec") or 2))
    timeout_sec = max(5, int(config.get("timeout_sec") or 30))
    prompt_path = config.get("prompt_path") or ""
    root = Path(__file__).resolve().parents[3]
    template = _load_prompt_template(prompt_path, root)
    prompt = _render_prompt(template, segment_id, raw_text[:int(config.get("max_input_chars") or 4096)])
    last_err = None
    for attempt in range(retry_times + 1):
        try:
            import openai
            client = openai.OpenAI(api_key=api_key, base_url=base_url or None)
            r = client.chat.completions.create(
                model=model_id,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_output_tokens,
                timeout=timeout_sec,
            )
            content = (r.choices[0].message.content or "").strip()
            for m in re.finditer(r"\{[^{}]*\}", content):
                try:
                    d = json.loads(m.group())
                    if _validate_schema(d):
                        if isinstance(d, dict) and "signal_source" not in d:
                            d["signal_source"] = "llm"
                        if audit_callback:
                            audit_callback(segment_id, "ai", raw_text[:2048], json.dumps(d, ensure_ascii=False), None)
                        return d
                except json.JSONDecodeError:
                    continue
            last_err = "无合法 JSON"
        except ImportError:
            last_err = "openai 未安装"
            break
        except Exception as e:
            last_err = str(e)
            logger.warning("AI 信号理解失败 (attempt %d): %s", attempt + 1, e)
        if attempt < retry_times:
            time.sleep(retry_backoff_sec)
    if audit_callback and last_err:
        audit_callback(segment_id, "ai", raw_text[:2048], None, last_err)
    return None


def understand_signal(
    raw_text: str,
    segment_id: str,
    config: Optional[Dict[str, Any]] = None,
    audit_callback: Optional[Callable[[str, str, Optional[str], Optional[str]], None]] = None,
) -> Optional[Dict[str, Any]]:
    """
    对原始文本仅通过大模型做信号理解与摘要（固定 JSON schema）。
    未配置 api_key+model_id 时返回 None。
    配置项 signal_understanding.mode 保留兼容 YAML，但不再启用任何规则分支。
    """
    if not raw_text or len(str(raw_text).strip()) < 5:
        return None
    cfg = config or {}
    text = str(raw_text).strip()[:int(cfg.get("max_input_chars") or 4096)]
    if not is_llm_configured(cfg):
        return None
    result = _ai_tag(text, segment_id, cfg, audit_callback)
    if result and _validate_schema(result):
        return result
    return None
