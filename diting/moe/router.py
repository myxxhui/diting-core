# [Ref: 04_A轨_MoE议会_设计] 按股配置 + 统一分析；supported_tags 决定走统一入口或一条「不支持」

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from diting.protocols.brain_pb2 import ExpertOpinion

from diting.protocols.brain_pb2 import TIME_HORIZON_MEDIUM_TERM, TIME_HORIZON_SHORT_TERM

from diting.moe.experts import unified_opinion, trash_bin_opinion
from diting.moe.vc_agent import vc_agent_opinion

logger = logging.getLogger(__name__)

_CONFIG_CACHE: Optional[Dict[str, Any]] = None


def _load_moe_config() -> Dict[str, Any]:
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE
    root = Path(os.environ.get("DITING_CORE_ROOT", "."))
    if not root.is_absolute():
        root = Path.cwd() / root
    for p in [root / "config" / "moe_router.yaml", Path.cwd() / "config" / "moe_router.yaml"]:
        if p.exists():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    _CONFIG_CACHE = yaml.safe_load(f) or {}
                    return _CONFIG_CACHE
            except Exception as e:
                logger.warning("加载 moe_router.yaml 失败: %s", e)
    _CONFIG_CACHE = {}
    return _CONFIG_CACHE


def resolve_router_domain_tag(
    domain_tags: Optional[List[str]],
    config: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """
    与 route_and_collect_opinions 短轨入口一致：返回进入 unified_opinion 的 domain_tag（农业/科技/宏观）；
    None 表示无可用映射（将产生垃圾箱意见）。供 CLI/报表展示，避免与 Router 逻辑分叉。
    """
    cfg = config if config is not None else _load_moe_config()
    moe = cfg.get("moe_router") or cfg
    supported = moe.get("supported_tags") or ["农业", "科技", "宏观"]
    tag_to_router = moe.get("tag_to_router_domain") or {}
    if not isinstance(tag_to_router, dict):
        tag_to_router = {}
    _raw_um = moe.get("unmapped_router_domain")
    unmapped: Optional[str] = None
    if isinstance(_raw_um, str) and _raw_um.strip():
        unmapped = _raw_um.strip()

    first_supported_tag: Optional[str] = None
    for tag in domain_tags or []:
        t = (tag if isinstance(tag, str) else "").strip()
        if not t:
            continue
        if t in supported:
            first_supported_tag = t
            break
        mapped = tag_to_router.get(t)
        if isinstance(mapped, str):
            m = mapped.strip()
            if m in supported:
                first_supported_tag = m
                break

    if first_supported_tag is None and unmapped and unmapped in supported:
        first_supported_tag = unmapped

    return first_supported_tag


def route_and_collect_opinions(
    symbol: str,
    quant_signal: Optional[Dict[str, Any]] = None,
    domain_tags: Optional[List[str]] = None,
    segment_list: Optional[List[Dict[str, Any]]] = None,
    segment_signals: Optional[Dict[str, Any]] = None,
    enable_vc_agent: bool = True,
    config: Optional[Dict[str, Any]] = None,
    track: str = "a",
) -> List[ExpertOpinion]:
    """
    按股配置 + 统一分析：每标的一条意见。
    - track=b 时 unified_opinion 输出 horizon=MEDIUM_TERM；track=a 时 SHORT_TERM。
    - 若 enable_vc_agent 且 quant_signal 为 long_term_candidate，可追加 VC-Agent 意见（LONG_TERM）。
    - 短轨：domain_tags 中首个命中 supported_tags 的标签走 unified_opinion；否则查 tag_to_router_domain。
    """
    opinions: List[ExpertOpinion] = []
    quant_signal = quant_signal or {}
    segment_list = segment_list or []
    segment_signals = segment_signals or {}
    cfg = config if config is not None else _load_moe_config()
    horizon = TIME_HORIZON_MEDIUM_TERM if str(track or "").strip().lower() == "b" else TIME_HORIZON_SHORT_TERM

    if enable_vc_agent and quant_signal.get("long_term_candidate") and str(track or "").strip().lower() != "b":
        try:
            op = vc_agent_opinion(symbol, quant_signal=quant_signal, enable_long_term=True)
            opinions.append(op)
            logger.debug("Router: %s -> VC-Agent (LONG_TERM)", symbol)
        except Exception as e:
            logger.warning("VC-Agent 占位调用异常: %s", e)

    first_supported_tag = resolve_router_domain_tag(domain_tags, cfg)

    if first_supported_tag is None:
        opinions.append(trash_bin_opinion(symbol, reason="无法归类或未映射标签"))
        return opinions

    op = unified_opinion(
        symbol, quant_signal, segment_list, segment_signals, cfg, domain_tag=first_supported_tag,
        horizon=horizon,
    )
    opinions.append(op)
    return opinions
