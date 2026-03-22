# [Ref: 02_B模块策略_策略实现规约] [Ref: 11_数据采集与输入层规约] 可选：按 Module A 输出过滤扫描标的
# match_mode：exact_primary（仅 primary_tag 字符串）| domain_or_primary（领域桶与 primary 任一命中）

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


def resolve_scanner_classifier_batch_id(config_batch_id: Optional[str]) -> Optional[str]:
    """B 扫描门控：SCANNER_CLASSIFIER_BATCH_ID 覆盖 scanner_rules 中的 classifier_gate.batch_id。"""
    v = (os.environ.get("SCANNER_CLASSIFIER_BATCH_ID") or "").strip()
    if v:
        return v
    return (config_batch_id or "").strip() or None


# 与 l2_snapshot_writer._DOMAIN_TAG_TO_STR、classifier_pb2.DomainTag 一致
_DOMAIN_INT_TO_BUCKET = {
    0: "未指定",
    1: "农业",
    2: "科技",
    3: "宏观",
    4: "未知",
    5: "自定义",
}


def _parse_jsonb(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (list, dict)):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return None
    return None


def _first_domain_bucket(tags_json: Any) -> Optional[str]:
    """tags_json 首条 domain_tag -> 门控用领域桶（自定义统一为「自定义」）。"""
    arr = _parse_jsonb(tags_json)
    if not isinstance(arr, list) or not arr:
        return None
    first = arr[0]
    if not isinstance(first, dict):
        return None
    dt = int(first.get("domain_tag", 4))
    return _DOMAIN_INT_TO_BUCKET.get(dt)


def fetch_symbol_classifier_rows(
    symbols: List[str],
    dsn: Optional[str],
    batch_id: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    L2 classifier_output_snapshot：symbol -> {primary_tag, tags_json, batch_id}。
    batch_id 非空时仅该批；否则每 symbol 取 created_at 最新一条。
    """
    if not symbols:
        return {}
    dsn = (dsn or os.environ.get("PG_L2_DSN", "") or os.environ.get("TIMESCALE_DSN", "") or "").strip()
    if not dsn:
        return {}
    uniq = sorted({str(s).strip().upper() for s in symbols if s and str(s).strip()})
    if not uniq:
        return {}
    try:
        import psycopg2
    except ImportError:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    bid = (batch_id or "").strip() or None
    try:
        conn = psycopg2.connect(dsn, connect_timeout=15)
        try:
            cur = conn.cursor()
            if bid:
                cur.execute(
                    """
                    SELECT DISTINCT ON (symbol) symbol, primary_tag, tags_json, batch_id
                    FROM classifier_output_snapshot
                    WHERE symbol = ANY(%s) AND batch_id = %s
                    ORDER BY symbol, created_at DESC
                    """,
                    (uniq, bid),
                )
            else:
                cur.execute(
                    """
                    SELECT DISTINCT ON (symbol) symbol, primary_tag, tags_json, batch_id
                    FROM classifier_output_snapshot
                    WHERE symbol = ANY(%s)
                    ORDER BY symbol, created_at DESC
                    """,
                    (uniq,),
                )
            for row in cur.fetchall():
                sym = str(row[0]).strip().upper()
                out[sym] = {
                    "primary_tag": str(row[1] or "").strip(),
                    "tags_json": row[2],
                    "batch_id": str(row[3] or "").strip(),
                }
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("classifier_output_snapshot 读取失败: %s", e)
        return {}
    return out


def matches_classifier_allowed(
    primary_tag: str,
    tags_json: Any,
    allowed_tags: List[str],
    match_mode: str = "domain_or_primary",
) -> bool:
    """单标的判定（供单测与编排层复用）；allowed_tags 非空。"""
    allow = {str(t).strip() for t in allowed_tags if t and str(t).strip()}
    if not allow:
        return True
    mode = (match_mode or "domain_or_primary").strip().lower()
    if mode in ("legacy", "exact", "exact_primary"):
        mode = "exact_primary"
    else:
        mode = "domain_or_primary"
    return _symbol_passes_allowed(primary_tag, tags_json, allow, mode)


def _symbol_passes_allowed(
    primary_tag: str,
    tags_json: Any,
    allow: Set[str],
    match_mode: str,
) -> bool:
    pt = (primary_tag or "").strip()
    if match_mode in ("legacy", "exact_primary"):
        return pt in allow
    # domain_or_primary
    if pt in allow:
        return True
    bucket = _first_domain_bucket(tags_json)
    if bucket and bucket in allow:
        return True
    return False


def allowed_symbols_by_classifier(
    symbols: List[str],
    dsn: Optional[str],
    allowed_tags: Optional[List[str]],
    *,
    match_mode: str = "domain_or_primary",
    batch_id: Optional[str] = None,
) -> Set[str]:
    """
    若 allowed_tags 非空：按 match_mode 过滤；无 A 数据的标的 **剔除**（严格门控）。
    allowed_tags 为空或 None：返回全部 symbol 集合（全放行）。

    :param match_mode: exact_primary（仅 primary_tag 精确匹配）| domain_or_primary（primary 或领域桶命中）
    :param batch_id: 非空时只读该 batch_id 的快照（与 A 同批对齐）；None 表示每 symbol 最新一条
    """
    base = {str(s).strip().upper() for s in symbols if s and str(s).strip()}
    if not allowed_tags:
        return base
    allow = {str(t).strip() for t in allowed_tags if t and str(t).strip()}
    if not allow:
        return base

    mode = (match_mode or "domain_or_primary").strip().lower()
    if mode in ("legacy", "exact", "exact_primary"):
        mode = "exact_primary"
    elif mode in ("domain", "domain_or_primary", "auto"):
        mode = "domain_or_primary"
    else:
        mode = "domain_or_primary"

    rows = fetch_symbol_classifier_rows(list(base), dsn, batch_id=batch_id)
    ok: Set[str] = set()
    for sym in base:
        row = rows.get(sym)
        if not row:
            continue
        if _symbol_passes_allowed(row.get("primary_tag", ""), row.get("tags_json"), allow, mode):
            ok.add(sym)
    return ok


def fetch_symbol_primary_tags(symbols: List[str], dsn: Optional[str]) -> Dict[str, str]:
    """兼容旧 API：symbol -> primary_tag（最新一条，不按 batch）。"""
    rows = fetch_symbol_classifier_rows(symbols, dsn, batch_id=None)
    return {k: v.get("primary_tag", "") for k, v in rows.items()}
