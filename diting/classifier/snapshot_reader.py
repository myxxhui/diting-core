# [Ref: 12_右脑数据支撑与Segment规约] [Ref: 01_语义分类器_实践]
# 从 L2 classifier_output_snapshot 读取 tags/segment_shares，供 B 门控与 C 独立部署（不重跑 A）

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

from diting.protocols.classifier_pb2 import DomainTag

logger = logging.getLogger(__name__)


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


def domain_tags_zh_from_tags_json(tags_json: Any) -> List[str]:
    """与 scripts/run_module_c_local._domain_tags_zh 语义一致，输入为 DB tags_json。"""
    arr = _parse_jsonb(tags_json)
    if not isinstance(arr, list) or not arr:
        return ["未知"]
    names: List[str] = []
    for t in arr:
        if not isinstance(t, dict):
            continue
        dt = int(t.get("domain_tag", 0))
        label = (t.get("domain_label") or "").strip()
        if dt == DomainTag.DOMAIN_CUSTOM and label:
            names.append(label[:64])
        elif dt == DomainTag.AGRI:
            names.append("农业")
        elif dt == DomainTag.TECH:
            names.append("科技")
        elif dt == DomainTag.GEO:
            names.append("宏观")
        elif dt == DomainTag.UNKNOWN:
            names.append("未知")
        else:
            names.append("未知")
    return names if names else ["未知"]


def segment_list_from_segment_shares_json(segment_shares_json: Any) -> List[Dict[str, Any]]:
    """将快照列 segment_shares_json 转为 MoE 用的 segment_list。"""
    arr = _parse_jsonb(segment_shares_json)
    if not isinstance(arr, list):
        return []
    out: List[Dict[str, Any]] = []
    for row in arr:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "segment_id": str(row.get("segment_id") or ""),
                "revenue_share": float(row.get("revenue_share") or 0),
                "is_primary": bool(row.get("is_primary", False)),
            }
        )
    return out


def fetch_snapshot_rows_batch(
    dsn: str,
    symbols: List[str],
    batch_id: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    读取 classifier_output_snapshot：symbol.upper() -> {primary_tag, tags_json, segment_shares_json, batch_id, created_at}
    batch_id 为 None 时每 symbol 取 created_at 最新一条；否则仅该 batch_id。
    """
    if not dsn or not symbols:
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
                    SELECT DISTINCT ON (symbol) symbol, primary_tag, tags_json, segment_shares_json,
                           batch_id, created_at
                    FROM classifier_output_snapshot
                    WHERE symbol = ANY(%s) AND batch_id = %s
                    ORDER BY symbol, created_at DESC
                    """,
                    (uniq, bid),
                )
            else:
                cur.execute(
                    """
                    SELECT DISTINCT ON (symbol) symbol, primary_tag, tags_json, segment_shares_json,
                           batch_id, created_at
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
                    "segment_shares_json": row[3],
                    "batch_id": str(row[4] or "").strip(),
                    "created_at": row[5],
                }
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("fetch_snapshot_rows_batch: %s", e)
        return {}
    return out


def resolve_moe_classifier_batch_id(config_batch_id: Optional[str]) -> Optional[str]:
    """Module C 读快照：MOE_CLASSIFIER_BATCH_ID 覆盖配置中的 batch_id。"""
    v = (os.environ.get("MOE_CLASSIFIER_BATCH_ID") or "").strip()
    if v:
        return v
    return (config_batch_id or "").strip() or None


def fetch_latest_classifier_batch_id(dsn: str) -> Optional[str]:
    """
    L2 classifier_output_snapshot 中，按 MAX(created_at) 取最近一次 A 写入的 batch_id。
    表空或失败时返回 None。
    """
    if not dsn:
        return None
    try:
        import psycopg2
    except ImportError:
        return None
    try:
        conn = psycopg2.connect(dsn, connect_timeout=15)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT batch_id
                FROM classifier_output_snapshot
                WHERE batch_id IS NOT NULL AND batch_id <> ''
                GROUP BY batch_id
                ORDER BY MAX(created_at) DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            cur.close()
            if row and row[0]:
                return str(row[0]).strip() or None
        finally:
            conn.close()
    except Exception as e:
        logger.warning("fetch_latest_classifier_batch_id: %s", e)
    return None
