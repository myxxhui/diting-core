# [Ref: 04_A轨_MoE议会_设计] Module C 专家意见写入 L2，供 query-module-c-output 与判官联调

import json
import logging
import uuid
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def expert_opinion_to_dict(op: Any) -> dict:
    """brain_pb2.ExpertOpinion → 可 JSON 序列化的 dict。"""
    return {
        "symbol": getattr(op, "symbol", "") or "",
        "domain": int(getattr(op, "domain", 0) or 0),
        "is_supported": bool(getattr(op, "is_supported", False)),
        "direction": int(getattr(op, "direction", 0) or 0),
        "confidence": float(getattr(op, "confidence", 0.0) or 0.0),
        "reasoning_summary": getattr(op, "reasoning_summary", "") or "",
        "risk_factors": list(getattr(op, "risk_factors", None) or []),
        "timestamp": int(getattr(op, "timestamp", 0) or 0),
        "horizon": int(getattr(op, "horizon", 0) or 0),
    }


def write_moe_expert_opinion_snapshot(
    dsn: str,
    rows: List[tuple],
    batch_id: str = "",
    correlation_id: str = "",
    run_metadata: Optional[Dict[str, Any]] = None,
) -> int:
    """
    写入 L2 表 moe_expert_opinion_snapshot。
    :param rows: [(symbol, List[ExpertOpinion]), ...]
    :param run_metadata: 单行写入元数据（stub、batch 对齐、pipeline 等）；每行相同，便于下游按批过滤。
    """
    if not rows or not dsn:
        return 0
    try:
        import psycopg2
    except ImportError:
        logger.warning("psycopg2 未安装，跳过写入 moe_expert_opinion_snapshot")
        return 0
    batch_id = batch_id or str(uuid.uuid4())
    correlation_id = correlation_id or batch_id
    meta_json = json.dumps(run_metadata or {}, ensure_ascii=False)
    sql_rows = []
    for sym, opinions in rows:
        payload = [expert_opinion_to_dict(o) for o in (opinions or [])]
        sql_rows.append(
            (
                batch_id,
                str(sym).strip().upper(),
                json.dumps(payload, ensure_ascii=False),
                correlation_id,
                meta_json,
            )
        )
    try:
        conn = psycopg2.connect(dsn)
        try:
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT INTO moe_expert_opinion_snapshot
                (batch_id, symbol, opinions_json, correlation_id, moe_run_metadata)
                VALUES (%s, %s, %s::jsonb, %s, %s::jsonb)
                """,
                sql_rows,
            )
            conn.commit()
            n = len(sql_rows)
            logger.info(
                "moe_expert_opinion_snapshot 写入 batch_id=%s 行数=%s", batch_id, n
            )
            return n
        finally:
            conn.close()
    except Exception as e:
        logger.warning("写入 moe_expert_opinion_snapshot 失败: %s", e)
        return 0
