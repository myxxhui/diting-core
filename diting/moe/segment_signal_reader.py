# [Ref: 12_右脑数据支撑与Segment规约] 从 L2 segment_signal_cache 读取细分垂直信号
# 消费方：Module C；写入方：信号层 refresh_segment_signals_for_symbols

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def fetch_segment_signals_for_segments(
    pg_l2_dsn: str,
    segment_ids: List[str],
) -> Dict[str, Dict[str, Any]]:
    """
    从 L2 segment_signal_cache 按 segment_id 批量读取信号。
    :param pg_l2_dsn: L2 PostgreSQL DSN
    :param segment_ids: 待查询的 segment_id 列表
    :return: segment_id -> {direction, strength, type, summary_cn, risk_tags}；表不存在或查询失败返回 {}
    """
    if not pg_l2_dsn or not segment_ids:
        return {}
    segment_ids = [str(s).strip() for s in segment_ids if s]
    if not segment_ids:
        return {}
    try:
        import psycopg2
        conn = psycopg2.connect(pg_l2_dsn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT segment_id, signal_summary
            FROM segment_signal_cache
            WHERE segment_id = ANY(%s)
            """,
            (segment_ids,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("segment_signal_cache 读取失败（表可能未建）: %s", e)
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        seg_id = str(row[0] or "").strip()
        raw = row[1]
        if not seg_id:
            continue
        parsed = _parse_signal_summary(raw)
        if parsed:
            out[seg_id] = parsed
    return out


def _parse_signal_summary(raw: Any) -> Optional[Dict[str, Any]]:
    """解析 signal_summary：支持 JSON 或 JSON 字符串。"""
    if raw is None:
        return None
    if isinstance(raw, dict):
        d = raw
    elif isinstance(raw, str):
        try:
            d = json.loads(raw)
        except json.JSONDecodeError:
            return None
    else:
        return None
    direction = (d.get("direction") or "neutral").strip().lower()
    if direction in ("bullish", "1", "多"):
        direction = "bullish"
    elif direction in ("bearish", "-1", "空"):
        direction = "bearish"
    else:
        direction = "neutral"
    strength = d.get("strength")
    if strength is not None and isinstance(strength, (int, float)):
        strength = max(0.0, min(1.0, float(strength)))
    else:
        strength = 0.5
    return {
        "direction": direction,
        "strength": strength,
        "type": str(d.get("type") or ""),
        "summary_cn": str(d.get("summary_cn") or ""),
        "risk_tags": list(d.get("risk_tags") or []),
    }
