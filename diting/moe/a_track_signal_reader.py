# [Ref: 06_A轨 信号层] 从 L2 a_track_signal_cache 读取标的级/行业级打标（Module C 合并）
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

_AT_INDUSTRY_SEG_ID = "a_track_industry"


def _parse(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, dict):
        d = raw
    elif isinstance(raw, str):
        try:
            d = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
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
        "source_scope": str(d.get("source_scope") or ""),
    }


def fetch_a_track_signals_for_symbol(
    pg_l2_dsn: str,
    symbol: str,
    industry_name: str,
) -> Dict[str, Optional[Dict[str, Any]]]:
    """
    读取该标的的 A 轨双路缓存：sym:SYMBOL、ind:行业名。
    :return: {"symbol": 解析后信号或 None, "industry": ...}
    """
    out: Dict[str, Optional[Dict[str, Any]]] = {"symbol": None, "industry": None}
    if not pg_l2_dsn:
        return out
    sym = (symbol or "").strip().upper()
    ind = (industry_name or "").strip()[:128]
    keys = [("sym:%s" % sym, "symbol")]
    if ind:
        keys.append(("ind:%s" % ind, "industry"))
    try:
        import psycopg2
        conn = psycopg2.connect(pg_l2_dsn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT cache_key, signal_summary FROM a_track_signal_cache
            WHERE cache_key = ANY(%s)
            """,
            ([k[0] for k in keys],),
        )
        rows = {str(r[0]): r[1] for r in (cur.fetchall() or [])}
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("a_track_signal_cache 读取失败: %s", e)
        return out
    for ck, k in keys:
        parsed = _parse(rows.get(ck))
        if parsed:
            out[k] = parsed
    return out


def merge_a_track_into_segment_signals(
    track: str,
    segment_list: list,
    segment_signals: Dict[str, Any],
    a_symbol: Optional[Dict[str, Any]],
    a_industry: Optional[Dict[str, Any]],
) -> Tuple[list, Dict[str, Any]]:
    """
    DITING_TRACK=a 时：若主营 segment 在 segment_signal_cache 中无条目，用 A 轨「标的新闻」结论填充 primary；
    并将「申万行业」信号挂到固定 segment_id=a_track_industry 供对齐与聚合。
    """
    if str(track or "").strip().lower() != "a":
        return segment_list, segment_signals
    sl = list(segment_list or [])
    ss = dict(segment_signals or {})
    primary = next((s for s in sl if s.get("is_primary")), None)
    pid = str(primary.get("segment_id") or "").strip() if primary else ""
    if pid and a_symbol and pid not in ss:
        ss[pid] = a_symbol
    if a_industry:
        if not any(str(s.get("segment_id") or "") == _AT_INDUSTRY_SEG_ID for s in sl):
            sl.append(
                {
                    "segment_id": _AT_INDUSTRY_SEG_ID,
                    "segment_label_cn": "申万行业新闻(A轨)",
                    "revenue_share": 0.05,
                    "is_primary": False,
                }
            )
        ss[_AT_INDUSTRY_SEG_ID] = a_industry
    return sl, ss
