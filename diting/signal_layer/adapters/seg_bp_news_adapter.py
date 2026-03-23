# [Ref: 06_B轨_信号层生产级数据采集_设计] seg_bp_* 细分：仅从 L2 news_content 读取采集模块写入的标的新闻/公告；不调用 AkShare

import logging
from typing import Any, Dict, List, Optional

import psycopg2

logger = logging.getLogger(__name__)

_MAX_TEXT_CHARS = 4096


def _fetch_news_from_l2(conn, symbol: str, days_back: int = 7, max_chars: int = _MAX_TEXT_CHARS) -> str:
    """从 L2 news_content 读取该标的最近新闻，聚合为文本。"""
    cur = conn.cursor()
    try:
        days = max(1, min(90, int(days_back)))
        cur.execute(
            """
            SELECT title, content FROM news_content
            WHERE symbol = %s AND published_at >= NOW() - INTERVAL '1 day' * %s
            ORDER BY published_at DESC LIMIT 20
            """,
            (symbol.upper(), days),
        )
        rows = cur.fetchall()
    except Exception as e:
        logger.warning("SegBpNewsAdapter L2 news_content 查询失败 symbol=%s: %s", symbol, e)
        return ""
    finally:
        cur.close()
    parts = []
    total = 0
    for title, content in (rows or []):
        t = (title or "").strip()
        c = (content or "").strip()
        if t or c:
            line = ("%s。%s" % (t, c)) if c else t
        else:
            line = ""
        if line and total + len(line) + 1 <= max_chars:
            parts.append(line[:500])
            total += len(parts[-1]) + 1
        if total >= max_chars:
            break
    return "\n".join(parts) if parts else ""


class SegBpNewsAdapter:
    """seg_bp_* 细分适配器：仅从 L2 读取采集模块写入的标的新闻/公告，不调用 AkShare。"""

    def fetch_raw(self, segment_id: str, context: Dict[str, Any]) -> Optional[str]:
        symbols = context.get("symbols") or []
        pg_l2_dsn = (context.get("pg_l2_dsn") or "").strip()
        max_chars = int(context.get("max_input_chars") or _MAX_TEXT_CHARS)
        days_back = int(context.get("days_back") or 7)
        if not symbols:
            return None
        symbol = symbols[0] if isinstance(symbols[0], str) else ""
        text = ""
        if pg_l2_dsn:
            try:
                conn = psycopg2.connect(pg_l2_dsn)
                try:
                    text = _fetch_news_from_l2(conn, symbol, days_back=days_back, max_chars=max_chars)
                finally:
                    conn.close()
            except Exception as e:
                logger.warning("SegBpNewsAdapter L2 连接失败: %s", e)
        if not text or len(text.strip()) < 10:
            return None
        return text[:max_chars]
