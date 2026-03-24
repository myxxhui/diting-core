# [Ref: 06_B轨_信号层生产级数据采集_设计] seg_bp_* 细分：仅从 L2 news_content 读取采集模块写入的标的新闻/公告；不调用 AkShare

import logging
from typing import Any, Dict, List, Optional

import psycopg2

from diting.signal_layer.news_fetch import fetch_symbol_news_text

logger = logging.getLogger(__name__)

_MAX_TEXT_CHARS = 4096


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
                    text = fetch_symbol_news_text(
                        conn, symbol, days_back=days_back, max_chars=max_chars
                    )
                finally:
                    conn.close()
            except Exception as e:
                logger.warning("SegBpNewsAdapter L2 连接失败: %s", e)
        if not text or len(text.strip()) < 10:
            return None
        return text[:max_chars]
