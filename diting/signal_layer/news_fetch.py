# [Ref: 07_行业新闻与标的新闻分离存储] 从 L2 news_content 按 scope 聚合正文（信号层共用）
from __future__ import annotations

import logging
from typing import Optional

import psycopg2

logger = logging.getLogger(__name__)

_DEFAULT_MAX_CHARS = 4096


def fetch_symbol_news_text(
    conn,
    symbol: str,
    *,
    days_back: int = 7,
    max_chars: int = _DEFAULT_MAX_CHARS,
) -> str:
    """标的新闻/公告：scope=symbol 或历史未迁移行（兼容 symbol 列）。"""
    sym = (symbol or "").strip().upper()
    if not sym:
        return ""
    days = max(1, min(90, int(days_back)))
    cur = conn.cursor()
    try:
        try:
            cur.execute(
                """
                SELECT title, content FROM news_content
                WHERE symbol = %s
                  AND published_at >= NOW() - INTERVAL '1 day' * %s
                  AND (scope = 'symbol' OR scope IS NULL OR scope = '')
                ORDER BY published_at DESC
                LIMIT 40
                """,
                (sym, days),
            )
            rows = cur.fetchall()
        except Exception:
            cur.execute(
                """
                SELECT title, content FROM news_content
                WHERE symbol = %s
                  AND published_at >= NOW() - INTERVAL '1 day' * %s
                ORDER BY published_at DESC
                LIMIT 40
                """,
                (sym, days),
            )
            rows = cur.fetchall()
    except Exception as e:
        logger.warning("fetch_symbol_news_text L2 失败 symbol=%s: %s", sym, e)
        return ""
    finally:
        cur.close()
    return _join_rows(rows, max_chars)


def fetch_industry_news_text(
    conn,
    industry_name: str,
    *,
    days_back: int = 7,
    max_chars: int = _DEFAULT_MAX_CHARS,
) -> str:
    """申万行业新闻：scope=industry, scope_id=行业名（与 industry_revenue_summary 同源）。"""
    ind = (industry_name or "").strip()
    if not ind:
        return ""
    days = max(1, min(90, int(days_back)))
    cur = conn.cursor()
    try:
        try:
            cur.execute(
                """
                SELECT title, content FROM news_content
                WHERE scope = 'industry'
                  AND scope_id = %s
                  AND published_at >= NOW() - INTERVAL '1 day' * %s
                ORDER BY published_at DESC
                LIMIT 40
                """,
                (ind, days),
            )
            rows = cur.fetchall()
        except Exception:
            rows = []
    except Exception as e:
        logger.warning("fetch_industry_news_text L2 失败 industry=%s: %s", ind, e)
        return ""
    finally:
        cur.close()
    return _join_rows(rows, max_chars)


def _join_rows(rows: list, max_chars: int) -> str:
    parts = []
    total = 0
    for title, content in (rows or []):
        t = (title or "").strip()
        c = (content or "").strip()
        line = ("%s。%s" % (t, c)) if c else t
        if not line:
            continue
        if total + len(line) + 1 <= max_chars:
            parts.append(line[:800])
            total += len(parts[-1]) + 1
        if total >= max_chars:
            break
    return "\n".join(parts) if parts else ""
