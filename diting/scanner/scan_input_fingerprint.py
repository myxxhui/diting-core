# [Ref: 02_B模块策略] 扫描输入指纹：L1 最新 K 线时间、L2 新闻/公告最新时间，用于冷却「仅当基本数据未变」

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def fetch_l1_ohlcv_max_ts_batch(
    symbols: List[str],
    dsn: Optional[str],
    *,
    period: str = "daily",
) -> Dict[str, datetime]:
    """L1 ohlcv 表：每标的最后一根 K 的 datetime（用于判断 K 线是否较上次扫描有更新）。"""
    out: Dict[str, datetime] = {}
    if not symbols or not (dsn or "").strip():
        return out
    dsn = dsn.strip()
    uniq = sorted({str(s).strip().upper() for s in symbols if s and str(s).strip()})
    if not uniq:
        return out
    try:
        import psycopg2
    except ImportError:
        return out
    try:
        conn = psycopg2.connect(dsn, connect_timeout=15)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT symbol, MAX(datetime)
                FROM ohlcv
                WHERE symbol = ANY(%s) AND period = %s
                GROUP BY symbol
                """,
                (uniq, period),
            )
            for sym, ts in cur.fetchall():
                if sym and ts is not None:
                    out[str(sym).strip().upper()] = ts
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("fetch_l1_ohlcv_max_ts_batch 失败: %s", e)
    return out


def fetch_l2_news_max_ts_batch(symbols: List[str], l2_dsn: Optional[str]) -> Dict[str, datetime]:
    """
    L2 news_content：每标的最新一条新闻/公告时间（published_at 与 created_at 取较大者）。
    无新闻的标的不出现在 dict 中。
    """
    out: Dict[str, datetime] = {}
    if not symbols or not (l2_dsn or "").strip():
        return out
    l2_dsn = l2_dsn.strip()
    uniq = sorted({str(s).strip().upper() for s in symbols if s and str(s).strip()})
    if not uniq:
        return out
    try:
        import psycopg2
    except ImportError:
        return out
    try:
        conn = psycopg2.connect(l2_dsn, connect_timeout=15)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT UPPER(TRIM(symbol)), MAX(GREATEST(published_at::timestamptz, created_at::timestamptz))
                FROM news_content
                WHERE UPPER(TRIM(symbol)) = ANY(%s)
                GROUP BY UPPER(TRIM(symbol))
                """,
                (uniq,),
            )
            for sym, ts in cur.fetchall():
                if sym and ts is not None:
                    out[str(sym).strip().upper()] = ts
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("fetch_l2_news_max_ts_batch 失败: %s", e)
    return out


def fetch_stored_scan_input_from_l2(
    symbols: List[str],
    l2_dsn: Optional[str],
) -> Dict[str, Tuple[Optional[datetime], Optional[datetime]]]:
    """
    从 quant_signal_scan_all 每标的最新一行读取上次扫描写入的指纹。
    若表无列或查询失败，返回空 dict（调用方按「无基线」处理）。
    """
    out: Dict[str, Tuple[Optional[datetime], Optional[datetime]]] = {}
    if not symbols or not (l2_dsn or "").strip():
        return out
    uniq = sorted({str(s).strip().upper() for s in symbols if s and str(s).strip()})
    if not uniq:
        return out
    try:
        import psycopg2
    except ImportError:
        return out
    try:
        conn = psycopg2.connect(l2_dsn.strip(), connect_timeout=15)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'quant_signal_scan_all'
                  AND column_name IN ('scan_input_ohlcv_max_ts', 'scan_input_news_max_ts')
                """
            )
            cols = {r[0] for r in cur.fetchall()}
            if "scan_input_ohlcv_max_ts" not in cols or "scan_input_news_max_ts" not in cols:
                cur.close()
                return out
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'quant_signal_scan_all'
                  AND column_name = 'updated_at'
                """
            )
            has_updated = cur.fetchone() is not None
            ts_expr = (
                "GREATEST(created_at, COALESCE(updated_at, created_at))"
                if has_updated
                else "created_at"
            )
            cur.execute(
                f"""
                SELECT DISTINCT ON (symbol)
                    symbol, scan_input_ohlcv_max_ts, scan_input_news_max_ts
                FROM quant_signal_scan_all
                WHERE symbol = ANY(%s)
                ORDER BY symbol, {ts_expr} DESC
                """,
                (uniq,),
            )
            for row in cur.fetchall():
                sym = str(row[0] or "").strip().upper()
                if sym:
                    out[sym] = (row[1], row[2])
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("fetch_stored_scan_input_from_l2 失败: %s", e)
    return out


def cooldown_still_valid(
    symbol: str,
    *,
    current_ohlcv_max: Optional[datetime],
    current_news_max: Optional[datetime],
    stored_ohlcv_max: Optional[datetime],
    stored_news_max: Optional[datetime],
) -> bool:
    """
    冷却是否仍生效：上次写入指纹存在，且当前 L1/L2 时间均未晚于上次扫描所依据的输入。
    无基线（旧行全 NULL）视为无效，应重算。
    """
    if stored_ohlcv_max is None and stored_news_max is None:
        return False
    if current_ohlcv_max is None:
        return False
    if stored_ohlcv_max is None or current_ohlcv_max > stored_ohlcv_max:
        return False
    # 新闻：若本次有比上次新的内容，则冷却失效
    if current_news_max is not None:
        if stored_news_max is None or current_news_max > stored_news_max:
            return False
    return True
