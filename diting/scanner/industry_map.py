# [Ref: 02_B模块策略_策略实现规约 §3.4] [Ref: 11_数据采集与输入层规约]
# 从 L2 industry_revenue_summary 批量读取 symbol -> industry_name，供板块强度截面聚合

from __future__ import annotations

import logging
import os
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def fetch_symbol_industry_map(symbols: List[str], dsn: Optional[str] = None) -> Dict[str, str]:
    """
    批量查询标的 -> 申万/采集侧行业名（与 Module A 同源表）。
    无 DSN、失败或表无数据时返回 {}（此时 sector_strength 按规约退化为 1.0）。
    """
    if not symbols:
        return {}
    # L2 表：优先 PG_L2_DSN，与 TIMESCALE 同库时后者可作回退 [Ref: 02_B模块策略_策略实现规约]
    dsn = (dsn or os.environ.get("PG_L2_DSN", "") or os.environ.get("TIMESCALE_DSN", "") or "").strip()
    if not dsn:
        return {}
    try:
        import psycopg2
    except ImportError:
        return {}
    uniq = sorted({str(s).strip().upper() for s in symbols if s and str(s).strip()})
    if not uniq:
        return {}
    out: Dict[str, str] = {}
    try:
        conn = psycopg2.connect(dsn, connect_timeout=15)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT symbol, COALESCE(TRIM(industry_name), '')
                FROM industry_revenue_summary
                WHERE symbol = ANY(%s)
                """,
                (uniq,),
            )
            for row in cur.fetchall():
                sym = str(row[0]).strip().upper()
                ind = str(row[1]).strip() if row[1] else ""
                if sym:
                    out[sym] = ind
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("industry_revenue_summary 批量读取失败: %s", e)
        return {}
    return out
