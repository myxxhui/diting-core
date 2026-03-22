# [Ref: 02_量化扫描引擎_实践] 从 L2 quant_signal_scan_all 读取 Module B 输出，供 Module C 不重跑扫描

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def resolve_moe_quant_batch_id(config_batch_id: Optional[str] = None) -> Optional[str]:
    """MOE_QUANT_BATCH_ID / SCANNER_QUANT_BATCH_ID 覆盖；否则用 config。"""
    for key in ("MOE_QUANT_BATCH_ID", "SCANNER_QUANT_BATCH_ID"):
        v = (os.environ.get(key) or "").strip()
        if v:
            return v
    return (config_batch_id or "").strip() or None


def fetch_latest_quant_batch_id(dsn: str) -> Optional[str]:
    """
    L2 quant_signal_scan_all 中，按 MAX(created_at) 取「最近一次 B 写入」的 batch_id（整批一致）。
    表空或失败时返回 None，调用方可回退为按标的取最新行。
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
                FROM quant_signal_scan_all
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
        logger.warning("fetch_latest_quant_batch_id: %s", e)
    return None


def fetch_quant_signal_scan_all_map(
    dsn: str,
    symbols: List[str],
    batch_id: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    读取 quant_signal_scan_all：symbol.upper() -> quant_signal 风格 dict（供 route_and_collect_opinions）。
    batch_id 非空时仅该批；否则每 symbol 取 created_at 最新一条。
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
                    SELECT DISTINCT ON (symbol) symbol, technical_score, strategy_source, sector_strength,
                           trend_score, reversion_score, breakout_score, momentum_score,
                           long_term_score, long_term_candidate, passed,
                           alert_passed, confirmed_passed, correlation_id, batch_id
                    FROM quant_signal_scan_all
                    WHERE symbol = ANY(%s) AND batch_id = %s
                    ORDER BY symbol, created_at DESC
                    """,
                    (uniq, bid),
                )
            else:
                cur.execute(
                    """
                    SELECT DISTINCT ON (symbol) symbol, technical_score, strategy_source, sector_strength,
                           trend_score, reversion_score, breakout_score, momentum_score,
                           long_term_score, long_term_candidate, passed,
                           alert_passed, confirmed_passed, correlation_id, batch_id
                    FROM quant_signal_scan_all
                    WHERE symbol = ANY(%s)
                    ORDER BY symbol, created_at DESC
                    """,
                    (uniq,),
                )
            for row in cur.fetchall():
                sym = str(row[0]).strip().upper()
                trend = float(row[4] or 0)
                rev = float(row[5] or 0)
                brk = float(row[6] or 0)
                mom = float(row[7] or 0)
                lt = row[8]
                lt_cand = bool(row[9])
                passed = bool(row[10])
                alert_p = bool(row[11])
                conf_p = bool(row[12])
                out[sym] = {
                    "symbol": sym,
                    "technical_score": float(row[1] or 0),
                    "strategy_source": str(row[2] or "UNSPECIFIED"),
                    "sector_strength": float(row[3] or 0),
                    "pool_scores": {1: trend, 2: rev, 3: brk, 4: mom},
                    "long_term_score": float(lt) if lt is not None else None,
                    "long_term_candidate": lt_cand,
                    "passed": passed,
                    "alert_passed": alert_p,
                    "confirmed_passed": conf_p,
                    "correlation_id": str(row[13] or ""),
                    "quant_batch_id": str(row[14] or ""),
                }
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("fetch_quant_signal_scan_all_map: %s", e)
        return {}
    return out
