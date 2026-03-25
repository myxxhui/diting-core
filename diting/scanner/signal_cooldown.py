# [Ref: 02_B模块策略_策略实现规约] 可选：近 N 个日历日内「最新一条仍为通过档」则跳过（读 L2）；
# 若自上次扫描后 L1 K 线或 L2 新闻/公告有更新，则该标的本轮不冷却（重算 TA-Lib）。

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Set

from diting.scanner.scan_input_fingerprint import (
    cooldown_still_valid,
    fetch_stored_scan_input_from_l2,
)

logger = logging.getLogger(__name__)


def symbols_in_signal_cooldown(
    symbols: List[str],
    dsn: Optional[str],
    cooldown_days: int,
    *,
    confirmed_only: bool = True,
    current_ohlcv_max_ts: Optional[Dict[str, datetime]] = None,
    current_news_max_ts: Optional[Dict[str, datetime]] = None,
) -> Set[str]:
    """
    返回「在冷却期内」的标的集合（读 L2 quant_signal_scan_all）。

    语义（与「只看过期时间内任意一次通过」不同）：
    - 对每个标的只取 **时间最新的一条**（有效时间 = GREATEST(created_at, COALESCE(updated_at, created_at))）；
    - 若 **最新一条** 的确认/通过档为假，则 **不在冷却**；
    - 若 **最新一条** 为真，且有效时间在最近 ``cooldown_days`` 天内，则进入 **候选** 冷却；
    - **数据指纹**（可选）：若传入 ``current_ohlcv_max_ts`` / ``current_news_max_ts``，则与上次写入 L2 的
      ``scan_input_*`` 比较；K 线或新闻较上次扫描有更新时 **不** 冷却（本轮重算）。

    cooldown_days<=0 或未配置 DSN 时返回空集。
    """
    if cooldown_days <= 0 or not symbols:
        return set()
    dsn = (dsn or os.environ.get("PG_L2_DSN", "") or os.environ.get("TIMESCALE_DSN", "") or "").strip()
    if not dsn:
        return set()
    uniq = sorted({str(s).strip().upper() for s in symbols if s and str(s).strip()})
    if not uniq:
        return set()
    try:
        import psycopg2
    except ImportError:
        return set()
    pass_col = "confirmed_passed" if confirmed_only else "passed"
    if pass_col not in ("confirmed_passed", "passed"):
        pass_col = "confirmed_passed"
    candidates: Set[str] = set()
    try:
        conn = psycopg2.connect(dsn, connect_timeout=15)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'quant_signal_scan_all' AND column_name = 'updated_at'
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
                SELECT symbol FROM (
                    SELECT DISTINCT ON (symbol)
                        symbol,
                        {pass_col} AS pass_ok,
                        {ts_expr} AS mt
                    FROM quant_signal_scan_all
                    WHERE symbol = ANY(%s)
                    ORDER BY symbol, {ts_expr} DESC
                ) t
                WHERE pass_ok = true
                  AND mt >= NOW() - (%s * INTERVAL '1 day')
                """,
                (uniq, cooldown_days),
            )
            for row in cur.fetchall():
                if row[0]:
                    candidates.add(str(row[0]).strip().upper())
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("signal_cooldown 查询失败: %s", e)
        return set()

    use_fp = current_ohlcv_max_ts is not None and current_news_max_ts is not None
    if not use_fp or not candidates:
        return candidates & set(uniq)

    co = current_ohlcv_max_ts or {}
    cn = current_news_max_ts or {}
    stored = fetch_stored_scan_input_from_l2(list(candidates), dsn)
    out: Set[str] = set()
    for sym in candidates:
        if sym not in uniq:
            continue
        raw = stored.get(sym)
        sto_o, sto_n = (raw[0], raw[1]) if raw is not None else (None, None)
        cur_o = co.get(sym)
        cur_n = cn.get(sym)
        if cooldown_still_valid(
            sym,
            current_ohlcv_max=cur_o,
            current_news_max=cur_n,
            stored_ohlcv_max=sto_o,
            stored_news_max=sto_n,
        ):
            out.add(sym)
    return out
