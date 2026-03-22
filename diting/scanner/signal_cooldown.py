# [Ref: 02_B模块策略_策略实现规约] 可选：近 N 个日历日内「最新一条仍为通过档」则跳过（读 L2）

from __future__ import annotations

import logging
import os
from typing import List, Optional, Set

logger = logging.getLogger(__name__)


def symbols_in_signal_cooldown(
    symbols: List[str],
    dsn: Optional[str],
    cooldown_days: int,
    *,
    confirmed_only: bool = True,
) -> Set[str]:
    """
    返回「在冷却期内」的标的集合（读 L2 quant_signal_scan_all）。

    语义（与「只看过期时间内任意一次通过」不同）：
    - 对每个标的只取 **时间最新的一条**（有效时间 = GREATEST(created_at, COALESCE(updated_at, created_at))）；
    - 若 **最新一条** 的确认/通过档为假，则 **不在冷却**（允许当天因行情或补数重算后重新出结果）；
    - 若 **最新一条** 为真，且有效时间在最近 ``cooldown_days`` 天内，则 **在冷却**（跳过本轮 TA-Lib）。

    表需含 ``updated_at``（init_l2_quant_signal_table 会 ADD）；无列时回退为仅 ``created_at``。

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
    out: Set[str] = set()
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
            # 每个 symbol 仅最新一行；再筛「最新行仍为通过」且在 N 日窗口内
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
                    out.add(str(row[0]).strip().upper())
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("signal_cooldown 查询失败: %s", e)
        return set()
    return out
