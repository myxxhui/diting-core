# [Ref: 02_B模块策略] 冷却沿用 L2 分数时，仍用 L1 最新 K 线重算参考价 / 止损 / 止盈

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from diting.scanner.ohlcv_feed import get_ohlcv_arrays_for_talib
from diting.scanner.risk_levels import compute_a_track_risk_levels

logger = logging.getLogger(__name__)

_STRATEGY_TO_INT = {
    "UNSPECIFIED": 0,
    "TREND": 1,
    "REVERSION": 2,
    "BREAKOUT": 3,
    "MOMENTUM": 4,
    "动量": 4,
}


def _pool_id_from_signal(signal: Dict[str, Any]) -> int:
    raw = signal.get("strategy_source")
    if isinstance(raw, int):
        return raw
    s = str(raw or "").strip().upper()
    return _STRATEGY_TO_INT.get(s, 0)


def refresh_carryover_signals_pricing(
    signals: List[Dict[str, Any]],
    ohlcv_dsn: Optional[str],
    risk_cfg: Optional[Dict[str, Any]],
    *,
    period: str = "daily",
    limit: int = 120,
) -> None:
    """
    就地更新 evaluation_source=CARRYOVER 的条目的 entry_reference_price、止损止盈与 risk_rules_json。
    无 OHLCV 或失败时保留原值。
    """
    if not signals or not ohlcv_dsn:
        return
    risk_cfg = risk_cfg or {}
    for item in signals:
        if str(item.get("evaluation_source") or "").upper() != "CARRYOVER":
            continue
        sym = str(item.get("symbol") or "").strip()
        if not sym:
            continue
        arr = get_ohlcv_arrays_for_talib(sym, period=period, limit=limit, dsn=ohlcv_dsn)
        if not arr:
            continue
        o, h, l, c, _v = arr
        if len(c) < 1:
            continue
        tier = str(item.get("signal_tier") or "NONE").upper() or "NONE"
        pool_id = _pool_id_from_signal(item)
        atr_pct = item.get("technical_score_percentile")
        try:
            atr_pct_f = float(atr_pct) if atr_pct is not None else None
        except (TypeError, ValueError):
            atr_pct_f = None
        rf = compute_a_track_risk_levels(
            o,
            h,
            l,
            c,
            risk_cfg,
            strategy_source=int(pool_id),
            atr_percentile=atr_pct_f,
            signal_tier=tier if tier in ("NONE", "ALERT", "CONFIRMED") else "NONE",
        )
        item["entry_reference_price"] = rf.get("entry_reference_price")
        item["stop_loss_price"] = rf.get("stop_loss_price")
        item["take_profit_prices"] = rf.get("take_profit_prices") or []
        item["risk_rules_json"] = rf.get("risk_rules_json")
        item["stop_rule_id"] = rf.get("stop_rule_id")
        item["tp_rule_id"] = rf.get("tp_rule_id")
