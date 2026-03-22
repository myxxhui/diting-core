# [Ref: 02_B模块策略_策略实现规约] 冷却跳过本轮 TA-Lib 时，从 L2 最近一次「通过表」快照沿用行写入本 batch_id，
# 使 make query-module-b-output 按批次可查「全部应出现在通过表中的标的」，与 Module C 消费一致。

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


def _tp_list_from_json(raw: Any) -> List[float]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        xs = list(raw)
    else:
        s = str(raw).strip()
        if not s or s == "[]":
            return []
        try:
            xs = json.loads(s)
        except (json.JSONDecodeError, TypeError, ValueError):
            return []
    out: List[float] = []
    for x in xs:
        try:
            out.append(float(x))
        except (TypeError, ValueError):
            pass
    return out


def _row_to_signal_dict(
    row: Dict[str, Any],
    *,
    batch_id: str,
    correlation_id: str,
    use_scan_all_passed: bool,
    scanner_rules_fingerprint: str,
    evaluation_source: str = "CARRYOVER",
) -> Dict[str, Any]:
    """将 L2 行（RealDict 或普通 dict）转为与 scan_market 输出同构的 signal dict。"""
    sym = str(row.get("symbol") or "").strip().upper()
    t = float(row.get("trend_score") or 0)
    rv = float(row.get("reversion_score") or 0)
    br = float(row.get("breakout_score") or 0)
    m = float(row.get("momentum_score") or 0)
    pool_scores = {1: t, 2: rv, 3: br, 4: m}
    src_raw = row.get("strategy_source")
    if isinstance(src_raw, int):
        _sn = {0: "UNSPECIFIED", 1: "TREND", 2: "REVERSION", 3: "BREAKOUT", 4: "MOMENTUM"}
        strategy_source = _sn.get(src_raw, "UNSPECIFIED")
    else:
        strategy_source = str(src_raw or "UNSPECIFIED").strip().upper()[:16]
    alert = bool(row.get("alert_passed"))
    confirmed = bool(row.get("confirmed_passed"))
    if use_scan_all_passed and "passed" in row:
        passed = bool(row.get("passed"))
    else:
        passed = confirmed
    tp_raw = row.get("take_profit_json")
    return {
        "symbol": sym,
        "symbol_name": str(row.get("symbol_name") or "")[:128],
        "technical_score": float(row.get("technical_score") or 0),
        "strategy_source": strategy_source,
        "sector_strength": float(row.get("sector_strength") or 0),
        "correlation_id": correlation_id,
        "passed": passed,
        "alert_passed": alert,
        "confirmed_passed": confirmed,
        "signal_tier": str(row.get("signal_tier") or "")[:16],
        "second_pool_id": None,
        "second_pool_score": 0.0,
        "pool_scores": pool_scores,
        "liquidity_score": 100.0,
        "volatility_ratio": 1.0,
        "entry_reference_price": row.get("entry_reference_price"),
        "stop_loss_price": row.get("stop_loss_price"),
        "take_profit_prices": _tp_list_from_json(tp_raw),
        "risk_rules_json": str(row.get("risk_rules_json") or "{}")[:4000],
        "technical_score_percentile": row.get("technical_score_percentile"),
        "long_term_score": row.get("long_term_score"),
        "long_term_candidate": bool(row.get("long_term_candidate", False)),
        "scanner_rules_fingerprint": str(scanner_rules_fingerprint or "")[:32],
        "evaluation_source": str(evaluation_source or "CARRYOVER")[:16],
        # 沿用行非本轮截面计算；与 FRESH 信号的 True/False 区分
        "industry_mapped": None,
    }


def carryover_signals_from_l2(
    dsn: str,
    cooldown_skipped_symbols: List[str],
    *,
    batch_id: str,
    correlation_id: str,
    scanner_rules_fingerprint: str = "",
    already_present_symbols: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    """
    对「因冷却跳过、本轮未重算」的标的，从 L2 最近一次 quant_signal_snapshot（缺则 quant_signal_scan_all）取行，
    改写 batch_id / correlation_id 后作为本批 signal 追加写入，保证通过表按批次完整。

    :param already_present_symbols: 本轮已产出 signal 的 symbol 大写集合，避免重复。
    """
    if not dsn or not cooldown_skipped_symbols:
        return []
    present = {str(s).strip().upper() for s in (already_present_symbols or set()) if s}
    need = sorted(
        {
            str(s).strip().upper()
            for s in cooldown_skipped_symbols
            if s and str(s).strip().upper() not in present
        }
    )
    if not need:
        return []
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except ImportError:
        logger.warning("carryover_signals_from_l2: psycopg2 未安装，跳过")
        return []

    out: List[Dict[str, Any]] = []
    got: Set[str] = set()

    try:
        conn = psycopg2.connect(dsn, connect_timeout=15)
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(
                """
                SELECT DISTINCT ON (symbol)
                    batch_id, symbol, symbol_name, technical_score, strategy_source, sector_strength,
                    trend_score, reversion_score, breakout_score, momentum_score, technical_score_percentile,
                    long_term_score, long_term_candidate, correlation_id, signal_tier,
                    alert_passed, confirmed_passed,
                    entry_reference_price, stop_loss_price, take_profit_json, risk_rules_json, created_at
                FROM quant_signal_snapshot
                WHERE symbol = ANY(%s)
                  AND (alert_passed = true OR confirmed_passed = true)
                ORDER BY symbol, created_at DESC
                """,
                (need,),
            )
            for row in cur.fetchall():
                d = dict(row)
                sym = str(d.get("symbol") or "").strip().upper()
                if not sym:
                    continue
                got.add(sym)
                out.append(
                    _row_to_signal_dict(
                        d,
                        batch_id=batch_id,
                        correlation_id=correlation_id,
                        use_scan_all_passed=False,
                        scanner_rules_fingerprint=scanner_rules_fingerprint,
                    )
                )

            missing = [s for s in need if s not in got]
            if missing:
                cur.execute(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'quant_signal_scan_all' AND column_name = 'updated_at'
                    """
                )
                has_updated = cur.fetchone() is not None
                ts_order = (
                    "GREATEST(created_at, COALESCE(updated_at, created_at)) DESC"
                    if has_updated
                    else "created_at DESC"
                )
                cur.execute(
                    f"""
                    SELECT DISTINCT ON (symbol)
                        batch_id, symbol, symbol_name, technical_score, strategy_source, sector_strength,
                        trend_score, reversion_score, breakout_score, momentum_score, technical_score_percentile,
                        passed, long_term_score, long_term_candidate, correlation_id, signal_tier,
                        alert_passed, confirmed_passed,
                        entry_reference_price, stop_loss_price, take_profit_json, risk_rules_json, created_at
                    FROM quant_signal_scan_all
                    WHERE symbol = ANY(%s)
                      AND (alert_passed = true OR confirmed_passed = true)
                    ORDER BY symbol, {ts_order}
                    """,
                    (missing,),
                )
                for row in cur.fetchall():
                    d = dict(row)
                    sym = str(d.get("symbol") or "").strip().upper()
                    if not sym:
                        continue
                    out.append(
                        _row_to_signal_dict(
                            d,
                            batch_id=batch_id,
                            correlation_id=correlation_id,
                            use_scan_all_passed=True,
                            scanner_rules_fingerprint=scanner_rules_fingerprint,
                        )
                    )
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("carryover_signals_from_l2 查询/组装失败: %s", e)
        return []

    return out
