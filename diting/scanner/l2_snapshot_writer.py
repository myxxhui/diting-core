# [Ref: 02_量化扫描引擎_实践] [Ref: 09_ Module B] QuantSignal 写入 L2：通过表 + 全量表（分开存放）
# quant_signal_snapshot = 预警/确认档候选（Module C 默认消费 confirmed）；quant_signal_scan_all = 全量含当前分数（通过/未通过可查）

import json
import logging
import uuid
from typing import Any, List

logger = logging.getLogger(__name__)

# StrategyPool 枚举名，与 02 规约 §4、QuantSignal 契约一致；L2 表 strategy_source VARCHAR(16)
_STRATEGY_NAMES = {
    0: "UNSPECIFIED",
    1: "TREND",
    2: "REVERSION",
    3: "BREAKOUT",
    4: "MOMENTUM",
}


def _get_attr(obj: Any, key: str, default: Any = None) -> Any:
    if hasattr(obj, key):
        return getattr(obj, key, default)
    if isinstance(obj, dict):
        return obj.get(key, default)
    return default


def _pool_scores(signal: Any) -> tuple:
    """从 signal 的 pool_scores 取出 趋势/反转/突破/动量 得分，池 id 1/2/3/4。"""
    ps = _get_attr(signal, "pool_scores") or {}
    if not isinstance(ps, dict):
        return (0.0, 0.0, 0.0, 0.0)
    return (
        float(ps.get(1, 0)),
        float(ps.get(2, 0)),
        float(ps.get(3, 0)),
        float(ps.get(4, 0)),
    )


def _signal_to_row(signal: Any, batch_id: str, correlation_id: str) -> tuple:
    """将单条 QuantSignal 转为 quant_signal_snapshot 行（含 symbol_name、各池得分、截面分位）。"""
    symbol = str(_get_attr(signal, "symbol") or "")[:32]
    symbol_name = str(_get_attr(signal, "symbol_name") or "")[:128]
    technical_score = float(_get_attr(signal, "technical_score") or 0)
    strategy_source = _get_attr(signal, "strategy_source", 0)
    if isinstance(strategy_source, int):
        strategy_source = _STRATEGY_NAMES.get(strategy_source, "UNSPECIFIED")
    else:
        strategy_source = str(strategy_source or "UNSPECIFIED")[:16]
    sector_strength = float(_get_attr(signal, "sector_strength") or 0)
    trend_score, reversion_score, breakout_score, momentum_score = _pool_scores(signal)
    corr = str(_get_attr(signal, "correlation_id") or correlation_id)[:64]
    pct = _get_attr(signal, "technical_score_percentile")
    score_percentile = float(pct) if pct is not None else None
    lt_score = _get_attr(signal, "long_term_score")
    long_term_score = float(lt_score) if lt_score is not None else None
    long_term_candidate = bool(_get_attr(signal, "long_term_candidate", False))
    signal_tier = str(_get_attr(signal, "signal_tier") or "")[:16]
    alert_passed = bool(_get_attr(signal, "alert_passed", False))
    confirmed_passed = bool(_get_attr(signal, "confirmed_passed", _get_attr(signal, "passed", False)))
    entry_p = _get_attr(signal, "entry_reference_price")
    stop_p = _get_attr(signal, "stop_loss_price")
    entry_ref = float(entry_p) if entry_p is not None else None
    stop_loss = float(stop_p) if stop_p is not None else None
    tps = _get_attr(signal, "take_profit_prices") or []
    take_profit_json = json.dumps(list(tps), ensure_ascii=False) if tps else "[]"
    risk_json = str(_get_attr(signal, "risk_rules_json") or "{}")[:4000]
    fp = str(_get_attr(signal, "scanner_rules_fingerprint") or "")[:32]
    ev = str(_get_attr(signal, "evaluation_source") or "FRESH")[:16]
    return (batch_id, symbol, symbol_name, technical_score, strategy_source, sector_strength,
            trend_score, reversion_score, breakout_score, momentum_score, score_percentile,
            long_term_score, long_term_candidate, corr,
            signal_tier, alert_passed, confirmed_passed, entry_ref, stop_loss, take_profit_json, risk_json,
            fp, ev)


def _signal_to_scan_all_row(signal: Any, batch_id: str, correlation_id: str) -> tuple:
    """将单条 QuantSignal 转为 quant_signal_scan_all 行：percentile 后为 passed，再 long_term… 至 risk_json。"""
    row = _signal_to_row(signal, batch_id, correlation_id)
    passed = bool(_get_attr(signal, "passed", False))
    # row: …21 元组末两项为 fingerprint、evaluation_source
    return row[:11] + (passed,) + row[11:]


def write_quant_signal_snapshot(
    dsn: str,
    signals: List[Any],
    batch_id: str = "",
    correlation_id: str = "",
) -> int:
    """
    将本批「预警或确认」档写入 L2 quant_signal_snapshot（Module C 默认仅用 confirmed_passed）。
    :param signals: 过滤 confirmed_passed 或 alert_passed（无双档字段时回退 passed=True）
    :return: 写入行数
    """
    def _in_snapshot(s: Any) -> bool:
        if _get_attr(s, "alert_passed", None) is not None or _get_attr(s, "confirmed_passed", None) is not None:
            return bool(_get_attr(s, "confirmed_passed", False) or _get_attr(s, "alert_passed", False))
        return bool(_get_attr(s, "passed", True))

    passed_only = [s for s in (signals or []) if _in_snapshot(s)]
    if not passed_only:
        return 0
    batch_id = batch_id or str(uuid.uuid4())
    correlation_id = correlation_id or batch_id

    try:
        import psycopg2
    except ImportError:
        logger.warning("psycopg2 未安装，跳过写入 L2 quant_signal_snapshot")
        return 0

    rows = [_signal_to_row(s, batch_id, correlation_id) for s in passed_only]

    try:
        conn = psycopg2.connect(dsn)
        try:
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT INTO quant_signal_snapshot
                (batch_id, symbol, symbol_name, technical_score, strategy_source, sector_strength, trend_score, reversion_score, breakout_score, momentum_score, technical_score_percentile, long_term_score, long_term_candidate, correlation_id, signal_tier, alert_passed, confirmed_passed, entry_reference_price, stop_loss_price, take_profit_json, risk_rules_json, scanner_rules_fingerprint, evaluation_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
            conn.commit()
            n = len(rows)
            logger.info("QuantSignal 写入 L2 quant_signal_snapshot（通过）: batch_id=%s, 行数=%s", batch_id, n)
            return n
        finally:
            conn.close()
    except Exception as e:
        logger.warning("写入 quant_signal_snapshot 失败（表可能未创建）: %s", e)
        return 0


def write_quant_signal_scan_all(
    dsn: str,
    signals: List[Any],
    batch_id: str = "",
    correlation_id: str = "",
) -> int:
    """
    将本批全量扫描结果（含通过/未通过）写入 L2 表 quant_signal_scan_all，保存当前分数供随时查询。
    :param signals: 每项具 symbol, technical_score, strategy_source, sector_strength, passed
    :return: 写入行数
    """
    if not signals:
        return 0
    batch_id = batch_id or str(uuid.uuid4())
    correlation_id = correlation_id or batch_id

    try:
        import psycopg2
    except ImportError:
        logger.warning("psycopg2 未安装，跳过写入 L2 quant_signal_scan_all")
        return 0

    rows = [_signal_to_scan_all_row(s, batch_id, correlation_id) for s in signals]

    try:
        conn = psycopg2.connect(dsn)
        try:
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT INTO quant_signal_scan_all
                (batch_id, symbol, symbol_name, technical_score, strategy_source, sector_strength, trend_score, reversion_score, breakout_score, momentum_score, technical_score_percentile, passed, long_term_score, long_term_candidate, correlation_id, signal_tier, alert_passed, confirmed_passed, entry_reference_price, stop_loss_price, take_profit_json, risk_rules_json, scanner_rules_fingerprint, evaluation_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
            conn.commit()
            n = len(rows)
            logger.info("QuantSignal 全量写入 L2 quant_signal_scan_all: batch_id=%s, 行数=%s", batch_id, n)
            return n
        finally:
            conn.close()
    except Exception as e:
        logger.warning("写入 quant_signal_scan_all 失败（表可能未创建）: %s", e)
        return 0
