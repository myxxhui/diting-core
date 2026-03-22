# [Ref: 02_B模块策略_策略实现规约] [Ref: 09_核心模块架构规约] A 轨短线：技术面建议入场/止损/止盈价位（仅信号层，执行在 Module E）
# 与 scanner_rules.yaml module_b_quant_engine.a_track_short.risk 对齐

import copy
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from diting.scanner import indicators

logger = logging.getLogger(__name__)


def _volatility_tier_scales(percentile: float, vol_cfg: Dict[str, Any]) -> Tuple[str, float, float]:
    """
    截面 ATR/close 分位 [0,1]，越高表示相对全样本波动越大。
    返回：(档位名, atr_stop_multiple 乘子, fixed_stop_pct 乘子)
    """
    low_max = float(vol_cfg.get("low_atr_percentile_max", 0.33))
    high_min = float(vol_cfg.get("high_atr_percentile_min", 0.66))
    p = max(0.0, min(1.0, float(percentile)))
    if p <= low_max:
        low = vol_cfg.get("low_volatility") or {}
        return (
            "low",
            float(low.get("atr_stop_multiple_scale", 1.0)),
            float(low.get("fixed_stop_pct_scale", 1.0)),
        )
    if p >= high_min:
        high = vol_cfg.get("high_volatility") or {}
        return (
            "high",
            float(high.get("atr_stop_multiple_scale", 1.0)),
            float(high.get("fixed_stop_pct_scale", 1.0)),
        )
    mid = vol_cfg.get("mid_volatility") or {}
    return (
        "mid",
        float(mid.get("atr_stop_multiple_scale", 1.0)),
        float(mid.get("fixed_stop_pct_scale", 1.0)),
    )


def _merge_strategy_and_meta(
    risk_cfg: Dict[str, Any],
    strategy_source: int,
    atr_percentile: Optional[float],
    signal_tier: str,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    从 YAML risk 段剥离元配置，合并 strategy_risk_overrides、volatility_tier、alert_tier，返回 (effective_flat_cfg, meta)。
    """
    meta: Dict[str, Any] = {}
    cfg = copy.deepcopy(risk_cfg) if risk_cfg else {}
    strat_over = cfg.pop("strategy_risk_overrides", None)
    vol_cfg = cfg.pop("volatility_tier", None)
    alert_cfg = cfg.pop("alert_tier", None)

    if isinstance(strat_over, dict) and strategy_source:
        po = strat_over.get(str(strategy_source))
        if po is None:
            po = strat_over.get(int(strategy_source))
        if isinstance(po, dict):
            cfg.update(po)
            meta["strategy_risk_override"] = sorted(po.keys())

    if isinstance(vol_cfg, dict) and vol_cfg.get("enabled") and atr_percentile is not None:
        name, ams, fps = _volatility_tier_scales(float(atr_percentile), vol_cfg)
        meta["volatility_tier"] = name
        meta["atr_percentile"] = round(float(atr_percentile), 4)
        cfg["atr_stop_multiple"] = float(cfg.get("atr_stop_multiple", 2.0)) * ams
        cfg["fixed_stop_pct"] = float(cfg.get("fixed_stop_pct", 0.02)) * fps

    if isinstance(alert_cfg, dict) and signal_tier == "ALERT":
        if alert_cfg.get("take_profit_r_multiples") is not None:
            cfg["take_profit_r_multiples"] = list(alert_cfg["take_profit_r_multiples"])
            meta["alert_tp_override"] = True
        if alert_cfg.get("fixed_stop_pct_scale") is not None:
            cfg["fixed_stop_pct"] = float(cfg.get("fixed_stop_pct", 0.02)) * float(alert_cfg["fixed_stop_pct_scale"])
            meta["alert_sl_scale"] = float(alert_cfg["fixed_stop_pct_scale"])
        if alert_cfg.get("atr_stop_multiple_scale") is not None:
            cfg["atr_stop_multiple"] = float(cfg.get("atr_stop_multiple", 2.0)) * float(alert_cfg["atr_stop_multiple_scale"])
            meta["alert_atr_mult_scale"] = float(alert_cfg["atr_stop_multiple_scale"])

    return cfg, meta


def compute_a_track_risk_levels(
    open_: Any,
    high: Any,
    low: Any,
    close: Any,
    risk_cfg: Optional[Dict[str, Any]] = None,
    *,
    strategy_source: int = 0,
    atr_percentile: Optional[float] = None,
    signal_tier: str = "NONE",
) -> Dict[str, Any]:
    """
    基于最后一根 K 线收盘价与 ATR/固定比例，产出建议止损与分档止盈（R 倍）。
    可选：按 strategy_source 覆盖、按截面波动分位调节止损参数、ALERT 档单独止盈档位。

    :param strategy_source: 主策略池 id（1 趋势 / 2 反转 / 3 突破 / 4 动量）
    :param atr_percentile: 本批截面 ATR/close 分位 [0,1]；与 volatility_tier 联用
    :param signal_tier: NONE / ALERT / CONFIRMED；ALERT 可读 alert_tier 子配置

    :return: entry_reference_price, stop_loss_price, take_profit_prices[], risk_rules_json, stop_rule_id, tp_rule_id
    """
    risk_cfg = risk_cfg or {}
    if not risk_cfg.get("enabled", True):
        return {
            "entry_reference_price": None,
            "stop_loss_price": None,
            "take_profit_prices": [],
            "risk_rules_json": "{}",
            "stop_rule_id": "disabled",
            "tp_rule_id": "disabled",
        }
    try:
        c_last = close[-1] if close is not None and hasattr(close, "__getitem__") and len(close) > 0 else None
    except (TypeError, IndexError):
        c_last = None
    if c_last is None or float(c_last) <= 0:
        return {
            "entry_reference_price": None,
            "stop_loss_price": None,
            "take_profit_prices": [],
            "risk_rules_json": json.dumps({"error": "no_close"}, ensure_ascii=False),
            "stop_rule_id": "none",
            "tp_rule_id": "none",
        }
    entry = float(c_last)
    # entry_price_field：配置项仅写入 risk_rules_json；数值入场恒为 close[-1]（未来若扩展 open/vwap 再分支）
    eff, meta = _merge_strategy_and_meta(
        copy.deepcopy(risk_cfg) if risk_cfg else {},
        strategy_source,
        atr_percentile,
        signal_tier,
    )

    stop_mode = str(eff.get("stop_mode", "fixed_pct")).strip().lower()
    atr_period = int(eff.get("atr_period", 14))
    fixed_pct = float(eff.get("fixed_stop_pct", 0.02))
    atr_mult = float(eff.get("atr_stop_multiple", 2.0))
    r_multiples: List[float] = eff.get("take_profit_r_multiples") or [1.0, 2.0]

    stop_price: float
    stop_rule_id: str

    if stop_mode == "atr_multiple" and indicators.has_talib():
        atr_vals = indicators.atr(high, low, close, atr_period)
        atr_last = atr_vals[-1] if atr_vals and len(atr_vals) > 0 else None
        if atr_last is not None and float(atr_last) > 0:
            risk_amount = float(atr_last) * atr_mult
            stop_price = entry - risk_amount
            stop_rule_id = "atr_%s_x%s" % (atr_period, atr_mult)
        else:
            stop_price = entry * (1.0 - fixed_pct)
            stop_rule_id = "fixed_pct_fallback_%.4f" % fixed_pct
    else:
        stop_price = entry * (1.0 - fixed_pct)
        stop_rule_id = "fixed_pct_%.4f" % fixed_pct

    risk_per_share = max(entry - stop_price, entry * 1e-6)
    tps: List[float] = []
    for m in r_multiples:
        try:
            tps.append(entry + float(m) * risk_per_share)
        except (TypeError, ValueError):
            continue
    rules: Dict[str, Any] = {
        "stop_mode": stop_mode,
        "entry_field": eff.get("entry_price_field", risk_cfg.get("entry_price_field", "last_close")),
        "fixed_stop_pct": fixed_pct,
        "atr_period": atr_period,
        "atr_multiple": atr_mult,
        "r_multiples": r_multiples,
        "strategy_source": int(strategy_source) if strategy_source else None,
        "signal_tier": signal_tier,
    }
    rules.update(meta)
    return {
        "entry_reference_price": entry,
        "stop_loss_price": float(stop_price),
        "take_profit_prices": tps,
        "risk_rules_json": json.dumps(rules, ensure_ascii=False),
        "stop_rule_id": stop_rule_id,
        "tp_rule_id": "r_multiples_%s" % ("_".join(str(x) for x in r_multiples)),
    }
