# [Ref: 02_量化扫描引擎_实践] [Ref: dna_module_b] 策略池与扫描阈值从 YAML 加载，禁止硬编码

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "scanner_rules.yaml"


def load_scanner_config(config_path: Optional[os.PathLike] = None) -> Dict[str, Any]:
    """
    加载 scanner_rules.yaml；与 dna_module_b.strategy_pools、scanner 语义一致。
    :return: 含 module_b_quant_engine.strategy_pools、scanner.technical_score_threshold、sector_strength_threshold 等。
    """
    path = Path(config_path) if config_path else _DEFAULT_CONFIG_PATH
    if not path.exists():
        logger.warning("scanner 配置不存在: %s，使用默认阈值", path)
        return {
            "module_b_quant_engine": {
                "strategy_pools": {},
                "scanner": {
                    "technical_score_threshold": 70,
                    "sector_strength_threshold": 1.0,
                },
            },
        }
    try:
        import yaml
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning("加载 scanner 配置失败: %s，使用默认阈值", e)
        return {
            "module_b_quant_engine": {
                "strategy_pools": {},
                "scanner": {
                    "technical_score_threshold": 70,
                    "sector_strength_threshold": 1.0,
                },
            },
        }
    return data


def get_thresholds(config: Optional[Dict[str, Any]] = None) -> tuple:
    """(technical_score_threshold, sector_strength_threshold) 从配置读取。"""
    if config is None:
        config = load_scanner_config()
    engine = config.get("module_b_quant_engine") or {}
    scanner = engine.get("scanner") or {}
    t = scanner.get("technical_score_threshold", 70)
    s = scanner.get("sector_strength_threshold", 1.0)
    return (int(t), float(s))


def get_pass_tightening_params(config: Optional[Dict[str, Any]] = None) -> Dict[str, Optional[float]]:
    """可选收紧：主池/次池下限。未设置（null 或缺失）则返回 None，表示不施加该约束。"""
    if config is None:
        config = load_scanner_config()
    engine = config.get("module_b_quant_engine") or {}
    scanner = engine.get("scanner") or {}
    lead = scanner.get("pass_require_lead_pool_min")
    second = scanner.get("pass_require_second_pool_min")
    return {
        "pass_require_lead_pool_min": float(lead) if lead is not None else None,
        "pass_require_second_pool_min": float(second) if second is not None else None,
    }


def get_scoring_params(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """连续打分参数；未配置时使用规约默认值。"""
    if config is None:
        config = load_scanner_config()
    engine = config.get("module_b_quant_engine") or {}
    s = engine.get("scoring") or {}
    trend = s.get("trend") or {}
    reversion = s.get("reversion") or {}
    breakout = s.get("breakout") or {}
    return {
        "trend": {
            "macd_hist_scale_ratio": float(trend.get("macd_hist_scale_ratio", 0.01)),
        },
        "reversion": {
            "rsi_oversold": int(reversion.get("rsi_oversold", 20)),
            "rsi_soft_ceiling": int(reversion.get("rsi_soft_ceiling", 35)),
        },
        "breakout": {
            "price_scale_ratio": float(breakout.get("price_scale_ratio", 0.02)),
            "volume_cap_multiple": float(breakout.get("volume_cap_multiple", 5.0)),
            "atr_scale_enabled": bool(breakout.get("atr_scale_enabled", True)),
            "min_atr_multiple": float(breakout.get("min_atr_multiple", 0.5)),
        },
    }


def get_optimization_params(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """优化项：趋势/突破确认、ADX/部分确认、反转放量止跌、多池加权融合、输出分位等；生产默认开启。"""
    if config is None:
        config = load_scanner_config()
    engine = config.get("module_b_quant_engine") or {}
    o = engine.get("optimization") or {}
    fw = o.get("fusion_weights") or [0.7, 0.2, 0.1]
    if not isinstance(fw, (list, tuple)) or len(fw) < 3:
        fw = [0.7, 0.2, 0.1]
    return {
        "trend_confirm_bars": int(o.get("trend_confirm_bars", 3)),
        "breakout_confirm_bars": int(o.get("breakout_confirm_bars", 2)),
        "reversion_require_above_ma5": bool(o.get("reversion_require_above_ma5", True)),
        "multi_pool_min_score": float(o.get("multi_pool_min_score", 40)),
        "multi_pool_bonus": float(o.get("multi_pool_bonus", 10)),
        "trend_adx_enabled": bool(o.get("trend_adx_enabled", True)),
        "trend_adx_min": float(o.get("trend_adx_min", 25)),
        "trend_adx_penalty_ratio": float(o.get("trend_adx_penalty_ratio", 0.6)),
        "trend_partial_confirm_enabled": bool(o.get("trend_partial_confirm_enabled", True)),
        "trend_partial_confirm_ratio": float(o.get("trend_partial_confirm_ratio", 0.7)),
        "trend_position_strength_enabled": bool(o.get("trend_position_strength_enabled", False)),
        "reversion_volume_bounce_enabled": bool(o.get("reversion_volume_bounce_enabled", True)),
        "reversion_volume_bounce_ratio": float(o.get("reversion_volume_bounce_ratio", 1.2)),
        "reversion_volume_bounce_bonus": float(o.get("reversion_volume_bounce_bonus", 1.1)),
        "reversion_acute_slow_enabled": bool(o.get("reversion_acute_slow_enabled", False)),
        "reversion_acute_threshold": float(o.get("reversion_acute_threshold", -0.08)),
        "reversion_slow_threshold": float(o.get("reversion_slow_threshold", -0.03)),
        "reversion_acute_weight": float(o.get("reversion_acute_weight", 1.2)),
        "reversion_slow_weight": float(o.get("reversion_slow_weight", 0.6)),
        "breakout_hold_days": int(o.get("breakout_hold_days", 0)),
        "breakout_hold_ratio": float(o.get("breakout_hold_ratio", 0.98)),
        "pool_4_momentum_enabled": bool(o.get("pool_4_momentum_enabled", False)),
        "momentum_percentile_threshold": float(o.get("momentum_percentile_threshold", 0.80)),
        "fusion_mode": str(o.get("fusion_mode", "weighted")).strip().lower(),
        "fusion_weights": [float(fw[0]), float(fw[1]), float(fw[2])],
        "multi_pool_tier2_enabled": bool(o.get("multi_pool_tier2_enabled", False)),
        "multi_pool_tier2_threshold": float(o.get("multi_pool_tier2_threshold", 60)),
        "multi_pool_tier2_bonus": float(o.get("multi_pool_tier2_bonus", 5)),
        "output_score_percentile": bool(o.get("output_score_percentile", True)),
    }


def get_long_term_params(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """B 轨长期价值发现：长期动量分位与候选阈值。见 06_B轨需求与实现缺口分析。"""
    if config is None:
        config = load_scanner_config()
    engine = config.get("module_b_quant_engine") or {}
    lt = engine.get("long_term") or {}
    return {
        "enabled": bool(lt.get("enabled", True)),
        "lookback_days": int(lt.get("lookback_days", 120)),
        "score_threshold": float(lt.get("score_threshold", 0.6)),
    }


def get_filters_params(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """过滤器：板块强度、流动性、波动率 regime；均可选。"""
    if config is None:
        config = load_scanner_config()
    engine = config.get("module_b_quant_engine") or {}
    f = engine.get("filters") or {}
    sector = f.get("sector_strength") or {}
    liq = f.get("liquidity") or {}
    vol = f.get("volatility_regime") or {}
    return {
        "sector_strength": {"enabled": bool(sector.get("enabled", False))},
        "liquidity": {
            "enabled": bool(liq.get("enabled", False)),
            "min_score": float(liq.get("min_score", 0)),
            "as_filter": bool(liq.get("as_filter", True)),
        },
        "volatility_regime": {
            "enabled": bool(vol.get("enabled", False)),
            "percentile_threshold": float(vol.get("percentile_threshold", 0.90)),
            "penalty_ratio": float(vol.get("penalty_ratio", 0.8)),
        },
    }
