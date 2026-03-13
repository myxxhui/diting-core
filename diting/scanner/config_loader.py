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
