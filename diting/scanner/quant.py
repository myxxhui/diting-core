# [Ref: 03_原子目标与规约/_共享规约/09_核心模块架构规约] [Ref: 11_数据采集与输入层规约]
# [Ref: 02_量化扫描引擎_实践] [Ref: 02_量化扫描引擎_策略实现规约]
# Module B 量化扫描引擎：TA-Lib 指标 + 三大策略池 + 阈值过滤；产出技术面得分 > 阈值的候选池

import logging
from typing import Any, List, Optional

from diting.scanner.config_loader import load_scanner_config, get_thresholds
from diting.scanner.ohlcv_feed import get_ohlcv_arrays_for_talib
from diting.scanner.pools import evaluate_pools, POOL_TREND, POOL_REVERSION, POOL_BREAKOUT

logger = logging.getLogger(__name__)


class QuantScanner:
    """
    量化扫描引擎：对 universe 全量扫描，用 TA-Lib 计算指标、三池判定，输出 technical_score >= 阈值的候选。
    标的池由 get_current_a_share_universe() 或调用方传入，同批与 Module A 使用同一列表。
    """

    def __init__(self, config_path: Optional[str] = None):
        self._config = load_scanner_config(config_path)
        self._score_threshold, self._sector_threshold = get_thresholds(self._config)

    def scan_market(
        self,
        universe: List[str],
        ohlcv_dsn: Optional[str] = None,
        correlation_id: str = "",
        return_all: bool = True,
    ) -> List[Any]:
        """
        扫描全市场：对每标取 OHLCV → TA-Lib 指标 → 三池得分；按阈值标记通过/未通过，全部输出并保存。
        :param universe: 当前全 A 股标的池
        :param ohlcv_dsn: L1 连接串（须配置 TIMESCALE_DSN 使用采集生产数据）
        :param correlation_id: 全链路请求 ID
        :param return_all: True 时返回全部结果（含 passed 标记）；False 时仅返回通过的列表（兼容旧调用）
        :return: 每项具 symbol, technical_score, strategy_source, sector_strength, correlation_id, passed
        """
        logger.info("QuantScanner.scan_market: len(universe)=%s, score_threshold=%s", len(universe), self._score_threshold)
        out = []
        for sym in universe or []:
            arr = get_ohlcv_arrays_for_talib(sym, period="daily", limit=120, dsn=ohlcv_dsn)
            if not arr or len(arr[0]) < 30:
                continue
            o, h, l, c, v = arr
            score, pool_id = evaluate_pools(o, h, l, c, v)
            sector_strength = 1.0  # 本阶段无板块信息，固定 1.0
            passed = score >= self._score_threshold and sector_strength >= self._sector_threshold
            out.append({
                "symbol": sym,
                "technical_score": float(score),
                "strategy_source": pool_id,
                "sector_strength": sector_strength,
                "correlation_id": correlation_id,
                "passed": passed,
            })
        n_passed = sum(1 for x in out if x.get("passed"))
        logger.info("QuantScanner.scan_market: 全量=%s, 通过阈值=%s", len(out), n_passed)
        if return_all:
            return out
        return [x for x in out if x.get("passed")]

    @classmethod
    def run_full(
        cls,
        universe: Optional[List[str]] = None,
        ohlcv_dsn: Optional[str] = None,
        correlation_id: str = "",
    ) -> List[Any]:
        """
        执行入口：先通过 get_current_a_share_universe() 或环境变量获取标的池，再全量扫描。
        """
        if universe is None:
            from diting.universe import get_current_a_share_universe, parse_symbol_list_from_env
            universe = parse_symbol_list_from_env("DITING_SYMBOLS") or parse_symbol_list_from_env("MODULE_AB_SYMBOLS")
            if not universe:
                universe = get_current_a_share_universe()
        logger.info("QuantScanner.run_full: len(universe)=%s", len(universe))
        return cls().scan_market(universe, ohlcv_dsn=ohlcv_dsn, correlation_id=correlation_id)
