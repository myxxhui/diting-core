# [Ref: 02_B模块策略_策略实现规约] [Ref: 03_A轨_量化扫描引擎_设计] 单次扫描可观测性与性能指标（结构化）

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class ScannerRunMetrics:
    """单次 scan_market 各阶段耗时与规模，供日志、last_scan_metrics、运维采集。"""

    universe_in: int = 0
    symbols_ohlcv_ok: int = 0
    symbols_scored: int = 0
    symbols_out: int = 0
    parallel_workers_used: int = 0
    ms_fetch_batch_ohlcv: float = 0.0
    ms_percentile_ranks: float = 0.0
    ms_l2_precheck: float = 0.0
    ms_evaluate_pools: float = 0.0
    ms_sector_strength: float = 0.0
    ms_build_output: float = 0.0
    ms_total: float = 0.0
    skipped_coarse: int = 0
    skipped_cooldown: int = 0
    skipped_classifier: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "universe_in": self.universe_in,
            "symbols_ohlcv_ok": self.symbols_ohlcv_ok,
            "symbols_scored": self.symbols_scored,
            "symbols_out": self.symbols_out,
            "parallel_workers_used": self.parallel_workers_used,
            "ms_fetch_batch_ohlcv": round(self.ms_fetch_batch_ohlcv, 2),
            "ms_percentile_ranks": round(self.ms_percentile_ranks, 2),
            "ms_l2_precheck": round(self.ms_l2_precheck, 2),
            "ms_evaluate_pools": round(self.ms_evaluate_pools, 2),
            "ms_sector_strength": round(self.ms_sector_strength, 2),
            "ms_build_output": round(self.ms_build_output, 2),
            "ms_total": round(self.ms_total, 2),
            "skipped_coarse": self.skipped_coarse,
            "skipped_cooldown": self.skipped_cooldown,
            "skipped_classifier": self.skipped_classifier,
        }
        if self.extra:
            d["extra"] = self.extra
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)


class PhaseTimer:
    """with PhaseTimer() as t: ...; t.elapsed_ms"""

    def __init__(self):
        self._t0 = 0.0
        self.elapsed_ms = 0.0

    def __enter__(self) -> "PhaseTimer":
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        self.elapsed_ms = (time.perf_counter() - self._t0) * 1000.0
