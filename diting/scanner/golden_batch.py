# [Ref: 02_B模块策略_策略实现规约 §3.12] Golden batch：固定标的 + 分数区间，防池子/YAML 静默漂移

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


def load_fixture(path: Union[str, Path]) -> Dict[str, Any]:
    p = Path(path)
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def validate_golden_scanner_batch(fixture: Dict[str, Any]) -> List[str]:
    """
    对 fixture 执行一次 QuantScanner（mock OHLCV，ohlcv_dsn=None）。
    调用方须在 **无 PG_L2_DSN/TIMESCALE_DSN** 且 **PYTHONHASHSEED=0** 的环境下执行，以保证 mock K 线哈希稳定。
    返回错误信息列表；空列表表示通过。
    """
    from diting.scanner.quant import QuantScanner

    errors: List[str] = []
    universe = fixture.get("universe") or []
    if not universe:
        return ["fixture.universe 为空"]

    scanner = QuantScanner()
    out = scanner.scan_market(list(universe), ohlcv_dsn=None, return_all=True)
    by_sym = {str(x.get("symbol", "")).strip().upper(): x for x in out}

    sym_rules: Dict[str, Any] = fixture.get("symbols") or {}
    for sym, rules in sym_rules.items():
        u = str(sym).strip().upper()
        row = by_sym.get(u)
        if not row:
            errors.append("缺少输出标的: %s" % u)
            continue
        ts = float(row.get("technical_score") or 0.0)
        tmin = rules.get("technical_score_min")
        tmax = rules.get("technical_score_max")
        if tmin is not None and ts < float(tmin):
            errors.append("%s technical_score=%.6f 低于下限 %.6f" % (u, ts, float(tmin)))
        if tmax is not None and ts > float(tmax):
            errors.append("%s technical_score=%.6f 高于上限 %.6f" % (u, ts, float(tmax)))

        if "sector_strength" in rules:
            exp_ss = float(rules["sector_strength"])
            got_ss = float(row.get("sector_strength") or 0.0)
            if abs(got_ss - exp_ss) > 1e-6:
                errors.append("%s sector_strength 期望 %s 实际 %s" % (u, exp_ss, got_ss))

        if "industry_mapped" in rules:
            exp_im = rules["industry_mapped"]
            got_im = row.get("industry_mapped")
            if exp_im is None and got_im is not None:
                errors.append("%s industry_mapped 期望 None 实际 %s" % (u, got_im))
            elif exp_im is not None and got_im != exp_im:
                errors.append("%s industry_mapped 期望 %s 实际 %s" % (u, exp_im, got_im))

        if "strategy_source" in rules:
            exp_src = rules["strategy_source"]
            got_src = row.get("strategy_source")
            if got_src != exp_src:
                errors.append("%s strategy_source 期望 %s 实际 %s" % (u, exp_src, got_src))

    me = fixture.get("metrics_extra") or {}
    ex = (scanner.last_scan_metrics or {}).get("extra") or {}
    for k, v in me.items():
        if ex.get(k) != v:
            errors.append("metrics.extra[%s] 期望 %s 实际 %s" % (k, v, ex.get(k)))

    fp_exp = fixture.get("scanner_rules_fingerprint_prefix")
    if fp_exp:
        fp = str(ex.get("scanner_rules_fingerprint") or "")
        if not fp.startswith(str(fp_exp)):
            errors.append(
                "scanner_rules_fingerprint 期望前缀 %s 实际 %s" % (fp_exp, fp or "(空)")
            )

    return errors


def validate_fixture_file(path: Union[str, Path]) -> List[str]:
    return validate_golden_scanner_batch(load_fixture(path))
