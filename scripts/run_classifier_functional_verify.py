#!/usr/bin/env python3
# [Ref: 01_语义分类器_实践] A 模块功能验证：本地连库跑分类逻辑，输出可判定结果（是否对 27 只执行、逻辑结果、是否符合预期）
# 执行顺序：先在本机加载 .env（含 PG_L2_DSN）并运行本脚本；跑通且结果符合预期后，再执行 make build-module-a

import os
import sys
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)

# 加载 .env
_env = Path(ROOT) / ".env"
if _env.exists():
    with open(_env, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and os.environ.get(k) is None:
                    os.environ[k] = v


def main():
    from diting.classifier import SemanticClassifier
    from diting.classifier.semantic import load_rules
    from diting.classifier.l2_snapshot_writer import write_classifier_output_snapshot
    from diting.universe import parse_symbol_list_from_env

    universe = parse_symbol_list_from_env("DITING_SYMBOLS") or parse_symbol_list_from_env("MODULE_AB_SYMBOLS")
    if not universe:
        from diting.classifier.run import _default_universe_from_diting_symbols
        universe = _default_universe_from_diting_symbols()
    if not universe:
        print("FAIL: 未获取到标的列表（请配置 DITING_SYMBOLS 或保证 config/diting_symbols.txt 存在且非空）")
        sys.exit(1)

    # 与 run.py 一致：L2 + industry_fallback + 主营 segment
    industry_provider = None
    business_segment_provider = None
    segment_top1_name_provider = None
    segment_disclosure_names_provider = None
    if os.environ.get("PG_L2_DSN") and universe:
        try:
            from diting.classifier.business_segment_provider import (
                get_segment_disclosure_names_batch,
                get_top_segment_disclosure_batch,
                make_business_segment_provider,
            )
            from diting.classifier.l2_provider import get_l2_industry_revenue_batch
            from diting.ingestion.industry_revenue import (
                _load_industry_fallback,
                industry_name_needs_fallback,
            )
            l2_data = get_l2_industry_revenue_batch(os.environ["PG_L2_DSN"], universe)
            missing = ("未知", 0.0, 0.0, 0.0)
            merged = {}
            for s in universe:
                key = (s or "").strip().upper()
                t = l2_data.get(key, ("", 0.0, 0.0, 0.0))
                if not industry_name_needs_fallback(t[0]):
                    merged[key] = t
                else:
                    iname = _load_industry_fallback(s) or "未知"
                    merged[key] = (iname, float(t[1] or 0), float(t[2] or 0), float(t[3] or 0))
            industry_provider = lambda sym, m=merged, mis=missing: m.get((sym or "").strip().upper(), mis)
            business_segment_provider = make_business_segment_provider(os.environ["PG_L2_DSN"], universe)
            _disc = get_top_segment_disclosure_batch(os.environ["PG_L2_DSN"], universe)
            _names_by_sym = get_segment_disclosure_names_batch(os.environ["PG_L2_DSN"], universe)

            def _top1_name(sym: str):
                row = _disc.get((sym or "").strip().upper())
                if not row:
                    return None
                n = (row[0] or "").strip()
                return n or None

            def _segment_disclosure_names(sym: str):
                return _names_by_sym.get((sym or "").strip().upper(), [])

            segment_top1_name_provider = _top1_name
            segment_disclosure_names_provider = _segment_disclosure_names
        except Exception:
            pass

    clf_kw = dict(
        rules=load_rules(),
        industry_revenue_provider=industry_provider,
        business_segment_provider=business_segment_provider,
    )
    if segment_top1_name_provider is not None:
        clf_kw["segment_top1_name_provider"] = segment_top1_name_provider
    if segment_disclosure_names_provider is not None:
        clf_kw["segment_disclosure_names_provider"] = segment_disclosure_names_provider
    clf = SemanticClassifier(**clf_kw)
    results = clf.classify_batch(universe, correlation_id="verify-batch")
    n_out = len(results)
    confidence_ok = True
    tag_names = set()
    for out in results:
        for t in out.tags:
            c = getattr(t, "confidence", 0.0)
            if not (0 <= c <= 1.0):
                confidence_ok = False
            tag_val = getattr(t, "domain_tag", 4)
            label = getattr(t, "domain_label", None) or ""
            if tag_val == 5 and label:
                tag_names.add(label)
            else:
                for name, val in [("农业", 1), ("科技", 2), ("宏观", 3), ("未知", 4)]:
                    if tag_val == val:
                        tag_names.add(name)
                        break

    l2_written = 0
    if results and os.environ.get("PG_L2_DSN"):
        try:
            l2_written = write_classifier_output_snapshot(
                os.environ["PG_L2_DSN"], results, batch_id="verify-batch", correlation_id="verify-batch"
            )
        except Exception:
            pass

    print()
    print("=" * 60)
    print("A 模块功能验证摘要")
    print("=" * 60)
    print("1. 是否对 27 个标的执行: 执行标的数 = %s（预期: 与 diting_symbols.txt 一致）" % len(universe))
    print("2. 逻辑结果: 输出条数 = %s，置信度均在 [0,1] = %s，涉及 Tag = %s" % (n_out, confidence_ok, sorted(tag_names)))
    print("3. L2 写入: 本批写入 classifier_output_snapshot 行数 = %s（PG_L2_DSN 可达时为 %s）" % (l2_written, n_out))
    expect_run_ok = n_out == len(universe) and n_out >= 1 and confidence_ok
    print("4. 是否符合预期: 执行数=输出数且置信度合法 = %s" % expect_run_ok)
    print("=" * 60)
    if not expect_run_ok:
        sys.exit(1)
    print("PASS: 功能验证通过；可执行 make build-module-a 构建镜像。")


if __name__ == "__main__":
    main()
