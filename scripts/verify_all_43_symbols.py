#!/usr/bin/env python3
# 43 标的全量检测与规则修正辅助脚本
# 对比每只标的：A 标签（vertical） vs 主营披露（segment disclosure），输出修正建议。
# 用法：PG_L2_DSN 已配时，python scripts/verify_all_43_symbols.py

import os
import sys
from typing import Any, Dict, List, Optional, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)


def main() -> int:
    from diting.classifier.business_segment_provider import (
        get_segment_labels_and_shares_batch,
        get_top_segment_disclosure_batch,
    )
    from diting.classifier.snapshot_reader import (
        fetch_latest_classifier_batch_id,
        fetch_snapshot_rows_batch,
        resolve_moe_classifier_batch_id,
    )
    from diting.universe import parse_symbol_list_from_env

    universe = parse_symbol_list_from_env("DITING_SYMBOLS") or parse_symbol_list_from_env("MODULE_AB_SYMBOLS")
    if not universe:
        from diting.classifier.run import _default_universe_from_diting_symbols
        universe = _default_universe_from_diting_symbols()
    if not universe:
        print("错误: 未获取到标的列表", file=sys.stderr)
        return 1

    pg_l2 = (os.environ.get("PG_L2_DSN") or "").strip()
    if not pg_l2:
        print("错误: 需配置 PG_L2_DSN", file=sys.stderr)
        return 1

    syms = [s.strip() for s in universe if (s or "").strip()]
    syms = list(dict.fromkeys(syms))[:50]  # 去重并限制 50，实际 run-module-c 可能更少

    # 拉取 A 快照（最近一批）
    batch_id = resolve_moe_classifier_batch_id(fetch_latest_classifier_batch_id(pg_l2))
    if not batch_id:
        print("错误: L2 无 classifier_output_snapshot，请先运行 make run-module-a", file=sys.stderr)
        return 1

    rows_map = fetch_snapshot_rows_batch(pg_l2, syms, batch_id)
    if not rows_map:
        print("错误: 快照无数据", file=sys.stderr)
        return 1
    rows = [{"symbol": k, **v} for k, v in rows_map.items()]

    seg_labels = get_segment_labels_and_shares_batch(pg_l2, syms, 5)
    seg_top1 = get_top_segment_disclosure_batch(pg_l2, syms)

    # 从快照解析 domain_tags / vertical
    from diting.classifier.snapshot_reader import domain_tags_zh_from_tags_json
    from diting.moe.router import resolve_router_domain_tag

    report: List[Dict[str, Any]] = []
    for r in rows:
        sym = (r.get("symbol") or "").strip().upper()
        if not sym:
            continue
        tags_json = r.get("tags_json") or "[]"
        domain_tags = domain_tags_zh_from_tags_json(tags_json) or []
        bucket = resolve_router_domain_tag(domain_tags, None) or "未知"
        vertical = [t for t in domain_tags if t not in ("农业", "科技", "宏观", "未知")][:3]

        # 披露侧：主营 Top3
        rows_s = seg_labels.get(sym, [])
        disclosure_top3 = [l for l, _ in rows_s[:3]]
        top1_name = None
        if seg_top1:
            row = seg_top1.get(sym)
            if row:
                top1_name = (row[0] or "").strip() if row else None

        match = "✓"
        if disclosure_top3 and vertical:
            # 主营首标签应与 vertical 首标签一致或包含
            d1 = (disclosure_top3[0] or "").strip()
            v1 = (vertical[0] or "").strip()
            if d1 and v1 and d1 != v1:
                # 检查是否为子类关系（如 电力 vs 火电）
                if d1 not in v1 and v1 not in d1:
                    match = "需核"
                else:
                    match = "近似"
        elif not disclosure_top3 and vertical:
            match = "无披露"

        report.append({
            "symbol": sym,
            "a_vertical": ",".join(vertical) if vertical else "-",
            "a_bucket": bucket,
            "disclosure_top3": ",".join(disclosure_top3) if disclosure_top3 else "-",
            "top1_raw": top1_name or "-",
            "match": match,
        })

    # 表头
    w = {"symbol": 12, "a_vertical": 28, "a_bucket": 6, "disclosure": 28, "top1_raw": 20, "match": 6}
    hdr = f"{'标的':<12} {'A垂直':<28} {'大类':<6} {'披露Top3':<28} {'披露首名':<20} {'核':<6}"
    print(hdr)
    print("-" * 100)
    for r in report:
        print(f"{r['symbol']:<12} {r['a_vertical'][:26]:<28} {r['a_bucket']:<6} {r['disclosure_top3'][:26]:<28} {(r['top1_raw'] or '-')[:18]:<20} {r['match']:<6}")

    # 需核 / 无披露 的标统计
    need_review = [x for x in report if x["match"] in ("需核", "无披露")]
    print("\n需核对数量: %d" % len(need_review))
    if need_review:
        print("需核对标的:", ", ".join(x["symbol"] for x in need_review))

    # 规则补全建议（基于披露首名未命中 refine 的情况）
    from diting.classifier.semantic import refine_power_label_from_disclosure
    unmapped: List[Tuple[str, str]] = []
    for r in report:
        raw = (r.get("top1_raw") or "").strip()
        if raw and raw != "-":
            out = refine_power_label_from_disclosure(raw)
            if out is None:
                unmapped.append((r["symbol"], raw))
    if unmapped:
        print("\n披露首名未映射（建议补全 refine_power_label_from_disclosure）:")
        for sym, raw in unmapped[:15]:
            print("  %s: %s" % (sym, raw))

    return 0


if __name__ == "__main__":
    sys.exit(main())
