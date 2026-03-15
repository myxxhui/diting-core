#!/usr/bin/env python3
# [Ref: 01_语义分类器_实践] 产出可验证结果：Module A 分类输出 + 写入 L2 的样本行
# 默认按 config/diting_symbols.txt 全部标的执行（与采集模块一致），为生产数据；供实践文档「已完成事项与执行结果」填写

import json
import os
import sys
from pathlib import Path

# 确保从 diting-core 根目录执行
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)


def load_universe_from_diting_symbols() -> list:
    """与 run.py / 采集模块一致：读取 config/diting_symbols.txt 全部标的。"""
    path = Path(ROOT) / "config" / "diting_symbols.txt"
    if not path.exists():
        return []
    from diting.universe import normalize_symbol
    symbols = []
    seen = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip().split("#")[0].strip()
            if line:
                sym = normalize_symbol(line)
                if sym and sym not in seen:
                    seen.add(sym)
                    symbols.append(sym)
    return symbols


def main():
    from diting.classifier import SemanticClassifier
    from diting.classifier.semantic import load_rules
    from diting.classifier.l2_snapshot_writer import _output_to_row

    # 默认按 diting_symbols.txt 全部标的（与采集模块一致，生产数据）
    universe = load_universe_from_diting_symbols()
    if not universe:
        universe = ["000998.SZ", "688981.SH", "601899.SH", "999999.SZ"]
        print("未找到 config/diting_symbols.txt，使用 fallback 4 只", file=sys.stderr)

    clf = SemanticClassifier(rules=load_rules())
    batch_id = "verify-stage3-01-batch"
    correlation_id = batch_id

    print("=" * 60)
    print("Module A 语义分类器 — 分类输出详情（ClassifierOutput）")
    print("标的来源: config/diting_symbols.txt，共 %s 只（与采集模块一致）" % len(universe))
    print("=" * 60)

    results = []
    for symbol in universe:
        out = clf.classify(symbol, correlation_id=correlation_id)
        results.append(out)
        tag_strs = []
        for t in out.tags:
            tag_val = getattr(t, "domain_tag", 4)
            label = getattr(t, "domain_label", None) or ""
            name = {"0": "未指定", "1": "农业", "2": "科技", "3": "宏观", "4": "未知", "5": "自定义"}.get(
                str(tag_val), str(tag_val)
            )
            if tag_val == 5 and label:
                name = label
            conf = getattr(t, "confidence", 0.0)
            tag_strs.append(f"{name}({conf:.2f})")
        print(f"  {out.symbol} -> tags: {', '.join(tag_strs)}")

    print()
    print("=" * 60)
    print("写入 L2 表 classifier_output_snapshot 的数据详情（本批将写入的行）")
    print("表: classifier_output_snapshot | 供 Module B 按 batch_id 读取")
    print("=" * 60)

    for out in results:
        row = _output_to_row(out, batch_id, correlation_id)
        # row: (batch_id, symbol, primary_tag, primary_confidence, tags_json, correlation_id)
        print(f"  symbol={row[1]}, primary_tag={row[2]}, primary_confidence={row[3]:.2f}, batch_id={row[0]}")
        if row[4]:
            tags_json = json.loads(row[4])
            print(f"    tags_json: {json.dumps(tags_json, ensure_ascii=False)}")

    print()
    print("判定：上述为 config/diting_symbols.txt 全部标的的生产数据分类结果；L2 可用时来自库内行业/营收，否则为 Mock/fallback。")


if __name__ == "__main__":
    main()
