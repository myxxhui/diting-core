#!/usr/bin/env python3
# [Ref: 01_语义分类器_实践] 一键本地运行 A 模块：输出执行标的、执行结果、写入位置
# 用法：在 diting-core 根目录 make run-module-a 或 PYTHONPATH=. python3 scripts/run_module_a_local.py

import os
import sys
import uuid
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

_DOMAIN_TAG_NAMES = {0: "未指定", 1: "农业", 2: "科技", 3: "宏观", 4: "未知", 5: ""}


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
        print("错误: 未获取到标的列表（请配置 DITING_SYMBOLS 或保证 config/diting_symbols.txt 存在且非空）", file=sys.stderr)
        sys.exit(1)

    # 与 run.py 一致：L2 + industry_fallback
    industry_provider = None
    if os.environ.get("PG_L2_DSN") and universe:
        try:
            from diting.classifier.l2_provider import get_l2_industry_revenue_batch
            from diting.ingestion.industry_revenue import _load_industry_fallback
            l2_data = get_l2_industry_revenue_batch(os.environ["PG_L2_DSN"], universe)
            missing = ("未知", 0.0, 0.0, 0.0)
            merged = {}
            for s in universe:
                key = (s or "").strip().upper()
                t = l2_data.get(key, ("", 0.0, 0.0, 0.0))
                if (t[0] or "").strip():
                    merged[key] = t
                else:
                    iname = _load_industry_fallback(s) or "未知"
                    merged[key] = (iname, float(t[1] or 0), float(t[2] or 0), float(t[3] or 0))
            industry_provider = lambda sym, m=merged, mis=missing: m.get((sym or "").strip().upper(), mis)
        except Exception:
            pass

    clf = SemanticClassifier(rules=load_rules(), industry_revenue_provider=industry_provider)
    batch_id = str(uuid.uuid4())
    results = clf.classify_batch(universe, correlation_id=batch_id)

    print()
    print("======== 执行标的（共 %s 只，来源: DITING_SYMBOLS / config/diting_symbols.txt）========  " % len(universe))
    for i, s in enumerate(universe, 1):
        print("  %s. %s" % (i, s))
    print()

    print("======== 执行结果（symbol -> 主 Tag，置信度）========  ")
    print("  说明: 置信度 = 分类器对该主 Tag 的确信程度，0~1 之间，越高越确信属于该类别。")
    for out in results:
        tag_strs = []
        for t in out.tags:
            name = getattr(t, "domain_label", None) or _DOMAIN_TAG_NAMES.get(getattr(t, "domain_tag", 4), str(getattr(t, "domain_tag", 4)))
            if name:
                tag_strs.append(name)
        conf = out.tags[0].confidence if out.tags else 0.0
        print("  %s -> %s（置信度 %.2f）" % (out.symbol, "、".join(tag_strs) or "未知", conf))
    print()

    write_location = "未写入"
    n_written = 0
    if results and os.environ.get("PG_L2_DSN"):
        try:
            n_written = write_classifier_output_snapshot(
                os.environ["PG_L2_DSN"], results, batch_id=batch_id, correlation_id=batch_id
            )
            if n_written > 0:
                write_location = "L2 表 classifier_output_snapshot（库: PG_L2_DSN），batch_id=%s，行数=%s" % (batch_id, n_written)
            else:
                write_location = "L2 写入未成功（表可能未创建或连接失败），batch_id=%s" % batch_id
        except Exception as e:
            write_location = "L2 写入失败: %s" % e
    else:
        write_location = "未写入（未配置 PG_L2_DSN 或无结果）"

    print("======== 写入 ========  ")
    print("  %s" % write_location)
    print()
    sys.exit(0)


if __name__ == "__main__":
    main()
