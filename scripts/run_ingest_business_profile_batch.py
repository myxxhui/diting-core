#!/usr/bin/env python3
# [Ref: 11_数据采集与输入层规约] [Ref: 12_右脑数据支撑与Segment规约]
# 批量采集主营构成 → L2 symbol_business_profile；供 Module A 申万「电力」等按披露细分。
# 工作目录: diting-core。用法: make ingest-business-profile 或 PYTHONPATH=. python3 scripts/run_ingest_business_profile_batch.py
# 依赖: make deps-ingest、PG_L2_DSN、make init-l2-business-profile-tables；标的来自 DITING_SYMBOLS 或 config/diting_symbols.txt

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

_env = ROOT / ".env"
if _env.exists():
    with open(_env, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and os.environ.get(k) is None:
                    os.environ[k] = v


def main() -> int:
    from diting.ingestion.business_profile import run_ingest_business_profile_batch
    from diting.universe import parse_symbol_list_from_env

    universe = parse_symbol_list_from_env("DITING_SYMBOLS") or parse_symbol_list_from_env("MODULE_AB_SYMBOLS")
    if not universe:
        from diting.classifier.run import _default_universe_from_diting_symbols

        universe = _default_universe_from_diting_symbols()
    if not universe:
        print(
            "错误: 未获取到标的列表（请配置 DITING_SYMBOLS 或保证 config/diting_symbols.txt 存在且非空）",
            file=sys.stderr,
        )
        return 1

    ok, zero, rows = run_ingest_business_profile_batch(universe)
    print(
        "ingest-business-profile: 标的=%s 写入成功=%s 无数据或失败=%s 累计分部行=%s"
        % (len(universe), ok, zero, rows)
    )
    return 0 if ok > 0 or rows > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
