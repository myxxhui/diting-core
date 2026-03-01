#!/usr/bin/env python3
# [Ref: 06_生产级数据要求_设计.md, 11_数据采集与输入层规约]
# 生产级数据插入与质量报告：对照设计检查数量与 AB 模块预期，仅输出报告不改退出码。

import os
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
env_file = root / ".env"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

try:
    import psycopg2
except ImportError:
    print("psycopg2 not installed", file=sys.stderr)
    sys.exit(1)

TIMESCALE_DSN = os.environ.get("TIMESCALE_DSN")
PG_L2_DSN = os.environ.get("PG_L2_DSN")
MIN_BARS_5_YEARS = 5 * 252  # 设计：单标日线 ≥5 年


def section(title: str) -> None:
    print()
    print("=" * 60)
    print(title)
    print("=" * 60)


def main() -> int:
    if not TIMESCALE_DSN:
        print("TIMESCALE_DSN 未设置，请配置 .env", file=sys.stderr)
        return 1

    # ----- L1 TimescaleDB -----
    section("1. 生产级数据是否已全部插入（L1）")
    try:
        conn_l1 = psycopg2.connect(TIMESCALE_DSN)
        conn_l1.autocommit = True
    except Exception as e:
        print(f"L1 连接失败: {e}", file=sys.stderr)
        return 1

    with conn_l1.cursor() as cur:
        # 表存在性
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name IN ('ohlcv', 'a_share_universe')"
        )
        tables = {r[0] for r in cur.fetchall()}
        print(f"  L1 表存在: ohlcv={('ohlcv' in tables)}, a_share_universe={('a_share_universe' in tables)}")

        if "ohlcv" not in tables:
            print("  ❌ 缺少 ohlcv 表，生产级 OHLCV 未插入")
        else:
            cur.execute("SELECT COUNT(*) FROM ohlcv")
            total_ohlcv = cur.fetchone()[0]
            cur.execute(
                "SELECT symbol, COUNT(*) AS cnt FROM ohlcv GROUP BY symbol ORDER BY symbol"
            )
            rows = cur.fetchall()
            symbols_ohlcv = [r[0] for r in rows]
            min_bars = min(r[1] for r in rows) if rows else 0
            max_bars = max(r[1] for r in rows) if rows else 0
            print(f"  ohlcv: 总行数={total_ohlcv}, 标的数={len(symbols_ohlcv)}, 单标最小 bar={min_bars}, 单标最大 bar={max_bars}")
            if min_bars >= MIN_BARS_5_YEARS:
                print(f"  ✅ 单标日线 ≥5 年（≥{MIN_BARS_5_YEARS} bar）满足设计")
            else:
                print(f"  ⚠️ 单标日线不足 5 年：最小 {min_bars} < {MIN_BARS_5_YEARS}（设计要求）")

        if "a_share_universe" not in tables:
            print("  ⚠️ 缺少 a_share_universe 表（get_current_a_share_universe 需此表）")
        else:
            cur.execute("SELECT COUNT(*) FROM a_share_universe")
            universe_count = cur.fetchone()[0]
            print(f"  a_share_universe: 标的数={universe_count}")
            if tables >= {"ohlcv", "a_share_universe"} and universe_count > 0 and len(symbols_ohlcv) < universe_count:
                print(f"  ⚠️ ohlcv 标的数({len(symbols_ohlcv)}) < universe 标的数({universe_count})，未全覆盖")
            elif tables >= {"ohlcv", "a_share_universe"} and universe_count > 0:
                print("  ✅ universe 与 ohlcv 标的覆盖一致（满足同批一致）")

    # 汇总 L1 用于 Module B 结论
    total, n_sym, min_b = 0, 0, 0
    if "ohlcv" in tables:
        with conn_l1.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ohlcv")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT symbol) FROM ohlcv")
            n_sym = cur.fetchone()[0]
            cur.execute(
                "SELECT MIN(cnt) FROM (SELECT COUNT(*) AS cnt FROM ohlcv GROUP BY symbol) t"
            )
            min_b = cur.fetchone()[0] or 0
    conn_l1.close()

    # ----- L2 PostgreSQL（Module A 输入：行业/营收、data_versions）-----
    section("2. 数据数量与质量是否符合 AB 模块生产级预期")
    print("  设计预期（06_/11_）：")
    print("    - Module B：L1 OHLCV 单标日线 ≥5 年、标的 = universe、复权与回测/实盘一致")
    print("    - Module A：申万层级/营收占比可从约定表或 L2 获取；板块与 B 同源（申万）")
    print("    - 标的覆盖：与 get_current_a_share_universe() 一致")

    print()
    print("  Module B（量化扫描）预期满足情况：")
    print(f"    - L1 OHLCV 已有：总行数 {total}，标的数 {n_sym}，单标最小 bar {min_b}")
    if min_b >= MIN_BARS_5_YEARS and n_sym > 0:
        print("    - ✅ 历史深度与标的覆盖可支撑 B 全量扫描（单标≥5 年、universe 可读）")
    else:
        print(f"    - ⚠️ 需补全：单标≥{MIN_BARS_5_YEARS} bar 或更多标的")

    if PG_L2_DSN:
        try:
            conn_l2 = psycopg2.connect(PG_L2_DSN)
            conn_l2.autocommit = True
            with conn_l2.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = 'data_versions'"
                )
                if cur.fetchone():
                    cur.execute(
                        "SELECT data_type, COUNT(*) FROM data_versions GROUP BY data_type"
                    )
                    type_rows = cur.fetchall()
                    print()
                    print("  Module A（语义分类）输入数据（L2 data_versions）：")
                    for dt, cnt in type_rows:
                        print(f"    - {dt}: {cnt} 条")
                    if any(dt == "industry_revenue" for dt, _ in type_rows):
                        print("    - ✅ 存在 industry_revenue，可支撑 Module A 申万/营收维度")
                    else:
                        print("    - ⚠️ 无 industry_revenue，Module A 需 ingest_industry_revenue 或 Mock")
                else:
                    print("  L2 无 data_versions 表，Module A 行业/营收数据未落库")
            conn_l2.close()
        except Exception as e:
            print(f"  L2 连接/查询失败: {e}")
    else:
        print("  PG_L2_DSN 未设置，未检查 L2/Module A 数据")

    section("3. 结论")
    print("  运行 make verify-data-production 可做通过/不通过验收。")
    print("  本报告仅做数量与设计对照，复权口径需与 Stage4 回测/实盘配置对齐后另行核对。")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
