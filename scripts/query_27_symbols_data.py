#!/usr/bin/env python3
# 连接数据库，查询指定 27 标的的 K 线、行业财务、新闻公告数据并输出结果。
# 工作目录: diting-core；DSN 与 ingest 一致（config）。

import os
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
env_file = root / ".env"
if env_file.exists():
    with open(env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and os.environ.get(k) is None:
                    os.environ[k] = v

sys.path.insert(0, str(root))
from diting.ingestion.config import get_timescale_dsn, get_pg_l2_dsn


def load_27_symbols():
    raw = os.environ.get("DITING_SYMBOLS", "config/diting_symbols.txt").strip()
    path = root / raw if not os.path.isabs(raw) else Path(raw)
    if not path.exists():
        path = root / "config" / "diting_symbols.txt"
    symbols = []
    if path.exists():
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip().split("#")[0].strip()
                if line:
                    symbols.append(line.upper())
    return symbols


def main():
    import psycopg2
    symbols = load_27_symbols()
    if not symbols:
        print("未找到 27 标的列表（DITING_SYMBOLS / config/diting_symbols.txt）", file=sys.stderr)
        return 1
    print("目标标的数:", len(symbols))
    print("标的列表:", ", ".join(symbols[:5]), "..." if len(symbols) > 5 else "")
    print()

    dsn_l1 = get_timescale_dsn()
    dsn_l2 = get_pg_l2_dsn()

    # ---------- L1：27 标的 K 线（ohlcv）----------
    print("========== 1. L1 表 ohlcv：27 标的 K 线 ==========")
    try:
        conn = psycopg2.connect(dsn_l1)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol, COUNT(*) AS cnt,
                   MIN(datetime)::date AS min_dt, MAX(datetime)::date AS max_dt
            FROM ohlcv
            WHERE symbol = ANY(%s) AND (period = %s OR period = %s)
            GROUP BY symbol ORDER BY symbol
            """,
            (symbols, "daily", "day"),
        )
        rows = cur.fetchall()
        conn.close()
        print("查询条件: symbol IN (27 标), period='day' 或 'daily'（与写入一致）")
        print("有 K 线数据的标的数:", len(rows), "/ 27")
        if rows:
            total_bars = sum(r[1] for r in rows)
            print("K 线总行数:", total_bars)
            print("逐标的结果（symbol, 行数, 最早日期, 最晚日期）:")
            for r in rows:
                print(" ", r[0], r[1], r[2], r[3])
        missing = set(symbols) - {r[0] for r in rows}
        if missing:
            print("L1 缺失的标的:", sorted(missing))
    except Exception as e:
        print("L1 查询异常:", e)
    print()

    # ---------- L2：27 标的行业财务（industry_revenue_summary）----------
    print("========== 2. L2 表 industry_revenue_summary：27 标的行业/财务 ==========")
    try:
        conn = psycopg2.connect(dsn_l2)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol, industry_name, revenue_ratio, rnd_ratio, commodity_ratio, updated_at
            FROM industry_revenue_summary
            WHERE symbol = ANY(%s)
            ORDER BY symbol
            """,
            (symbols,),
        )
        rows = cur.fetchall()
        conn.close()
        print("查询条件: symbol IN (27 标)")
        print("有行业/财务记录的标的数:", len(rows), "/ 27")
        if rows:
            print("逐标的结果（symbol, industry_name, revenue_ratio, rnd_ratio, commodity_ratio, updated_at）:")
            for r in rows:
                iname = (r[1] or "").strip()
                iname_display = repr(iname) if iname else "(空)"
                print(" ", r[0], "industry_name=", iname_display, "revenue_ratio=", r[2], "rnd_ratio=", r[3], "commodity_ratio=", r[4], "updated_at=", r[5])
        empty_industry = [r[0] for r in rows if not (r[1] or "").strip()]
        if empty_industry:
            print("industry_name 为空的标的数:", len(empty_industry), "->", empty_industry[:10], "..." if len(empty_industry) > 10 else "")
        missing = set(symbols) - {r[0] for r in rows}
        if missing:
            print("L2 industry_revenue_summary 缺失的标的:", sorted(missing))
    except Exception as e:
        print("L2 industry_revenue_summary 查询异常:", e)
    print()

    # ---------- L2：27 标的新闻/公告（data_versions 中 data_type=news 且 version_id 按标）----------
    print("========== 3. L2 表 data_versions：27 标的新闻/公告版本 ==========")
    try:
        conn = psycopg2.connect(dsn_l2)
        cur = conn.cursor()
        # 个股新闻 version_id 格式: news_002371.SZ_20260307120000
        cur.execute(
            "SELECT COUNT(*) FROM data_versions WHERE data_type = %s AND version_id LIKE %s",
            ("news", "news_%"),
        )
        total_news = cur.fetchone()[0]
        print("查询条件: data_type='news' AND version_id LIKE 'news_%'")
        print("data_versions 中新闻相关记录总数:", total_news)
        print("按标的统计（version_id 形如 news_<symbol>_*）:")
        per_symbol = {}
        for s in symbols:
            cur.execute(
                "SELECT COUNT(*) FROM data_versions WHERE data_type = %s AND version_id LIKE %s",
                ("news", "news_" + s + "_%"),
            )
            cnt = cur.fetchone()[0]
            per_symbol[s] = cnt
            print(" ", s, "新闻版本数:", cnt)
        has_news = sum(1 for s in symbols if per_symbol.get(s, 0) > 0)
        print("27 标中有新闻版本记录的标的数:", has_news, "/ 27")
        conn.close()
    except Exception as e:
        print("L2 data_versions 查询异常:", e)
    print()

    print("========== 查询结束 ==========")
    return 0


if __name__ == "__main__":
    sys.exit(main())
