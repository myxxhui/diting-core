#!/usr/bin/env python3
# 一键查询采集模块已落库数据概况：标的列表、中文名、K线条数/日期范围、新闻/行业/财务概况。
# 工作目录: diting-core；DSN 与 ingest 一致（.env + config）。
# 用法: make query-ingest-overview 或 PYTHONPATH=. python3 scripts/query_ingest_data_overview.py

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


def _section(title: str) -> None:
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


def main() -> int:
    try:
        import psycopg2
    except ImportError:
        print("请安装 psycopg2: pip install psycopg2-binary", file=sys.stderr)
        return 1

    dsn_l1 = get_timescale_dsn()
    dsn_l2 = get_pg_l2_dsn()
    if not dsn_l1:
        print("未配置 TIMESCALE_DSN，请在 .env 中配置。", file=sys.stderr)
        return 1

    # ---------- L1：全部标的 K 线概况（从 ohlcv 取标的集合）----------
    _section("1. L1 表 ohlcv：标的列表、K 线条数、日期范围")
    try:
        conn_l1 = psycopg2.connect(dsn_l1)
        cur = conn_l1.cursor()
        cur.execute(
            """
            SELECT symbol, COUNT(*) AS cnt,
                   MIN(datetime)::date AS min_dt, MAX(datetime)::date AS max_dt
            FROM ohlcv
            WHERE period IN ('day', 'daily')
            GROUP BY symbol
            ORDER BY symbol
            """
        )
        ohlcv_rows = cur.fetchall()
        conn_l1.close()
    except Exception as e:
        print("L1 查询异常:", e, file=sys.stderr)
        return 1

    if not ohlcv_rows:
        print("  (无数据或 period 非 day/daily)")
        symbols_ordered = []
    else:
        total_bars = sum(r[1] for r in ohlcv_rows)
        print("  标的数: %d | K 线总行数: %d" % (len(ohlcv_rows), total_bars))
        print("  列: symbol | 中文名 | K线条数 | 最早日期 | 最晚日期")
        print("-" * 70)
        symbols_ordered = [r[0] for r in ohlcv_rows]

    # 中文名：从 L2 symbol_names 读（不调 akshare，仅 DB）
    name_map = {}
    if dsn_l2 and symbols_ordered:
        try:
            conn = psycopg2.connect(dsn_l2)
            cur = conn.cursor()
            cur.execute(
                "SELECT symbol, name_cn FROM symbol_names WHERE symbol = ANY(%s) AND name_cn IS NOT NULL AND name_cn != ''",
                (symbols_ordered,),
            )
            for row in cur.fetchall():
                name_map[row[0]] = (row[1] or "").strip()[:32]
            conn.close()
        except Exception:
            pass

    for r in ohlcv_rows:
        sym, cnt, min_dt, max_dt = r[0], r[1], r[2], r[3]
        name = name_map.get(sym) or "(未录中文名)"
        print("  %s | %s | %d | %s | %s" % (sym, name, cnt, min_dt, max_dt))

    # ---------- L2：行业（industry_revenue_summary）----------
    _section("2. L2 表 industry_revenue_summary：行业/营收（每标一条）")
    if not dsn_l2:
        print("  未配置 PG_L2_DSN，跳过。")
    else:
        try:
            conn = psycopg2.connect(dsn_l2)
            cur = conn.cursor()
            cur.execute(
                "SELECT symbol, industry_name, revenue_ratio, rnd_ratio, updated_at FROM industry_revenue_summary ORDER BY symbol"
            )
            ind_rows = cur.fetchall()
            cur.execute("SELECT COUNT(*) FROM industry_revenue_summary")
            total_ind = cur.fetchone()[0]
            conn.close()
            print("  总记录数: %d（每标一条）" % total_ind)
            print("  列: symbol | 行业名 | 营收占比 | 研发占比 | 更新时间")
            print("-" * 70)
            for row in ind_rows[:50]:
                iname = (row[1] or "").strip() or "(空)"
                print("  %s | %s | %s | %s | %s" % (row[0], iname[:20], row[2], row[3], row[4]))
            if len(ind_rows) > 50:
                print("  ... 共 %d 条，仅展示前 50 条" % len(ind_rows))
        except Exception as e:
            print("  查询异常:", e)

    # ---------- L2：新闻（news_content）----------
    _section("3. L2 表 news_content：新闻/公告条数及时间范围")
    if not dsn_l2:
        print("  未配置 PG_L2_DSN，跳过。")
    else:
        try:
            conn = psycopg2.connect(dsn_l2)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM news_content")
            total_news = cur.fetchone()[0]
            cur.execute(
                "SELECT MIN(published_at)::date, MAX(published_at)::date FROM news_content"
            )
            row = cur.fetchone()
            min_pub, max_pub = (row[0], row[1]) if row and row[0] else (None, None)
            cur.execute(
                """
                SELECT symbol, COUNT(*) AS cnt, MIN(published_at)::date AS min_d, MAX(published_at)::date AS max_d
                FROM news_content
                GROUP BY symbol
                ORDER BY cnt DESC
                """
            )
            per_sym = cur.fetchall()
            conn.close()
            print("  总条数: %d | 全表时间范围: %s ~ %s" % (total_news, min_pub, max_pub))
            print("  列: symbol | 条数 | 最早日期 | 最晚日期")
            print("-" * 70)
            for row in per_sym[:40]:
                print("  %s | %d | %s | %s" % (row[0], row[1], row[2], row[3]))
            if len(per_sym) > 40:
                print("  ... 共 %d 个标的有新闻，仅展示前 40 个" % len(per_sym))
        except Exception as e:
            print("  查询异常:", e)

    # ---------- L2：财务（financial_summary）----------
    _section("4. L2 表 financial_summary：财务摘要（每标多报告期）")
    if not dsn_l2:
        print("  未配置 PG_L2_DSN，跳过。")
    else:
        try:
            conn = psycopg2.connect(dsn_l2)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM financial_summary")
            total_fin = cur.fetchone()[0]
            cur.execute(
                "SELECT MIN(report_date), MAX(report_date) FROM financial_summary"
            )
            row = cur.fetchone()
            min_rd, max_rd = (row[0], row[1]) if row and row[0] else (None, None)
            cur.execute(
                """
                SELECT symbol, COUNT(*) AS cnt, MIN(report_date) AS min_rd, MAX(report_date) AS max_rd
                FROM financial_summary
                GROUP BY symbol
                ORDER BY cnt DESC
                """
            )
            per_sym = cur.fetchall()
            conn.close()
            print("  总记录数: %d | 报告期范围: %s ~ %s" % (total_fin, min_rd, max_rd))
            print("  列: symbol | 报告期条数 | 最早报告期 | 最晚报告期")
            print("-" * 70)
            for row in per_sym[:40]:
                print("  %s | %d | %s | %s" % (row[0], row[1], row[2], row[3]))
            if len(per_sym) > 40:
                print("  ... 共 %d 个标的有财务数据，仅展示前 40 个" % len(per_sym))
        except Exception as e:
            # 表可能不存在
            print("  查询异常:", e)

    # ---------- L2：data_versions 汇总（按 data_type）----------
    _section("5. L2 表 data_versions：按类型汇总")
    if not dsn_l2:
        print("  未配置 PG_L2_DSN，跳过。")
    else:
        try:
            conn = psycopg2.connect(dsn_l2)
            cur = conn.cursor()
            cur.execute(
                "SELECT data_type, COUNT(*) FROM data_versions GROUP BY data_type ORDER BY data_type"
            )
            rows = cur.fetchall()
            conn.close()
            if not rows:
                print("  (无记录)")
            else:
                for dt, cnt in rows:
                    print("  %s: %d 条" % (dt, cnt))
        except Exception as e:
            print("  查询异常:", e)

    _section("查询结束")
    return 0


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    sys.exit(main())
