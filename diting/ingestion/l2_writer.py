# [Ref: 03_原子目标与规约/_共享规约/07_数据版本控制规约]
# [Ref: diting-infra schemas/sql/02_l2_data_versions.sql]
# [Ref: diting-infra schemas/sql/04_l2_news_content.sql]
# [Ref: diting-infra schemas/sql/05_l2_financial_summary.sql]
# 写入 L2 表 data_versions + news_content + financial_summary

import logging
from datetime import datetime
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)


def write_data_version(
    conn,
    data_type: str,
    version_id: str,
    timestamp: datetime,
    file_path: str,
    file_size: Optional[int] = None,
    checksum: Optional[str] = None,
) -> None:
    """
    写入一条 data_versions 记录。UNIQUE(data_type, version_id)，冲突时忽略或更新由调用方决定。
    此处采用 ON CONFLICT DO NOTHING 避免重复写入同版本。
    """
    sql = """
    INSERT INTO data_versions (data_type, version_id, timestamp, file_path, file_size, checksum)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (data_type, version_id) DO NOTHING
    """
    cur = conn.cursor()
    try:
        cur.execute(
            sql,
            (data_type, version_id, timestamp, file_path, file_size or 0, checksum or ""),
        )
        conn.commit()
        if cur.rowcount:
            logger.info("write_data_version: data_type=%s version_id=%s", data_type, version_id)
    finally:
        cur.close()


def write_news_content_batch(
    conn,
    rows: List[Tuple[str, str, str, str, str, str, str, Optional[datetime]]],
) -> int:
    """
    批量写入 news_content 表。每行元组：
    - 8 元组：(symbol, source, source_type, title, content, url, keywords, published_at)
      → scope='symbol', scope_id=symbol（与 07_ 一致）
    - 10 元组：末尾加 (scope, scope_id)；行业新闻可 symbol=NULL, scope='industry', scope_id=申万名
    Python 端计算 title_hash；唯一键 (scope, scope_id, title_hash, published_at)。
    """
    if not rows:
        return 0
    import hashlib
    expanded = []
    epoch = datetime(1970, 1, 1)
    for row in rows:
        if len(row) >= 10:
            symbol, source, source_type, title, content, url, keywords, pub_at, scope, scope_id = row[:10]
        else:
            symbol, source, source_type, title, content, url, keywords, pub_at = row[:8]
            scope, scope_id = "symbol", (symbol or "").strip()
        title_hash = hashlib.md5(title.encode("utf-8")).hexdigest()
        sym = symbol if (symbol or "").strip() else None
        sc = (scope or "symbol").strip() or "symbol"
        sid = (scope_id or "").strip()
        if sc == "symbol" and not sid and sym:
            sid = sym
        expanded.append((sym, source, source_type, title, title_hash, content, url, keywords, pub_at or epoch, sc, sid))
    sql = """
    INSERT INTO news_content (symbol, source, source_type, title, title_hash, content, url, keywords, published_at, scope, scope_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (scope, scope_id, title_hash, published_at)
    DO UPDATE SET content  = EXCLUDED.content,
                  url      = EXCLUDED.url,
                  keywords = EXCLUDED.keywords,
                  symbol   = COALESCE(EXCLUDED.symbol, news_content.symbol)
    """
    cur = conn.cursor()
    try:
        from psycopg2.extras import execute_batch
        execute_batch(cur, sql, expanded, page_size=200)
        conn.commit()
        affected = cur.rowcount
        logger.info("write_news_content_batch: %s rows upserted", affected)
        return affected
    finally:
        cur.close()


def write_financial_summary_batch(conn, rows: list) -> int:
    """
    批量写入 financial_summary 表。每行为 tuple：
    (symbol, report_date, revenue, net_profit, net_profit_parent, deducted_np,
     gross_margin, net_margin, roe, roa, eps, bvps, debt_ratio,
     revenue_growth, np_growth, ocf, current_ratio, cost_ratio, equity, goodwill)
    以 (symbol, report_date) 去重，冲突时更新全部指标。
    """
    if not rows:
        return 0
    sql = """
    INSERT INTO financial_summary
        (symbol, report_date, revenue, net_profit, net_profit_parent, deducted_np,
         gross_margin, net_margin, roe, roa, eps, bvps, debt_ratio,
         revenue_growth, np_growth, ocf, current_ratio, cost_ratio, equity, goodwill)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (symbol, report_date)
    DO UPDATE SET revenue=EXCLUDED.revenue, net_profit=EXCLUDED.net_profit,
        net_profit_parent=EXCLUDED.net_profit_parent, deducted_np=EXCLUDED.deducted_np,
        gross_margin=EXCLUDED.gross_margin, net_margin=EXCLUDED.net_margin,
        roe=EXCLUDED.roe, roa=EXCLUDED.roa, eps=EXCLUDED.eps, bvps=EXCLUDED.bvps,
        debt_ratio=EXCLUDED.debt_ratio, revenue_growth=EXCLUDED.revenue_growth,
        np_growth=EXCLUDED.np_growth, ocf=EXCLUDED.ocf, current_ratio=EXCLUDED.current_ratio,
        cost_ratio=EXCLUDED.cost_ratio, equity=EXCLUDED.equity, goodwill=EXCLUDED.goodwill,
        updated_at=NOW()
    """
    cur = conn.cursor()
    try:
        from psycopg2.extras import execute_batch
        execute_batch(cur, sql, rows, page_size=100)
        conn.commit()
        affected = cur.rowcount
        logger.info("write_financial_summary_batch: %s rows upserted", affected)
        return affected
    finally:
        cur.close()
