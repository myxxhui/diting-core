# [Ref: 03_原子目标与规约/_共享规约/11_数据采集与输入层规约] 全 A 股标的池
# 表名与字段与 11_ 约定一致：symbol, market, updated_at；可选 count, source
# Stage2 采集 Job 可调 AkShare/OpenBB 获取全 A 股并写入；支持按有效条件触发更新

import logging
import os
from datetime import datetime, timezone
from typing import List, Tuple, Optional

import psycopg2
from psycopg2.extras import execute_values

from diting.ingestion.config import get_timescale_dsn

logger = logging.getLogger(__name__)

# 表名与 11_ 约定一致，与 L1 同库
TABLE_NAME = "a_share_universe"

# 建表 SQL：至少 symbol, market, updated_at；可选 count, source
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    symbol TEXT NOT NULL PRIMARY KEY,
    market TEXT NOT NULL DEFAULT 'A',
    updated_at TIMESTAMPTZ NOT NULL,
    count INTEGER,
    source TEXT
);
"""


def ensure_universe_table(conn) -> None:
    """确保 a_share_universe 表存在；与 11_ 存储约定一致。"""
    cur = conn.cursor()
    try:
        cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        logger.debug("ensure_universe_table: table %s ready", TABLE_NAME)
    finally:
        cur.close()


def write_universe_batch(
    conn,
    rows: List[Tuple[str, str, datetime, Optional[int], Optional[str]]],
    updated_at: Optional[datetime] = None,
) -> int:
    """
    全量刷新标的池：先清表再写入。每行 (symbol, market, updated_at, count, source)。
    与 11_ 约定一致；同批写入使用同一 updated_at 便于有效条件判断。
    """
    if not rows:
        return 0
    ts = updated_at or datetime.now(timezone.utc)
    cur = conn.cursor()
    try:
        cur.execute(f"DELETE FROM {TABLE_NAME}")
        # 使用 execute_values 批量写入，避免 5000+ 次逐行 round-trip（远程库时显著加速）
        sql = f"""
        INSERT INTO {TABLE_NAME} (symbol, market, updated_at, count, source)
        VALUES %s
        ON CONFLICT (symbol) DO UPDATE SET
            market = EXCLUDED.market,
            updated_at = EXCLUDED.updated_at,
            count = EXCLUDED.count,
            source = EXCLUDED.source
        """
        batch = [(r[0], r[1], ts, r[3] if len(r) > 3 else None, r[4] if len(r) > 4 else None) for r in rows]
        execute_values(cur, sql, batch, page_size=1000)
        conn.commit()
        n = len(batch)
        logger.info("write_universe_batch: refreshed %s rows, updated_at=%s", n, ts)
        return n
    finally:
        cur.close()


def _is_mock() -> bool:
    """DITING_INGEST_MOCK=1 时使用本地 mock 列表，不请求外网。"""
    return os.environ.get("DITING_INGEST_MOCK", "").strip().lower() in ("1", "true", "yes")


def _symbol_to_ts(code: str) -> str:
    """A 股代码转 exchange 后缀：6xxxxx -> .SH，否则 .SZ"""
    code = str(code).strip()
    if code.startswith("6"):
        return f"{code}.SH"
    return f"{code}.SZ"


def _get_ingest_source() -> str:
    """INGEST_SOURCE：akshare（默认）或 jqdata。"""
    raw = (os.environ.get("INGEST_SOURCE") or "akshare").strip().lower()
    return "jqdata" if raw == "jqdata" else "akshare"


def _fetch_jqdata_universe() -> List[Tuple[str, str, datetime, Optional[int], Optional[str]]]:
    """通过 JQData 获取全 A 股列表；需配置 JQDATA_USER、JQDATA_PASSWORD 并 pip install jqdatasdk。"""
    try:
        from diting.ingestion.jqdata_client import get_all_stock_codes

        return get_all_stock_codes()
    except ImportError:
        logger.warning("jqdata_client 或 jqdatasdk 不可用，无法使用 JQData 标的源")
        return []


def _fetch_akshare_universe() -> List[Tuple[str, str, datetime, Optional[int], Optional[str]]]:
    """
    通过 AkShare 获取当前全 A 股列表；与 11_ 写入方约定一致。
    返回 [(symbol_ts, market, updated_at, count, source), ...]
    """
    import time

    import akshare as ak

    max_retries = 3
    retry_delay = 2.0
    ts = datetime.now(timezone.utc)
    for attempt in range(max_retries):
        try:
            df = ak.stock_info_a_code_name()
            if df is None or df.empty:
                logger.warning("akshare stock_info_a_code_name returned empty")
                return []
            # 列名可能为 code、代码、或首列
            code_col = None
            for c in ("code", "代码", "symbol"):
                if c in df.columns:
                    code_col = c
                    break
            if code_col is None:
                code_col = df.columns[0]
            rows = []
            for _, r in df.iterrows():
                code = str(r[code_col]).strip()
                if not code or not code.isdigit():
                    continue
                symbol_ts = _symbol_to_ts(code)
                rows.append((symbol_ts, "A", ts, None, "akshare"))
            logger.info("_fetch_akshare_universe: fetched %s symbols", len(rows))
            return rows
        except Exception as e:
            logger.warning("akshare stock_info_a_code_name attempt %s failed: %s", attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            else:
                raise
    return []


def _mock_universe_rows() -> List[Tuple[str, str, datetime, Optional[int], Optional[str]]]:
    """Mock 数据：无外网时供 ingest-test 验证表与写入。"""
    ts = datetime.now(timezone.utc)
    mock_codes = ["000001", "600000", "000998", "688981", "601899"]
    return [(_symbol_to_ts(c), "A", ts, None, "mock") for c in mock_codes]


def run_ingest_universe() -> int:
    """
    执行 universe 采集 Job：获取当前全 A 股并写入 L1 表 a_share_universe。
    工作目录: diting-core。DITING_INGEST_MOCK=1 时写入 mock 列表。
    """
    if _is_mock():
        rows = _mock_universe_rows()
        logger.info("run_ingest_universe: mock mode, %s symbols", len(rows))
    else:
        source = _get_ingest_source()
        if source == "jqdata":
            logger.info("universe：正在从 JQData（聚宽）拉取全 A 股列表…")
            rows = _fetch_jqdata_universe()
        else:
            logger.info("universe：正在从东方财富拉取全 A 股列表…")
            rows = _fetch_akshare_universe()
        if not rows:
            logger.warning("run_ingest_universe: no symbols fetched")
            return 0
        logger.info("universe：已拉取 %s 只标的，正在写入数据库…", len(rows))

    dsn = get_timescale_dsn()
    conn = psycopg2.connect(dsn)
    try:
        ensure_universe_table(conn)
        n = write_universe_batch(conn, rows)
        return n
    finally:
        conn.close()
