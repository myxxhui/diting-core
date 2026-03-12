# [Ref: diting-infra schemas/sql/05_l2_financial_summary.sql]
# 财务数据采集：AkShare stock_financial_abstract → L2 financial_summary

import logging
import math
import os
import time
from typing import Optional

import psycopg2

from diting.ingestion.config import get_pg_l2_dsn
from diting.ingestion.l2_writer import write_financial_summary_batch

logger = logging.getLogger(__name__)

METRIC_MAP = {
    "营业总收入": "revenue",
    "净利润": "net_profit",
    "归母净利润": "net_profit_parent",
    "扣非净利润": "deducted_np",
    "毛利率": "gross_margin",
    "销售净利率": "net_margin",
    "净资产收益率(ROE)": "roe",
    "总资产报酬率(ROA)": "roa",
    "基本每股收益": "eps",
    "每股净资产": "bvps",
    "资产负债率": "debt_ratio",
    "营业总收入增长率": "revenue_growth",
    "归属母公司净利润增长率": "np_growth",
    "经营现金流量净额": "ocf",
    "流动比率": "current_ratio",
    "期间费用率": "cost_ratio",
    "股东权益合计(净资产)": "equity",
    "商誉": "goodwill",
}

DB_COLS = [
    "revenue", "net_profit", "net_profit_parent", "deducted_np",
    "gross_margin", "net_margin", "roe", "roa", "eps", "bvps",
    "debt_ratio", "revenue_growth", "np_growth", "ocf",
    "current_ratio", "cost_ratio", "equity", "goodwill",
]


def _safe_float(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        f = float(v)
        return default if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return default


def run_ingest_financial(
    symbol: str,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> int:
    """
    拉取单标的全部历史财务摘要（年报 Q4 + 半年报 Q2 + 季报 Q1/Q3），写入 L2 financial_summary。
    symbol 带后缀如 '601138.SH'，AkShare 需要纯 6 位数字。
    返回写入的报告期数量。
    """
    code = symbol.split(".")[0]
    dsn = get_pg_l2_dsn()
    conn = psycopg2.connect(dsn)
    try:
        for attempt in range(max_retries):
            try:
                import akshare as ak
                df = ak.stock_financial_abstract(symbol=code)
                if df is None or df.empty:
                    logger.warning("financial abstract empty for %s", symbol)
                    return 0
                break
            except Exception as e:
                logger.warning("stock_financial_abstract %s attempt %s: %s", symbol, attempt + 1, e)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    return 0
        _QUARTER_SUFFIXES = ("1231", "0930", "0630", "0331")
        date_cols = [c for c in df.columns[2:] if any(c.endswith(s) for s in _QUARTER_SUFFIXES)]
        if not date_cols:
            date_cols = list(df.columns[2:])
        metric_rows = {}
        for _, row in df.iterrows():
            m = row.get("指标", "")
            if m in METRIC_MAP:
                key = METRIC_MAP[m]
                vals = {}
                for dc in date_cols:
                    vals[dc] = _safe_float(row.get(dc, 0))
                metric_rows[key] = vals
        rows = []
        for dc in date_cols:
            r = [symbol, dc]
            for col in DB_COLS:
                r.append(metric_rows.get(col, {}).get(dc, 0.0))
            rows.append(tuple(r))
        if rows:
            n = write_financial_summary_batch(conn, rows)
            logger.info("ingest_financial: %s wrote %s periods", symbol, len(rows))
            return len(rows)
        return 0
    finally:
        conn.close()
