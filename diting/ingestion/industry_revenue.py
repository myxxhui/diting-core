# [Ref: 03_原子目标与规约/Stage2_数据采集与存储/02_采集逻辑与Dockerfile_设计.md#design-stage2-02-integration-akshare]
# ingest_industry_revenue：AkShare 行业/财报/营收 → 约定表或 L2 版本化（Module A 输入）

import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2

from diting.ingestion.config import get_pg_l2_dsn
from diting.ingestion.l2_writer import write_data_version

logger = logging.getLogger(__name__)


def _parse_industry_revenue_row(data: dict) -> tuple:
    """从采集得到的 dict 中解析 (industry_name, revenue_ratio, rnd_ratio, commodity_ratio)。"""
    industry_name = ""
    revenue_ratio = 0.0
    rnd_ratio = 0.0
    commodity_ratio = 0.0
    for k, v in (data or {}).items():
        k = (k or "").strip()
        if not isinstance(v, (int, float, str)):
            continue
        if k in ("行业", "所属行业", "申万行业"):
            industry_name = str(v).strip() if v else ""
        elif k in ("主营业务收入占比", "主营营收占比", "revenue_ratio"):
            try:
                revenue_ratio = float(v) if v is not None else 0.0
            except (TypeError, ValueError):
                pass
        elif k in ("研发投入占比", "研发支出占比", "rnd_ratio"):
            try:
                rnd_ratio = float(v) if v is not None else 0.0
            except (TypeError, ValueError):
                pass
        elif k in ("大宗商品营收占比", "commodity_ratio"):
            try:
                commodity_ratio = float(v) if v is not None else 0.0
            except (TypeError, ValueError):
                pass
    return (industry_name, revenue_ratio, rnd_ratio, commodity_ratio)


def _upsert_industry_revenue_summary(conn, symbol: str, industry_name: str, revenue_ratio: float, rnd_ratio: float, commodity_ratio: float) -> None:
    """写入或更新 L2 industry_revenue_summary，供 Module A 按标的查询。"""
    symbol = (symbol or "").strip().upper()
    if not symbol:
        return
    sql = """
    INSERT INTO industry_revenue_summary (symbol, industry_name, revenue_ratio, rnd_ratio, commodity_ratio, updated_at)
    VALUES (%s, %s, %s, %s, %s, NOW())
    ON CONFLICT (symbol) DO UPDATE SET
        industry_name = EXCLUDED.industry_name,
        revenue_ratio = EXCLUDED.revenue_ratio,
        rnd_ratio = EXCLUDED.rnd_ratio,
        commodity_ratio = EXCLUDED.commodity_ratio,
        updated_at = NOW()
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (symbol, industry_name or "", revenue_ratio, rnd_ratio, commodity_ratio))
        conn.commit()
    finally:
        cur.close()

# ingest-test 目标：至少 1 只标的的财务摘要写入 L2 data_versions
DEFAULT_SYMBOL = "000001"
DATA_TYPE = "industry_revenue"


def _is_mock() -> bool:
    return os.environ.get("DITING_INGEST_MOCK", "").strip().lower() in ("1", "true", "yes")


def _get_ingest_source() -> str:
    """INGEST_SOURCE：akshare（默认）或 jqdata。"""
    raw = (os.environ.get("INGEST_SOURCE") or "akshare").strip().lower()
    return "jqdata" if raw == "jqdata" else "akshare"


def _fetch_jqdata_financial(symbol: str):
    """从 JQData 拉取估值/财务摘要，返回 dict 或 None。"""
    try:
        from diting.ingestion.jqdata_client import get_valuation_or_fundamentals

        return get_valuation_or_fundamentals(symbol)
    except ImportError:
        return None


def _fetch_akshare_financial_abstract(
    symbol: str,
    max_retries: int = 3,
    retry_delay: float = 2.0,
):
    """AkShare 股票财务摘要；错误与限流：重试+退避。"""
    import akshare as ak

    for attempt in range(max_retries):
        try:
            df = ak.stock_financial_abstract(symbol=symbol)
            return df
        except Exception as e:
            logger.warning("akshare stock_financial_abstract attempt %s failed: %s", attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            else:
                raise
    return None


def run_ingest_industry_revenue(symbol: str = None) -> int:
    """
    执行 ingest_industry_revenue：从 AkShare 拉取财务摘要并写入 L2 data_versions 与 industry_revenue_summary。
    工作目录: diting-core。symbol 建议为带交易所后缀（如 000001.SZ），与 get_current_a_share_universe 一致；
    调用外部 API 时使用无后缀代码，写入 industry_revenue_summary 时保留传入的 symbol 供 Module A 查询。
    """
    symbol = (symbol or DEFAULT_SYMBOL).strip()
    api_symbol = symbol.split(".")[0] if "." in symbol else symbol  # AkShare/JQ 使用无后缀
    now = datetime.now(timezone.utc)
    version_id = f"industry_revenue_{symbol}_{now.strftime('%Y%m%d%H%M%S')}"
    file_path = f"l2/industry_revenue/{symbol}.json"

    if _is_mock():
        file_size = len(b'{"mock": true}')
        dsn = get_pg_l2_dsn()
        conn = psycopg2.connect(dsn)
        try:
            write_data_version(
                conn,
                data_type=DATA_TYPE,
                version_id=version_id,
                timestamp=now,
                file_path=file_path,
                file_size=file_size,
                checksum="",
            )
            logger.info("ingest_industry_revenue: mock mode, 1 version")
            return 1
        finally:
            conn.close()
    else:
        source = _get_ingest_source()
        if source == "jqdata":
            data = _fetch_jqdata_financial(api_symbol)
            if not data:
                logger.warning("ingest_industry_revenue: no jqdata for symbol=%s", symbol)
                return 0
            try:
                for k, v in data.items():
                    if hasattr(v, "isoformat"):
                        data[k] = v.isoformat()
                payload = json.dumps(data, ensure_ascii=False, default=str)
                file_size = len(payload.encode("utf-8"))
            except Exception:
                file_size = 0
            dsn = get_pg_l2_dsn()
            conn = psycopg2.connect(dsn)
            try:
                write_data_version(
                    conn,
                    data_type=DATA_TYPE,
                    version_id=version_id,
                    timestamp=now,
                    file_path=file_path,
                    file_size=file_size,
                    checksum="",
                )
                try:
                    iname, rev, rnd, comm = _parse_industry_revenue_row(data)
                    _upsert_industry_revenue_summary(conn, symbol, iname, rev, rnd, comm)
                except Exception as e:
                    logger.debug("upsert industry_revenue_summary: %s", e)
                return 1
            finally:
                conn.close()
        df = _fetch_akshare_financial_abstract(api_symbol)
        if df is None or df.empty:
            logger.warning("ingest_industry_revenue: no data for symbol=%s", symbol)
            return 0
        try:
            first = df.iloc[0].to_dict()
            for k, v in first.items():
                if hasattr(v, "isoformat"):
                    first[k] = v.isoformat()
            payload = json.dumps(first, ensure_ascii=False, default=str)
            file_size = len(payload.encode("utf-8"))
        except Exception:
            file_size = 0
        dsn = get_pg_l2_dsn()
        conn = psycopg2.connect(dsn)
        try:
            write_data_version(
                conn,
                data_type=DATA_TYPE,
                version_id=version_id,
                timestamp=now,
                file_path=file_path,
                file_size=file_size,
                checksum="",
            )
            try:
                iname, rev, rnd, comm = _parse_industry_revenue_row(first)
                _upsert_industry_revenue_summary(conn, symbol, iname, rev, rnd, comm)
            except Exception as e:
                logger.debug("upsert industry_revenue_summary: %s", e)
            return 1
        finally:
            conn.close()
