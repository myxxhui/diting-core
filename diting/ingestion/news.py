# [Ref: 03_原子目标与规约/Stage2_数据采集与存储/02_采集逻辑与Dockerfile_设计.md#design-stage2-02-integration-akshare]
# [Ref: design-stage2-02-integration-openbb]
# ingest_news：按 INGEST_SOURCE 选择数据源。jqdata 时用 finance.STK_NEWS_INFO + STK_ANN_REPORT；akshare 时用 AkShare+OpenBB。

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2

from diting.ingestion.config import get_pg_l2_dsn
from diting.ingestion.l2_writer import write_data_version

logger = logging.getLogger(__name__)

DATA_TYPE_NEWS = "news"


def _get_ingest_source() -> str:
    """INGEST_SOURCE：akshare（默认）或 jqdata。与 universe/industry_revenue 一致。"""
    raw = (os.environ.get("INGEST_SOURCE") or "akshare").strip().lower()
    return "jqdata" if raw == "jqdata" else "akshare"


def _is_mock() -> bool:
    return os.environ.get("DITING_INGEST_MOCK", "").strip().lower() in ("1", "true", "yes")


def _fetch_akshare_news(max_retries: int = 3, retry_delay: float = 2.0) -> list:
    """国内：AkShare 财经资讯。优先 js_news；不可用时退化为 stock_news_em（个股新闻）。
    东方财富接口常返回空/非 JSON 时不再抛异常，降级为返回空列表，保证采集流水继续。
    """
    for attempt in range(max_retries):
        try:
            import akshare as ak

            if hasattr(ak, "js_news"):
                df = ak.js_news(indicator="最新资讯")
            elif hasattr(ak, "stock_news_em"):
                df = ak.stock_news_em(symbol="000001")
            else:
                df = None
            if df is None or df.empty:
                return []
            return df.to_dict("records")
        except Exception as e:
            logger.warning("akshare news attempt %s failed: %s", attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            else:
                # 重试耗尽：降级为无新闻，不抛异常，避免整条采集流水报错（东方财富反爬/接口变更常见）
                logger.warning("akshare news all retries exhausted, skipping news step: %s", e)
                return []
    return []


def _fetch_akshare_stock_news_em(
    symbol: str,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> list:
    """按标的拉取东方财富个股新闻；用于生产级每标的数据。失败时返回空列表，不抛异常。"""
    for attempt in range(max_retries):
        try:
            import akshare as ak

            if not hasattr(ak, "stock_news_em"):
                return []
            df = ak.stock_news_em(symbol=symbol)
            if df is None or df.empty:
                return []
            return df.to_dict("records")
        except Exception as e:
            logger.warning("akshare stock_news_em symbol=%s attempt %s failed: %s", symbol, attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            else:
                logger.warning("akshare stock_news_em symbol=%s all retries exhausted, returning []", symbol)
                return []
    return []


def _fetch_openbb_macro_or_news(max_retries: int = 2, retry_delay: float = 2.0) -> dict:
    """
    OpenBB 国际/宏观：至少一条到 L2 的写入路径。
    [Ref: design-stage2-02-integration-openbb] Provider 抽象，OpenBB 为默认实现。
    镜像未安装 openbb 时返回占位 meta，不抛异常，保证采集流水不因 openbb 报错。
    """
    try:
        from openbb import obb
    except ModuleNotFoundError as e:
        logger.info("openbb not installed, skipping macro/news: %s", e)
        return {"source": "openbb", "skipped": True, "reason": "not_installed"}

    for attempt in range(max_retries):
        try:
            # 宏观：economy.gdp.nominal 或 real（OpenBB Platform 4.x）
            result = obb.economy.gdp.nominal(country="united_states", provider="oecd")
            if result and getattr(result, "results", None):
                return {"source": "openbb", "provider": "oecd", "count": len(result.results)}
            result = obb.economy.gdp.real(country="united_states")
            if result and getattr(result, "results", None):
                return {"source": "openbb", "provider": "real_gdp", "count": len(result.results)}
            return {"source": "openbb", "provider": "none", "count": 0}
        except Exception as e:
            logger.warning("openbb attempt %s failed: %s", attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            else:
                # 重试耗尽：返回占位，不抛异常，上层仍可写 L2
                return {"source": "openbb", "error": "all_retries_failed", "message": str(e)}
    return {"source": "openbb", "error": "all_retries_failed"}


def _parse_news_date(record: dict) -> Optional[datetime]:
    """从单条新闻 dict 中解析日期，支持常见字段名。"""
    for key in ("date", "日期", "发布时间", "time"):
        v = record.get(key)
        if v is None:
            continue
        if isinstance(v, datetime):
            return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v
        try:
            if isinstance(v, str):
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
                    try:
                        return datetime.strptime(v[:19].replace("/", "-"), fmt).replace(tzinfo=timezone.utc)
                    except (ValueError, TypeError):
                        continue
        except Exception:
            pass
    return None


def _filter_news_by_days(records: list, days_back: int) -> list:
    """只保留最近 days_back 天内的新闻；无日期字段的条目保留。"""
    if not records or days_back <= 0:
        return records
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    out = []
    for r in records:
        if not isinstance(r, dict):
            out.append(r)
            continue
        dt = _parse_news_date(r)
        if dt is None:
            out.append(r)
        elif dt >= cutoff:
            out.append(r)
    return out


def _parse_date_bound(s: str):
    """解析 YYYY-MM-DD 或 YYYYMMDD 为 date，用于日期范围过滤。"""
    if not s or not isinstance(s, str):
        return None
    s = s.strip().replace("-", "")[:8]
    if len(s) != 8:
        return None
    try:
        from datetime import date
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except (ValueError, TypeError):
        return None


def _filter_news_by_date_range(records: list, start_date, end_date) -> list:
    """只保留日期在 [start_date, end_date] 内的新闻；start/end 为 date 或 datetime；无日期字段的条目保留。"""
    if not records or start_date is None or end_date is None:
        return records
    start_d = start_date.date() if hasattr(start_date, "date") else start_date
    end_d = end_date.date() if hasattr(end_date, "date") else end_date
    out = []
    for r in records:
        if not isinstance(r, dict):
            out.append(r)
            continue
        dt = _parse_news_date(r)
        if dt is None:
            out.append(r)
        else:
            d = dt.date() if hasattr(dt, "date") else dt
            if start_d <= d <= end_d:
                out.append(r)
    return out


def run_ingest_news(
    symbol: str = None,
    days_back: int = None,
    date_start: str = None,
    date_end: str = None,
) -> int:
    """
    执行 ingest_news：国内 AkShare + 国际 OpenBB，写入 L2 data_versions。
    symbol 为 None：拉取全市场最新资讯 + OpenBB 宏观（各写一条版本）。
    symbol 不为 None：拉取该标的个股新闻（stock_news_em）并写入 L2，用于生产级每标的数据。
    days_back：仅保留最近 N 天内的新闻（按条目的日期字段过滤）；None 或 0 表示不过滤。
    date_start/date_end：与 days_back 二选一；指定时只保留日期在 [date_start, date_end] 内的新闻（格式 YYYY-MM-DD 或 YYYYMMDD）。
    工作目录: diting-core。DITING_INGEST_MOCK=1 时写入两条 mock 版本（akshare + openbb）。
    """
    written = 0
    now = datetime.now(timezone.utc)
    dsn = get_pg_l2_dsn()
    conn = psycopg2.connect(dsn)

    try:
        if _is_mock():
            if symbol:
                version_id = f"news_{symbol}_{now.strftime('%Y%m%d%H%M%S')}"
                write_data_version(
                    conn,
                    data_type=DATA_TYPE_NEWS,
                    version_id=version_id,
                    timestamp=now,
                    file_path=f"l2/news/{symbol}.json",
                    file_size=0,
                    checksum="",
                )
                return 1
            version_id_ak = f"news_akshare_{now.strftime('%Y%m%d%H%M%S')}"
            write_data_version(
                conn,
                data_type=DATA_TYPE_NEWS,
                version_id=version_id_ak,
                timestamp=now,
                file_path="l2/news/akshare_latest.json",
                file_size=0,
                checksum="",
            )
            written += 1
            version_id_ob = f"news_openbb_{now.strftime('%Y%m%d%H%M%S')}"
            write_data_version(
                conn,
                data_type=DATA_TYPE_NEWS,
                version_id=version_id_ob,
                timestamp=now,
                file_path="l2/news/openbb_macro.json",
                file_size=0,
                checksum="",
            )
            written += 1
            logger.info("ingest_news: mock mode, %s versions", written)
            return written

        source = _get_ingest_source()
        # INGEST_SOURCE=jqdata 时：个股用 JQData STK_NEWS_INFO + STK_ANN_REPORT；无 symbol 时仅写一条汇总占位（按标由生产脚本循环调用）
        if source == "jqdata":
            if symbol:
                try:
                    from diting.ingestion.jqdata_client import get_stock_news, get_stock_announcements

                    news_records = get_stock_news(
                        symbol_ts=symbol,
                        start_date=date_start,
                        end_date=date_end,
                        days_back=days_back if (not date_start or not date_end) else None,
                        limit=500,
                    )
                    ann_records = get_stock_announcements(
                        symbol_ts=symbol,
                        start_date=date_start,
                        end_date=date_end,
                        days_back=days_back if (not date_start or not date_end) else None,
                        limit=200,
                    )
                    for r in news_records:
                        r["_source_type"] = "news"
                    for r in ann_records:
                        r["_source_type"] = "announcement"
                    records = news_records + ann_records
                    if date_start and date_end and records:
                        start_d = _parse_date_bound(date_start)
                        end_d = _parse_date_bound(date_end)
                        if start_d is not None and end_d is not None:
                            records = _filter_news_by_date_range(records, start_d, end_d)
                    elif days_back and days_back > 0 and records:
                        records = _filter_news_by_days(records, days_back)
                    version_id = f"news_{symbol}_{now.strftime('%Y%m%d%H%M%S')}"
                    file_path = f"l2/news/{symbol}.json"
                    payload = {
                        "source": "jqdata",
                        "news_count": len([r for r in records if r.get("_source_type") == "news"]),
                        "announcements_count": len([r for r in records if r.get("_source_type") == "announcement"]),
                        "items": records,
                    }
                    file_size = len(str(payload))
                    write_data_version(
                        conn,
                        data_type=DATA_TYPE_NEWS,
                        version_id=version_id,
                        timestamp=now,
                        file_path=file_path,
                        file_size=file_size,
                        checksum="",
                    )
                    written += 1
                    logger.info("ingest_news: JQData symbol=%s news=%s announcements=%s", symbol, len(news_records), len(ann_records))
                except Exception as e:
                    logger.warning("ingest_news jqdata symbol=%s failed: %s", symbol, e)
                return written
            try:
                meta = {"source": "jqdata", "message": "per-symbol news/announcements via run_ingest_news(symbol=...)"}
                version_id = f"news_jqdata_{now.strftime('%Y%m%d%H%M%S')}"
                write_data_version(
                    conn,
                    data_type=DATA_TYPE_NEWS,
                    version_id=version_id,
                    timestamp=now,
                    file_path="l2/news/jqdata_placeholder.json",
                    file_size=len(str(meta)),
                    checksum="",
                )
                written += 1
                logger.info("ingest_news: INGEST_SOURCE=jqdata, wrote placeholder (per-symbol in production loop)")
            except Exception as e:
                logger.warning("ingest_news jqdata placeholder failed: %s", e)
            return written

        # 以下为 akshare 源：按标的 / 全市场 AkShare + OpenBB
        # 按标的：个股新闻
        if symbol:
            try:
                records = _fetch_akshare_stock_news_em(symbol)
                if date_start and date_end:
                    start_d = _parse_date_bound(date_start)
                    end_d = _parse_date_bound(date_end)
                    if start_d is not None and end_d is not None:
                        records = _filter_news_by_date_range(records, start_d, end_d)
                elif days_back and days_back > 0:
                    records = _filter_news_by_days(records, days_back)
                version_id = f"news_{symbol}_{now.strftime('%Y%m%d%H%M%S')}"
                file_path = f"l2/news/{symbol}.json"
                file_size = len(str(records)) if records else 0
                write_data_version(
                    conn,
                    data_type=DATA_TYPE_NEWS,
                    version_id=version_id,
                    timestamp=now,
                    file_path=file_path,
                    file_size=file_size,
                    checksum="",
                )
                return 1
            except Exception as e:
                logger.warning("ingest_news symbol=%s failed: %s", symbol, e)
                return 0

        # 全市场：国内 AkShare 最新资讯
        try:
            records = _fetch_akshare_news()
            if date_start and date_end and records:
                start_d = _parse_date_bound(date_start)
                end_d = _parse_date_bound(date_end)
                if start_d is not None and end_d is not None:
                    records = _filter_news_by_date_range(records, start_d, end_d)
            elif days_back and days_back > 0 and records:
                records = _filter_news_by_days(records, days_back)
            if records:
                version_id = f"news_akshare_{now.strftime('%Y%m%d%H%M%S')}"
                file_path = "l2/news/akshare_latest.json"
                write_data_version(
                    conn,
                    data_type=DATA_TYPE_NEWS,
                    version_id=version_id,
                    timestamp=now,
                    file_path=file_path,
                    file_size=len(str(records)),
                    checksum="",
                )
                written += 1
        except Exception as e:
            logger.exception("ingest_news akshare failed: %s", e)

        # 国际/宏观：OpenBB
        try:
            meta = _fetch_openbb_macro_or_news()
            version_id = f"news_openbb_{now.strftime('%Y%m%d%H%M%S')}"
            file_path = "l2/news/openbb_macro.json"
            write_data_version(
                conn,
                data_type=DATA_TYPE_NEWS,
                version_id=version_id,
                timestamp=now,
                file_path=file_path,
                file_size=len(str(meta)),
                checksum="",
            )
            written += 1
        except Exception as e:
            logger.exception("ingest_news openbb failed: %s", e)

        return written
    finally:
        conn.close()
