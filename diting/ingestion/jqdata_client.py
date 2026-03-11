# [Ref: 聚宽 JQData 使用说明] 数据采集从 JQData 获取时的客户端封装
# 需配置 JQDATA_USER / JQDATA_PASSWORD，并 pip install jqdatasdk

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple, Any

logger = logging.getLogger(__name__)

_JQDATA_AUTH_DONE = False


def _ensure_auth() -> bool:
    """从环境变量认证 JQData，成功返回 True。"""
    global _JQDATA_AUTH_DONE
    if _JQDATA_AUTH_DONE:
        return True
    user = (os.environ.get("JQDATA_USER") or "").strip()
    password = (os.environ.get("JQDATA_PASSWORD") or "").strip()
    if not user or not password:
        logger.warning("JQData: 未配置 JQDATA_USER / JQDATA_PASSWORD，无法使用 JQData 源")
        return False
    try:
        from jqdatasdk import auth

        auth(user, password)
        _JQDATA_AUTH_DONE = True
        logger.debug("JQData auth OK")
        return True
    except Exception as e:
        logger.warning("JQData auth failed: %s", e)
        return False


def is_available() -> bool:
    """是否已安装 jqdatasdk 且认证可用。"""
    try:
        import jqdatasdk  # noqa: F401
    except ImportError:
        return False
    return _ensure_auth()


def jqcode_to_ts(code: str) -> str:
    """聚宽代码转本库 symbol：000001.XSHE -> 000001.SZ，600000.XSHG -> 600000.SH"""
    code = str(code).strip().upper()
    if ".XSHG" in code:
        return code.replace(".XSHG", ".SH")
    if ".XSHE" in code:
        return code.replace(".XSHE", ".SZ")
    return code


def ts_to_jqcode(symbol: str) -> str:
    """本库 symbol 转聚宽代码：000001.SZ -> 000001.XSHE，600000.SH/588000.SH -> 600000.XSHG/588000.XSHG"""
    s = str(symbol).strip().split(".")[0]
    if not s:
        return symbol
    # 沪市：6 开头、58/51/50 开头（ETF/基金等）
    if s.startswith("6") or s.startswith("58") or s.startswith("51") or s.startswith("50"):
        return f"{s}.XSHG"
    return f"{s}.XSHE"


def get_all_stock_codes() -> List[Tuple[str, str, datetime, Optional[int], Optional[str]]]:
    """
    获取全 A 股列表，返回与 write_universe_batch 一致的格式：
    [(symbol_ts, market, updated_at, count, source), ...]
    """
    if not _ensure_auth():
        return []
    try:
        from jqdatasdk import get_all_securities

        df = get_all_securities(types=["stock"])
        if df is None or df.empty:
            return []
        ts = datetime.now(timezone.utc)
        rows = []
        # 聚宽返回的 DataFrame 可能 code 在 index 或列 'code' 中
        codes = df.index.tolist() if hasattr(df.index, "tolist") else []
        if not codes and "code" in df.columns:
            codes = df["code"].astype(str).tolist()
        for code in codes:
            symbol_ts = jqcode_to_ts(str(code))
            rows.append((symbol_ts, "A", ts, None, "jqdata"))
        logger.info("JQData get_all_securities: %s symbols", len(rows))
        return rows
    except Exception as e:
        logger.warning("JQData get_all_stock_codes failed: %s", e)
        return []


def get_price(
    symbol: str,
    start_date: str,
    end_date: str,
    symbol_ts: str,
    period: str = "daily",
) -> List[Tuple[str, str, Any, float, float, float, float, int]]:
    """
    拉取单只标的日线，返回与 write_ohlcv_batch 一致的格式：
    [(symbol_ts, period, datetime, open, high, low, close, volume), ...]
    start_date/end_date 格式 YYYYMMDD，内部会转为 YYYY-MM-DD。
    """
    if not _ensure_auth():
        return []
    jq_code = ts_to_jqcode(symbol)
    start_ymd = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}" if len(start_date) >= 8 else start_date
    end_ymd = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}" if len(end_date) >= 8 else end_date
    try:
        from jqdatasdk import get_price as jq_get_price

        df = jq_get_price(
            jq_code,
            start_date=start_ymd,
            end_date=end_ymd,
            frequency="daily",
            skip_paused=True,
            fq="post",  # 后复权，与规约一致
        )
        if df is None or df.empty:
            return []
        rows = []
        for dt_index, r in df.iterrows():
            if hasattr(dt_index, "to_pydatetime"):
                dt = dt_index.to_pydatetime()
            else:
                dt = datetime.fromisoformat(str(dt_index)[:10])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            o = float(r.get("open", 0) or 0)
            h = float(r.get("high", 0) or 0)
            l_ = float(r.get("low", 0) or 0)
            c = float(r.get("close", 0) or 0)
            vol = int(float(r.get("volume", 0) or 0))
            rows.append((symbol_ts, period, dt, o, h, l_, c, vol))
        return rows
    except Exception as e:
        logger.warning("JQData get_price %s failed: %s", jq_code, e)
        return []


def _norm_ymd(start_date: Optional[str], end_date: Optional[str], days_back: Optional[int]):
    """将 start_date/end_date（YYYYMMDD 或 YYYY-MM-DD）或 days_back 转为 (start_ymd, end_ymd)。"""
    from datetime import date, timedelta

    if start_date and end_date:
        s = str(start_date).strip().replace("-", "")[:8]
        e = str(end_date).strip().replace("-", "")[:8]
        if len(s) == 8 and len(e) == 8:
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}", f"{e[:4]}-{e[4:6]}-{e[6:8]}"
    if days_back and days_back > 0:
        end_d = date.today()
        start_d = end_d - timedelta(days=days_back)
        return start_d.isoformat(), end_d.isoformat()
    return None, None


def get_stock_news(
    symbol_ts: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days_back: Optional[int] = 30,
    limit: int = 500,
) -> List[dict]:
    """
    个股新闻：finance.STK_NEWS_INFO（媒体报道、新闻快讯及摘要）。
    返回 list of dict：title, date/pub_date, content/summary, code 等，供 L2 写入。
    """
    if not _ensure_auth():
        return []
    jq_code = ts_to_jqcode(symbol_ts)
    start_ymd, end_ymd = _norm_ymd(start_date, end_date, days_back)
    if not start_ymd or not end_ymd:
        end_d = datetime.now(timezone.utc).date()
        start_ymd = (end_d - timedelta(days=30)).isoformat()
        end_ymd = end_d.isoformat()
    try:
        from jqdatasdk import finance, query

        q = (
            query(finance.STK_NEWS_INFO)
            .filter(finance.STK_NEWS_INFO.code == jq_code)
            .filter(finance.STK_NEWS_INFO.pub_date >= start_ymd)
            .filter(finance.STK_NEWS_INFO.pub_date <= end_ymd)
            .limit(limit)
        )
        df = finance.run_query(q)
        if df is None or df.empty:
            return []
        records = []
        for _, r in df.iterrows():
            rec = {"code": jqcode_to_ts(str(r.get("code", ""))), "title": r.get("title"), "date": r.get("pub_date")}
            if "content" in r and r.get("content") is not None:
                rec["content"] = r["content"]
            elif "summary" in r and r.get("summary") is not None:
                rec["content"] = r["summary"]
            else:
                rec["content"] = ""
            if hasattr(rec["date"], "isoformat"):
                rec["date"] = rec["date"].isoformat() if rec["date"] else None
            records.append(rec)
        logger.debug("JQData STK_NEWS_INFO %s: %s rows", symbol_ts, len(records))
        return records
    except Exception as e:
        logger.warning("JQData get_stock_news %s failed: %s", symbol_ts, e)
        return []


def get_stock_announcements(
    symbol_ts: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days_back: Optional[int] = 30,
    limit: int = 200,
) -> List[dict]:
    """
    上市公司公告：finance.STK_ANN_REPORT（公告 PDF/HTML 链接及基本信息）。
    返回 list of dict：title, pub_date, announcement_type, file_link, code 等。
    """
    if not _ensure_auth():
        return []
    jq_code = ts_to_jqcode(symbol_ts)
    start_ymd, end_ymd = _norm_ymd(start_date, end_date, days_back)
    if not start_ymd or not end_ymd:
        end_d = datetime.now(timezone.utc).date()
        start_ymd = (end_d - timedelta(days=30)).isoformat()
        end_ymd = end_d.isoformat()
    try:
        from jqdatasdk import finance, query

        q = (
            query(finance.STK_ANN_REPORT)
            .filter(finance.STK_ANN_REPORT.code == jq_code)
            .filter(finance.STK_ANN_REPORT.pub_date >= start_ymd)
            .filter(finance.STK_ANN_REPORT.pub_date <= end_ymd)
            .limit(limit)
        )
        df = finance.run_query(q)
        if df is None or df.empty:
            return []
        records = []
        for _, r in df.iterrows():
            rec = {
                "code": jqcode_to_ts(str(r.get("code", ""))),
                "title": r.get("title"),
                "date": r.get("pub_date"),
                "announcement_type": r.get("announcement_type"),
                "file_link": r.get("file_link"),
            }
            if hasattr(rec["date"], "isoformat"):
                rec["date"] = rec["date"].isoformat() if rec["date"] else None
            records.append(rec)
        logger.debug("JQData STK_ANN_REPORT %s: %s rows", symbol_ts, len(records))
        return records
    except Exception as e:
        logger.warning("JQData get_stock_announcements %s failed: %s", symbol_ts, e)
        return []


def get_valuation_or_fundamentals(symbol: str) -> Optional[dict]:
    """
    拉取单只标的估值/财务摘要（用于 industry_revenue 写入 L2）。
    返回可 json 序列化的 dict，无数据返回 None。
    """
    if not _ensure_auth():
        return None
    jq_code = ts_to_jqcode(symbol)
    try:
        from datetime import date, timedelta

        from jqdatasdk import get_valuation

        end_d = date.today()
        start_d = end_d - timedelta(days=60)
        # get_valuation(security, start_date, end_date) 或类似签名
        df = get_valuation(security=jq_code, start_date=start_d, end_date=end_d)
        if df is not None and not df.empty:
            row = df.iloc[-1].to_dict()
            out = {}
            for k, v in row.items():
                if hasattr(v, "isoformat"):
                    out[k] = v.isoformat()
                else:
                    out[k] = v
            return out
        return None
    except Exception as e:
        logger.debug("JQData get_valuation %s: %s", jq_code, e)
        return None
