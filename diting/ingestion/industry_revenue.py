# [Ref: 03_原子目标与规约/Stage2_数据采集与存储/02_采集逻辑与Dockerfile_设计.md#design-stage2-02-integration-akshare]
# ingest_industry_revenue：AkShare 行业/财报/营收 → 约定表或 L2 版本化（Module A 输入）

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import psycopg2

from diting.ingestion.config import get_pg_l2_dsn
from diting.ingestion.l2_writer import write_data_version

logger = logging.getLogger(__name__)

# 静态行业回退文件（当 API 不可达时使用，使 Module A 能输出 AGRI/TECH/GEO）
_FALLBACK_CSV = Path(__file__).resolve().parents[2] / "config" / "industry_fallback.csv"
_fallback_cache: Optional[dict] = None


def _load_industry_fallback(symbol: str) -> Optional[str]:
    """从 config/industry_fallback.csv 读取 symbol 对应 industry_name；含 电子/计算机/通信/有色金属/石油石化/农林牧渔 等与 classifier_rules 一致。"""
    global _fallback_cache
    symbol = (symbol or "").strip().upper()
    if not symbol:
        return None
    if _fallback_cache is None:
        _fallback_cache = {}
        if _FALLBACK_CSV.exists():
            try:
                with open(_FALLBACK_CSV, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip().split("#")[0].strip()
                        if not line or line.lower().startswith("symbol"):
                            continue
                        parts = line.split(",", 1)
                        if len(parts) >= 2:
                            _fallback_cache[parts[0].strip().upper()] = parts[1].strip()
            except Exception as e:
                logger.debug("load industry_fallback.csv: %s", e)
    return _fallback_cache.get(symbol)


# L2 或 API 可能写入「-」等占位，与空字符串一样应触发 industry_fallback
_PLACEHOLDER_INDUSTRY_NAMES = frozenset(
    ("-", "—", "－", "--", "未知", "N/A", "n/a", "None", "null", "NULL")
)


def industry_name_needs_fallback(name: str) -> bool:
    """行业名为空或为占位符时返回 True，应使用 industry_fallback.csv 或「未知」。"""
    s = (name or "").strip()
    if not s:
        return True
    return s in _PLACEHOLDER_INDUSTRY_NAMES


def _safe_float(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        f = float(v)
        import math
        return default if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return default


def _parse_industry_revenue_row(data: dict) -> tuple:
    """从采集得到的 dict 中解析 (industry_name, revenue_ratio, rnd_ratio, commodity_ratio)。

    字段语义：
      revenue_ratio  — 主营业务利润率（AkShare: 主营业务利润率(%) 或 销售毛利率(%)）
      rnd_ratio      — 三项费用比重（管理+销售+财务费用占营收比例，非独立研发费率）
      commodity_ratio — 大宗商品营收占比（由行业名规则估算）
    """
    industry_name = ""
    revenue_ratio = 0.0
    rnd_ratio = 0.0
    commodity_ratio = 0.0
    for k, v in (data or {}).items():
        k = str(k).strip() if k is not None else ""
        if not k:
            continue
        if k in ("行业", "所属行业", "申万行业"):
            industry_name = str(v).strip() if v is not None else ""
        elif k in ("主营业务收入占比", "主营营收占比", "revenue_ratio"):
            revenue_ratio = _safe_float(v)
        elif k in ("研发投入占比", "研发支出占比", "rnd_ratio"):
            rnd_ratio = _safe_float(v)
        elif k in ("大宗商品营收占比", "commodity_ratio"):
            commodity_ratio = _safe_float(v)
        elif k == "主营业务利润率(%)" and revenue_ratio == 0.0:
            revenue_ratio = _safe_float(v) / 100.0
        elif k == "销售毛利率(%)" and revenue_ratio == 0.0:
            revenue_ratio = _safe_float(v) / 100.0
        elif k == "三项费用比重" and rnd_ratio == 0.0:
            rnd_ratio = _safe_float(v) / 100.0
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


def _use_industry_fallback_only() -> bool:
    """INGEST_INDUSTRY_USE_FALLBACK_ONLY=1 时跳过东方财富，仅用 config/industry_fallback.csv 写 L2。适用于境外/香港等无法访问东方财富的环境。"""
    return os.environ.get("INGEST_INDUSTRY_USE_FALLBACK_ONLY", "").strip().lower() in ("1", "true", "yes")


def _apply_akshare_proxy() -> None:
    """在调用 akshare 前设置 HTTP_PROXY/HTTPS_PROXY，使 requests 走代理。东方财富在境外/香港易 RemoteDisconnected，可设 INGEST_HTTP_PROXY/INGEST_HTTPS_PROXY 为境内代理地址。"""
    for env_key, target_key in (
        ("INGEST_HTTP_PROXY", "HTTP_PROXY"),
        ("INGEST_HTTPS_PROXY", "HTTPS_PROXY"),
    ):
        val = os.environ.get(env_key, "").strip()
        if val and os.environ.get(target_key) is None:
            os.environ[target_key] = val


def _fetch_jqdata_financial(symbol: str):
    """从 JQData 拉取估值/财务摘要，返回 dict 或 None。"""
    try:
        from diting.ingestion.jqdata_client import get_valuation_or_fundamentals

        return get_valuation_or_fundamentals(symbol)
    except ImportError:
        return None


def _fetch_akshare_individual_info_em(
    symbol: str,
    max_retries: int = 3,
    retry_delay: float = 2.0,
):
    """东方财富个股信息：含「行业」等，返回 item->value 的 dict，供 Module A 行业分类。
    优先使用本接口以写入有效 industry_name；占比字段若接口未提供则仍为 0。
    注意：东方财富接口可能在某些网络环境（如境外/香港节点）不可达（RemoteDisconnected），
    可设 INGEST_HTTP_PROXY/INGEST_HTTPS_PROXY 为境内代理，或 INGEST_INDUSTRY_USE_FALLBACK_ONLY=1 仅用静态回退。"""
    _apply_akshare_proxy()
    import akshare as ak

    for attempt in range(max_retries):
        try:
            df = ak.stock_individual_info_em(symbol=symbol)
            if df is None or df.empty:
                return None
            if "item" in df.columns and "value" in df.columns:
                # 转为 dict，key 为 item 列（如 行业、总市值），value 为 value 列
                data = {}
                for _, row in df.iterrows():
                    k = row.get("item")
                    v = row.get("value")
                    if k is not None and str(k).strip():
                        data[str(k).strip()] = v
                return data if data else None
            return None
        except Exception as e:
            logger.warning("akshare stock_individual_info_em attempt %s failed: %s", attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            else:
                raise
    return None


def _fetch_akshare_financial_abstract(
    symbol: str,
    max_retries: int = 3,
    retry_delay: float = 2.0,
):
    """AkShare 股票财务摘要（日期列结构）；用于 data_versions 落库或东方财富接口失败时的回退。
    本接口返回结构不含「行业」「主营业务收入占比」，无法解析出有效行业/占比，仅作版本记录或兜底。"""
    _apply_akshare_proxy()
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


def _fetch_akshare_financial_indicator(
    symbol: str,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> Optional[dict]:
    """从 stock_financial_analysis_indicator 获取财务分析指标（如主营业务利润率、销售毛利率、三项费用比重等），
    返回最新一期数据的 dict（列名→值），供 _parse_industry_revenue_row 解析 revenue_ratio/rnd_ratio。"""
    _apply_akshare_proxy()
    import akshare as ak

    if not hasattr(ak, "stock_financial_analysis_indicator"):
        return None
    from datetime import datetime as _dt
    start_year = str(_dt.now().year - 1)
    for attempt in range(max_retries):
        try:
            df = ak.stock_financial_analysis_indicator(symbol=symbol, start_year=start_year)
            if df is None or df.empty:
                return None
            latest = df.iloc[-1]
            return {str(col): latest[col] for col in df.columns}
        except Exception as e:
            logger.warning("akshare stock_financial_analysis_indicator attempt %s failed: %s", attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            else:
                return None
    return None


def _ensure_industry_and_ratios(symbol: str, data: dict) -> tuple:
    """从 data 解析 industry/ratios，行业名为空时无条件走 fallback CSV 补全；返回 (iname, rev, rnd, comm)。"""
    iname, rev, rnd, comm = _parse_industry_revenue_row(data)
    if not (iname or "").strip():
        fallback = _load_industry_fallback(symbol)
        if fallback:
            iname = fallback
            logger.info("industry_revenue: symbol=%s 行业由 fallback 补全 → %s", symbol, iname)
        else:
            logger.warning("industry_revenue: symbol=%s 行业为空（API 未返回、fallback 无匹配）", symbol)
    return iname, rev, rnd, comm


_COMMODITY_KEYWORDS = (
    "有色金属", "工业金属", "贵金属", "稀有金属", "金属",
    "石油石化", "石油", "石化", "油气", "油服",
    "煤炭", "焦煤", "动力煤",
    "钢铁", "铁矿", "特钢", "普钢",
    "化工", "基础化学", "化学", "化肥", "农药",
)


def _estimate_commodity_ratio(industry_name: str) -> float:
    """根据行业名估算大宗商品营收占比（无 API 直接提供此字段）。
    匹配申万一/二/三级行业关键词。"""
    if not industry_name:
        return 0.0
    for keyword in _COMMODITY_KEYWORDS:
        if keyword in industry_name:
            return 0.7
    return 0.0


def run_ingest_industry_revenue(symbol: str = None) -> int:
    """
    执行 ingest_industry_revenue：从 AkShare 拉取财务摘要并写入 L2 data_versions 与 industry_revenue_summary。
    工作目录: diting-core。symbol 建议为带交易所后缀（如 000001.SZ），与 get_current_a_share_universe 一致；
    调用外部 API 时使用无后缀代码，写入 industry_revenue_summary 时保留传入的 symbol 供 Module A 查询。

    采集策略（AkShare 源）：
    1. stock_individual_info_em → 行业名
    2. stock_financial_analysis_indicator → 财务指标（主营利润率→revenue_ratio、三项费用比重→rnd_ratio）
    3. 行业名为空时无条件使用 industry_fallback.csv 补全
    4. 大宗商品占比由行业名规则估算
    """
    symbol = (symbol or DEFAULT_SYMBOL).strip()
    api_symbol = symbol.split(".")[0] if "." in symbol else symbol
    now = datetime.now(timezone.utc)
    version_id = f"industry_revenue_{symbol}_{now.strftime('%Y%m%d%H%M%S')}"
    file_path = f"l2/industry_revenue/{symbol}.json"

    if _is_mock():
        file_size = len(b'{"mock": true}')
        dsn = get_pg_l2_dsn()
        conn = psycopg2.connect(dsn)
        try:
            write_data_version(conn, data_type=DATA_TYPE, version_id=version_id,
                               timestamp=now, file_path=file_path, file_size=file_size, checksum="")
            logger.info("ingest_industry_revenue: mock mode, 1 version")
            return 1
        finally:
            conn.close()

    source = _get_ingest_source()

    # --- JQData 路径 ---
    if source == "jqdata":
        data = _fetch_jqdata_financial(api_symbol)
        if data:
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
                write_data_version(conn, data_type=DATA_TYPE, version_id=version_id,
                                   timestamp=now, file_path=file_path, file_size=file_size, checksum="")
                try:
                    iname, rev, rnd, comm = _ensure_industry_and_ratios(symbol, data)
                    if comm == 0.0:
                        comm = _estimate_commodity_ratio(iname)
                    _upsert_industry_revenue_summary(conn, symbol, iname, rev, rnd, comm)
                except Exception as e:
                    logger.warning("upsert industry_revenue_summary failed: %s", e)
                return 1
            finally:
                conn.close()
        logger.debug("ingest_industry_revenue: no jqdata for symbol=%s, try akshare", symbol)

    # --- AkShare 路径 ---
    # 仅用 fallback CSV 模式（境外/香港无法访问东方财富时启用）
    if _use_industry_fallback_only():
        iname = _load_industry_fallback(symbol) or ""
        if not iname:
            logger.warning("industry_revenue: FALLBACK_ONLY 但 symbol=%s 无 fallback 匹配", symbol)
            return 0
        dsn = get_pg_l2_dsn()
        conn = psycopg2.connect(dsn)
        try:
            fallback_data = {"行业": iname, "source": "fallback_only"}
            payload = json.dumps(fallback_data, ensure_ascii=False)
            write_data_version(conn, data_type=DATA_TYPE, version_id=version_id,
                               timestamp=now, file_path=file_path,
                               file_size=len(payload.encode("utf-8")), checksum="")
            comm = _estimate_commodity_ratio(iname)
            _upsert_industry_revenue_summary(conn, symbol, iname, 0.0, 0.0, comm)
            logger.info("industry_revenue: symbol=%s fallback_only → %s", symbol, iname)
            return 1
        finally:
            conn.close()

    # 步骤 1: 东方财富个股信息（行业名）
    info_data = None
    try:
        info_data = _fetch_akshare_individual_info_em(api_symbol)
    except Exception as e:
        logger.warning("stock_individual_info_em failed for %s: %s", symbol, e)

    # 步骤 2: 财务分析指标（利润率/费用比重等）
    fin_data = None
    try:
        fin_data = _fetch_akshare_financial_indicator(api_symbol)
    except Exception as e:
        logger.debug("stock_financial_analysis_indicator failed for %s: %s", symbol, e)

    # 合并：info_data 提供行业名，fin_data 提供财务指标
    merged = {}
    if fin_data:
        merged.update(fin_data)
    if info_data:
        merged.update(info_data)

    # 步骤 3: 都没数据时尝试 financial_abstract 兜底
    if not merged:
        try:
            df = _fetch_akshare_financial_abstract(api_symbol)
            if df is not None and not df.empty:
                first = df.iloc[0].to_dict()
                for k, v in first.items():
                    if hasattr(v, "isoformat"):
                        first[k] = v.isoformat()
                merged = first
        except Exception as e:
            logger.debug("stock_financial_abstract fallback failed for %s: %s", symbol, e)

    # 步骤 4: API 全部失败 → 仅用 fallback CSV 保底（确保 Module A 至少有行业名）
    if not merged:
        iname = _load_industry_fallback(symbol) or ""
        if iname:
            logger.info("industry_revenue: symbol=%s API 全部失败，仅用 fallback → %s", symbol, iname)
            dsn = get_pg_l2_dsn()
            conn = psycopg2.connect(dsn)
            try:
                fallback_data = {"行业": iname, "source": "fallback_api_failed"}
                payload = json.dumps(fallback_data, ensure_ascii=False)
                write_data_version(conn, data_type=DATA_TYPE, version_id=version_id,
                                   timestamp=now, file_path=file_path,
                                   file_size=len(payload.encode("utf-8")), checksum="")
                comm = _estimate_commodity_ratio(iname)
                _upsert_industry_revenue_summary(conn, symbol, iname, 0.0, 0.0, comm)
                return 1
            finally:
                conn.close()
        logger.warning(
            "ingest_industry_revenue: no data for symbol=%s（API 全部失败、fallback 无匹配）",
            symbol,
        )
        return 0

    # 序列化 & 写入
    try:
        for k, v in list(merged.items()):
            if hasattr(v, "isoformat"):
                merged[k] = v.isoformat()
        payload = json.dumps(merged, ensure_ascii=False, default=str)
        file_size = len(payload.encode("utf-8"))
    except Exception:
        file_size = 0

    dsn = get_pg_l2_dsn()
    conn = psycopg2.connect(dsn)
    try:
        write_data_version(conn, data_type=DATA_TYPE, version_id=version_id,
                           timestamp=now, file_path=file_path, file_size=file_size, checksum="")
        try:
            iname, rev, rnd, comm = _ensure_industry_and_ratios(symbol, merged)
            if comm == 0.0:
                comm = _estimate_commodity_ratio(iname)
            _upsert_industry_revenue_summary(conn, symbol, iname, rev, rnd, comm)
        except Exception as e:
            logger.warning("upsert industry_revenue_summary failed: %s", e)
        return 1
    finally:
        conn.close()
