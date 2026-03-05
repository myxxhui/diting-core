# [Ref: 03_原子目标与规约/Stage2_数据采集与存储/02_采集逻辑与Dockerfile_设计.md#design-stage2-02-integration-akshare]
# ingest_ohlcv：AkShare A 股日线 → L1 ohlcv

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import psycopg2

from diting.ingestion.config import get_timescale_dsn
from diting.ingestion.l1_writer import write_ohlcv_batch

logger = logging.getLogger(__name__)

# 逻辑填充期：ingest-test 目标 symbol 与 period（见 docs/ingest-test-target.md）
DEFAULT_SYMBOLS = ["000001", "600000"]  # 平安银行(SZ)、浦发银行(SH)
# 测试集 15 标（少量真实行情）：生产数据环境步骤 3、7 使用真实 AkShare 拉取
REAL_TEST_SYMBOLS_15 = [
    "000001", "600000", "000002", "600519", "000858", "601318", "000333", "600036",
    "002594", "601012", "000725", "300750", "603259", "688981", "300059",
]
DEFAULT_PERIOD = "daily"

# 标与标之间间隔（秒），减轻东方财富限流；可通过 INGEST_OHLCV_DELAY_BETWEEN_SYMBOLS 覆盖
# 全 A 股约 5000+ 标：2s 仅间隔约 2.8h，可设为 0 或 0.5 缩短时间（自担限流/断连风险）
def _delay_between_symbols_sec() -> float:
    raw = os.environ.get("INGEST_OHLCV_DELAY_BETWEEN_SYMBOLS", "2").strip()
    try:
        v = float(raw)
        return max(0.0, v)
    except ValueError:
        return 2.0


def _concurrent_workers() -> int:
    """INGEST_OHLCV_CONCURRENT：并发数，默认 1（串行）；全 A 股可设 3～5 配合限速使用。"""
    raw = os.environ.get("INGEST_OHLCV_CONCURRENT", "1").strip()
    try:
        return max(1, min(16, int(raw)))
    except ValueError:
        return 1


def _rate_per_sec() -> float:
    """INGEST_OHLCV_RATE_PER_SEC：全局限速（请求/秒），并发时生效，默认 1.0。"""
    raw = os.environ.get("INGEST_OHLCV_RATE_PER_SEC", "1.0").strip()
    try:
        return max(0.2, min(5.0, float(raw)))
    except ValueError:
        return 1.0


class _RateLimiter:
    """全局限速：多线程下保证每秒不超过 rate 次请求。"""
    __slots__ = ("_lock", "_last", "_interval")

    def __init__(self, rate_per_sec: float):
        self._lock = threading.Lock()
        self._last = 0.0
        self._interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 1.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait_until = self._last + self._interval
            if now < wait_until:
                time.sleep(wait_until - now)
                now = wait_until
            self._last = now


def _is_mock() -> bool:
    """DITING_INGEST_MOCK=1 时使用本地 mock 数据，不请求外网（CI/无外网环境）。"""
    return os.environ.get("DITING_INGEST_MOCK", "").strip().lower() in ("1", "true", "yes")


def _symbol_to_ts(symbol: str) -> str:
    """A 股代码转 exchange 后缀：6xxxxx -> .SH，否则 .SZ"""
    if symbol.startswith("6"):
        return f"{symbol}.SH"
    return f"{symbol}.SZ"


def _symbol_to_baostock_code(symbol: str) -> str:
    """A 股代码转 Baostock 代码：6xxxxx -> sh.6xxxxx，否则 sz.xxxxxx"""
    if symbol.startswith("6"):
        return f"sh.{symbol}"
    return f"sz.{symbol}"


def _get_ohlcv_source() -> str:
    """INGEST_OHLCV_SOURCE：akshare（默认）或 baostock。东方财富频繁断连时可设为 baostock。"""
    raw = (os.environ.get("INGEST_OHLCV_SOURCE") or "akshare").strip().lower()
    return "baostock" if raw == "baostock" else "akshare"


def _fetch_baostock_ohlcv(
    symbol: str,
    start_date: str,
    end_date: str,
    adjustflag: str = "2",
) -> list:
    """
    使用 Baostock 拉取 A 股日线。当东方财富接口持续 RemoteDisconnected 时可改用此源。
    日期格式：start_date/end_date 为 YYYYMMDD，内部会转为 YYYY-MM-DD。
    """
    import baostock as bs

    start_ymd = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}" if len(start_date) >= 8 else start_date
    end_ymd = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}" if len(end_date) >= 8 else end_date
    code = _symbol_to_baostock_code(symbol)
    symbol_ts = _symbol_to_ts(symbol)
    rows = []
    try:
        lg = bs.login()
        if lg.error_code != "0":
            logger.warning("baostock login failed: %s %s", lg.error_code, lg.error_msg)
            return []
        rs = bs.query_history_k_data_plus(
            code,
            "date,open,high,low,close,volume",
            start_date=start_ymd,
            end_date=end_ymd,
            frequency="d",
            adjustflag=adjustflag,
        )
        if rs.error_code != "0":
            logger.warning("baostock query %s failed: %s %s", code, rs.error_code, rs.error_msg)
            return []
        while rs.next():
            row = rs.get_row_data()
            if not row or len(row) < 6:
                continue
            date_s, o, h, l, c, vol = row[0], row[1], row[2], row[3], row[4], row[5]
            try:
                dt = datetime.strptime(date_s[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                continue
            try:
                vol_int = int(float(vol)) if vol else 0
            except (TypeError, ValueError):
                vol_int = 0
            rows.append(
                (
                    symbol_ts,
                    DEFAULT_PERIOD,
                    dt,
                    float(o),
                    float(h),
                    float(l),
                    float(c),
                    vol_int,
                )
            )
    finally:
        try:
            bs.logout()
        except Exception:
            pass
    return rows


def _fetch_akshare_ohlcv(
    symbol: str,
    period: str,
    start_date: str,
    end_date: str,
    adjust: str = "",
    max_retries: int = 5,
    retry_delay: float = 5.0,
) -> list:
    """
    AkShare 拉取 A 股日线。东方财富接口易出现 RemoteDisconnected，加重试+退避+浏览器头。
    [Ref: design-stage2-02-integration-akshare]
    """
    import time

    import requests
    import akshare as ak

    # 东方财富数据接口常对非浏览器 UA 或缺失 Referer 断连，临时为 requests 注入浏览器头
    _orig_get = requests.get
    def _get_with_headers(url, *args, **kwargs):
        if "eastmoney.com" in (url or ""):
            h = kwargs.setdefault("headers", {})
            h.setdefault("User-Agent", (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ))
            h.setdefault("Referer", "https://quote.eastmoney.com/")
        return _orig_get(url, *args, **kwargs)
    requests.get = _get_with_headers
    try:
        for attempt in range(max_retries):
            try:
                if attempt == 0:
                    time.sleep(1)  # 首请求前短延迟，降低被接口误判为脚本的概率
                df = ak.stock_zh_a_hist(
                    symbol=symbol,
                    period=period,
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust,
                )
                if df is None or df.empty:
                    return []
                # 列名：日期, 开盘, 收盘, 最高, 最低, 成交量, ...
                df = df.rename(
                    columns={
                        "日期": "date",
                        "开盘": "open",
                        "收盘": "close",
                        "最高": "high",
                        "最低": "low",
                        "成交量": "volume",
                    }
                )
                rows = []
                for _, r in df.iterrows():
                    dt = r["date"]
                    if hasattr(dt, "to_pydatetime"):
                        dt = dt.to_pydatetime()
                    elif isinstance(dt, str):
                        dt = datetime.strptime(dt[:10], "%Y-%m-%d")
                    else:
                        from datetime import date as date_type
                        if isinstance(dt, date_type) and not isinstance(dt, datetime):
                            dt = datetime.combine(dt, datetime.min.time())
                    # 确保 timezone-aware（UTC 存）
                    if getattr(dt, "tzinfo", None) is None:
                        from datetime import timezone
                        dt = dt.replace(tzinfo=timezone.utc)
                    symbol_ts = _symbol_to_ts(symbol)
                    vol = r["volume"]
                    try:
                        vol_int = int(float(vol)) if vol == vol else 0
                    except (TypeError, ValueError):
                        vol_int = 0
                    rows.append(
                        (
                            symbol_ts,
                            DEFAULT_PERIOD,
                            dt,
                            float(r["open"]),
                            float(r["high"]),
                            float(r["low"]),
                            float(r["close"]),
                            vol_int,
                        )
                    )
                return rows
            except Exception as e:
                logger.warning("akshare stock_zh_a_hist attempt %s failed: %s", attempt + 1, e)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    raise
        return []
    finally:
        requests.get = _orig_get


def _fetch_ohlcv(symbol: str, period: str, start_str: str, end_str: str, adjust: str = "") -> list:
    """按 INGEST_OHLCV_SOURCE 选择 akshare 或 baostock 拉取日线；东方财富持续断连时请设 baostock。"""
    if _get_ohlcv_source() == "baostock":
        return _fetch_baostock_ohlcv(symbol, start_str, end_str)
    return _fetch_akshare_ohlcv(symbol, period, start_str, end_str, adjust=adjust)


def _mock_ohlcv_rows(symbols: list, period: str, days: int = 15) -> list:
    """Mock 数据：与 docs/ingest-test-target.md 约定一致，供无外网时 V-INGEST/V-DATA 验证。"""
    rows = []
    end = datetime.now(timezone.utc)
    for sym in symbols:
        sym_ts = _symbol_to_ts(sym)
        for i in range(days):
            dt = end - timedelta(days=days - 1 - i)
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            base = 10.0 + (hash(sym) % 100) / 10.0
            rows.append(
                (sym_ts, period, dt, base, base + 0.5, base - 0.2, base + 0.1, 1000000 + i * 1000)
            )
    return rows


def run_ingest_ohlcv(
    symbols: list = None,
    period: str = DEFAULT_PERIOD,
    days_back: int = 30,
) -> int:
    """
    执行 ingest_ohlcv：从 AkShare 拉取 A 股日线并写入 L1 ohlcv。
    工作目录: diting-core（由 Makefile / 调用方保证）
    真实模式（未设置 DITING_INGEST_MOCK）：symbols 为空时使用 REAL_TEST_SYMBOLS_15（约 15 标），拉取真实行情。
    DITING_INGEST_MOCK=1 时写入 mock 数据（仅用于非生产流水线，如 CI/无外网）。
    """
    if _is_mock():
        symbols = symbols or DEFAULT_SYMBOLS
        all_rows = _mock_ohlcv_rows(symbols, period, days=15)
        logger.info("ingest_ohlcv: mock mode, %s rows", len(all_rows))
    else:
        symbols = symbols or REAL_TEST_SYMBOLS_15
        end = datetime.utcnow()
        start = end - timedelta(days=days_back)
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        all_rows = []
        source = _get_ohlcv_source()
        workers = _concurrent_workers()
        if workers > 1:
            # 可控并发 + 全局限速：全 A 股约 5000 标、1.5 req/s 约 55 分钟
            rate = _rate_per_sec()
            limiter = _RateLimiter(rate)
            logger.info("OHLCV 拉取：数据源=%s 并发=%s 限速=%.1f 次/秒（来自 .env 或 Make 默认）", source, workers, rate)

            def _task(sym: str):
                limiter.wait()
                return (sym, _fetch_ohlcv(sym, period, start_str, end_str))

            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = {ex.submit(_task, sym): sym for sym in symbols}
                for fut in as_completed(futures):
                    sym = futures[fut]
                    try:
                        _, rows = fut.result()
                        all_rows.extend(rows)
                    except Exception as e:
                        logger.exception("ingest_ohlcv symbol=%s failed: %s", sym, e)
                        raise
        else:
            logger.info("OHLCV 拉取：数据源=%s 串行（CONCURRENT=1）", source)
            for i, sym in enumerate(symbols):
                try:
                    rows = _fetch_ohlcv(sym, period, start_str, end_str)
                    all_rows.extend(rows)
                    delay = _delay_between_symbols_sec()
                    if delay > 0 and i < len(symbols) - 1:
                        time.sleep(delay)
                except Exception as e:
                    logger.exception("ingest_ohlcv symbol=%s failed: %s", sym, e)
                    raise
        if not all_rows:
            logger.warning("ingest_ohlcv: no rows fetched for symbols=%s", symbols)
            return 0

    dsn = get_timescale_dsn()
    conn = psycopg2.connect(dsn)
    try:
        n = write_ohlcv_batch(conn, all_rows)
        return n
    finally:
        conn.close()
