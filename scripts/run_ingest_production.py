#!/usr/bin/env python3
"""
生产级数据采集入口（支持智能增量 / 强制全量）。

INGEST_MODE:
  auto  — 智能增量：按维度检测缺失/过期数据，只补差量（默认、日常使用）
  full  — 强制全量：忽略已有数据，全部重新拉取（首次建库 / 数据修复）

各维度增量策略:
  K 线  — DB 最新日期距今 > INGEST_OHLCV_STALE_DAYS 才补拉，
          拉取范围 = [max_date - OVERLAP_DAYS, today]
  行业  — updated_at 超过 INGEST_INDUSTRY_REFRESH_DAYS 天重新拉取
  财务  — updated_at 超过 INGEST_FINANCIAL_REFRESH_DAYS 天重新拉取
  新闻  — INGEST_NEWS_ALWAYS_REFRESH=true 时每次都拉最新（UPSERT 去重）
"""

import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

_env_file = root / ".env"
if _env_file.exists():
    with open(_env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if k and os.environ.get(k) is None:
                os.environ[k] = v

from diting.ingestion import (
    run_ingest_universe,
    run_ingest_ohlcv,
    run_ingest_industry_revenue,
    run_ingest_news,
    run_ingest_financial,
)
from diting.ingestion.config import get_pg_l2_dsn, get_timescale_dsn
from diting.universe import get_current_a_share_universe, parse_symbol_list_from_env

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

DAYS_BACK_DEFAULT = 5 * 365
PROGRESS_FILE = root / ".ingest_production_progress"

# ===== .env 配置读取 =====


def _env_bool(key: str, default: bool = True) -> bool:
    raw = (os.environ.get(key) or "").strip().lower()
    if raw in ("false", "0", "no"):
        return False
    if raw in ("true", "1", "yes"):
        return True
    return default


def _env_int(key: str, default: int, lo: int = 0, hi: int = 99999) -> int:
    raw = (os.environ.get(key) or "").strip()
    try:
        return max(lo, min(hi, int(raw)))
    except (ValueError, TypeError):
        return default


def _env_float(key: str, default: float, lo: float = 0.0, hi: float = 600.0) -> float:
    raw = (os.environ.get(key) or "").strip()
    try:
        return max(lo, min(hi, float(raw)))
    except (ValueError, TypeError):
        return default


def _ingest_enabled(scope: str) -> bool:
    return _env_bool(f"INGEST_PRODUCTION_{scope}", default=True)


def _get_ingest_mode() -> str:
    raw = (os.environ.get("INGEST_MODE") or "auto").strip().lower()
    return raw if raw in ("auto", "full") else "auto"


def _ohlcv_stale_days() -> int:
    return _env_int("INGEST_OHLCV_STALE_DAYS", 1, 0, 365)


def _ohlcv_overlap_days() -> int:
    return _env_int("INGEST_OHLCV_OVERLAP_DAYS", 3, 0, 30)


def _industry_refresh_days() -> int:
    return _env_int("INGEST_INDUSTRY_REFRESH_DAYS", 30, 1, 365)


def _financial_refresh_days() -> int:
    return _env_int("INGEST_FINANCIAL_REFRESH_DAYS", 7, 1, 365)


def _news_always_refresh() -> bool:
    return _env_bool("INGEST_NEWS_ALWAYS_REFRESH", default=True)


def _batch_size() -> int:
    return _env_int("INGEST_OHLCV_BATCH_SIZE", 15, 1, 500)


def _batch_pause_sec() -> float:
    return _env_float("INGEST_OHLCV_BATCH_PAUSE_SEC", 60.0, 0.0, 300.0)


def _extra_delay_sec() -> float:
    return _env_float("INGEST_EXTRA_DELAY_SEC", 2.0, 0.0, 30.0)


def _separate_phases() -> bool:
    return _env_bool("INGEST_SEPARATE_PHASES", default=False)


def _phase_pause_sec() -> float:
    return _env_float("INGEST_PHASE_PAUSE_SEC", 30.0)


def _resume_enabled() -> bool:
    return _env_bool("INGEST_PRODUCTION_RESUME", default=True)


def _unified_date_range():
    start_raw = (os.environ.get("INGEST_JQDATA_DATE_START") or "").strip()
    end_raw = (os.environ.get("INGEST_JQDATA_DATE_END") or "").strip()
    if not start_raw or not end_raw:
        return None, None
    s = start_raw.replace("-", "")[:8]
    e = end_raw.replace("-", "")[:8]
    if len(s) != 8 or len(e) != 8:
        return None, None
    try:
        int(s)
        int(e)
    except ValueError:
        return None, None
    start_str = f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    end_str = f"{e[:4]}-{e[4:6]}-{e[6:8]}"
    if start_str > end_str:
        start_str, end_str = end_str, start_str
    return start_str, end_str


def _ohlcv_days_back() -> int:
    if not _ingest_enabled("OHLCV"):
        return DAYS_BACK_DEFAULT
    start_str, end_str = _unified_date_range()
    if start_str and end_str:
        try:
            d1 = datetime.strptime(start_str, "%Y-%m-%d")
            d2 = datetime.strptime(end_str, "%Y-%m-%d")
            return max(1, (d2 - d1).days)
        except (ValueError, TypeError):
            pass
    return _env_int("INGEST_PRODUCTION_OHLCV_DAYS_BACK", DAYS_BACK_DEFAULT, 1, 365 * 20)


def _news_days_back() -> int:
    if not _ingest_enabled("NEWS"):
        return 0
    if _unified_date_range()[0]:
        return 0
    return _env_int("INGEST_PRODUCTION_NEWS_DAYS_BACK", 365, 1, 365 * 5)


# ===== DB 查询：增量状态检测 =====


def _pg_connect(dsn_func):
    import psycopg2
    return psycopg2.connect(dsn_func())


def _ohlcv_latest_dates_batch(symbols: list) -> dict:
    """返回 {symbol: max_date(date)} — 每个标的在 L1 的最新 K 线日期。"""
    if not symbols:
        return {}
    syms = [s.strip().upper() for s in symbols if (s or "").strip()]
    try:
        conn = _pg_connect(get_timescale_dsn)
        try:
            cur = conn.cursor()
            cur.execute(
                """SELECT symbol, MAX(datetime::date)
                   FROM ohlcv
                   WHERE symbol = ANY(%s) AND period IN ('day','daily')
                   GROUP BY symbol""",
                (syms,),
            )
            return {row[0]: row[1] for row in cur.fetchall()}
        finally:
            conn.close()
    except Exception as e:
        logger.warning("查询 K 线最新日期失败: %s", e)
        return {}


def _industry_updated_at_batch(symbols: list) -> dict:
    """返回 {symbol: updated_at(datetime)} — 行业数据的最后更新时间。"""
    if not symbols:
        return {}
    syms = [s.strip().upper() for s in symbols if (s or "").strip()]
    try:
        conn = _pg_connect(get_pg_l2_dsn)
        try:
            cur = conn.cursor()
            cur.execute(
                """SELECT symbol, MAX(updated_at)
                   FROM industry_revenue_summary
                   WHERE symbol = ANY(%s)
                     AND industry_name IS NOT NULL AND TRIM(industry_name) <> ''
                   GROUP BY symbol""",
                (syms,),
            )
            return {row[0]: row[1] for row in cur.fetchall()}
        finally:
            conn.close()
    except Exception as e:
        logger.warning("查询行业 updated_at 失败: %s", e)
        return {}


def _financial_updated_at_batch(symbols: list) -> dict:
    """返回 {symbol: max_updated_at(datetime)} — 财务数据的最后更新时间。"""
    if not symbols:
        return {}
    syms = [s.strip().upper() for s in symbols if (s or "").strip()]
    try:
        conn = _pg_connect(get_pg_l2_dsn)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT symbol, MAX(updated_at) FROM financial_summary WHERE symbol = ANY(%s) GROUP BY symbol",
                (syms,),
            )
            return {row[0]: row[1] for row in cur.fetchall()}
        finally:
            conn.close()
    except Exception as e:
        logger.warning("查询财务 updated_at 失败: %s", e)
        return {}


def _news_latest_dates_batch(symbols: list) -> dict:
    """返回 {symbol: max_published_at(datetime)} — 新闻最新发布时间。"""
    if not symbols:
        return {}
    syms = [s.strip().upper() for s in symbols if (s or "").strip()]
    try:
        conn = _pg_connect(get_pg_l2_dsn)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT symbol, MAX(published_at) FROM news_content WHERE symbol = ANY(%s) GROUP BY symbol",
                (syms,),
            )
            return {row[0]: row[1] for row in cur.fetchall()}
        finally:
            conn.close()
    except Exception as e:
        logger.warning("查询新闻最新日期失败: %s", e)
        return {}


# ===== 增量决策函数 =====


def _decide_ohlcv(sym_key: str, latest_map: dict, mode: str, today: date):
    """
    返回 (should_ingest: bool, start_date: str|None, end_date: str|None, reason: str)
    start_date/end_date 格式 YYYY-MM-DD；None 表示用默认全量回溯。
    """
    today_str = today.isoformat()
    if mode == "full":
        return True, None, None, "全量模式"

    latest = latest_map.get(sym_key)
    if latest is None:
        return True, None, None, "无历史数据，全量补录"

    stale = _ohlcv_stale_days()
    overlap = _ohlcv_overlap_days()
    days_gap = (today - latest).days

    if days_gap <= stale:
        return False, None, None, f"数据新鲜（最新 {latest}，距今 {days_gap} 天 <= {stale}）"

    start = latest - timedelta(days=overlap)
    return True, start.isoformat(), today_str, f"增量补拉 {start} → {today_str}（缺 {days_gap} 天）"


def _decide_industry(sym_key: str, updated_map: dict, mode: str, now: datetime):
    if mode == "full":
        return True, "全量模式"
    ts = updated_map.get(sym_key)
    if ts is None:
        return True, "无历史数据"
    refresh = _industry_refresh_days()
    if ts.tzinfo is None:
        age = (now.replace(tzinfo=None) - ts).days
    else:
        age = (now - ts).days
    if age > refresh:
        return True, f"已过期（{age} 天 > {refresh}）"
    return False, f"数据新鲜（{age} 天前更新）"


def _decide_financial(sym_key: str, updated_map: dict, mode: str, now: datetime):
    if mode == "full":
        return True, "全量模式"
    ts = updated_map.get(sym_key)
    if ts is None:
        return True, "无历史数据"
    refresh = _financial_refresh_days()
    if ts.tzinfo is None:
        age = (now.replace(tzinfo=None) - ts).days
    else:
        age = (now - ts).days
    if age > refresh:
        return True, f"已过期（{age} 天 > {refresh}）"
    return False, f"数据新鲜（{age} 天前更新）"


def _decide_news(sym_key: str, news_map: dict, mode: str):
    if mode == "full":
        return True, "全量模式"
    if _news_always_refresh():
        return True, "始终刷新（INGEST_NEWS_ALWAYS_REFRESH=true）"
    if sym_key not in news_map:
        return True, "无历史数据"
    return False, "已有数据且未启用始终刷新"


# ===== 进度断点 =====


def _read_progress() -> int:
    if not _resume_enabled() or not PROGRESS_FILE.exists():
        return 0
    try:
        with open(PROGRESS_FILE) as f:
            for line in f:
                line = line.strip()
                if line.startswith("completed_count="):
                    return max(0, int(line.split("=", 1)[1].strip()))
    except (ValueError, OSError):
        pass
    return 0


def _write_progress(completed_count: int) -> None:
    if not _resume_enabled():
        return
    try:
        with open(PROGRESS_FILE, "w") as f:
            f.write("completed_count=%d\n" % completed_count)
    except OSError as e:
        logger.warning("写入进度文件失败: %s", e)


def _clear_progress() -> None:
    if PROGRESS_FILE.exists():
        try:
            PROGRESS_FILE.unlink()
        except OSError:
            pass


# ===== K 线增量拉取：临时修改环境变量 =====


def _ingest_ohlcv_incremental(sym_raw: str, start_str: str, end_str: str) -> int:
    """临时覆盖 INGEST_JQDATA_DATE_* 以实现单标的增量拉取。"""
    saved = {
        "INGEST_JQDATA_DATE_START": os.environ.get("INGEST_JQDATA_DATE_START"),
        "INGEST_JQDATA_DATE_END": os.environ.get("INGEST_JQDATA_DATE_END"),
    }
    try:
        os.environ["INGEST_JQDATA_DATE_START"] = start_str
        os.environ["INGEST_JQDATA_DATE_END"] = end_str
        return run_ingest_ohlcv(symbols=[sym_raw], days_back=DAYS_BACK_DEFAULT)
    finally:
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v
            elif k in os.environ:
                del os.environ[k]


# ===== 主流程 =====


def main() -> int:
    if _env_bool("DITING_INGEST_MOCK", default=False):
        logger.error("生产采集禁止使用 DITING_INGEST_MOCK=1")
        return 1
    try:
        import akshare  # noqa: F401
    except ImportError:
        logger.error("缺少 akshare，请先 pip install akshare")
        return 1

    mode = _get_ingest_mode()
    today = date.today()
    now = datetime.now()
    logger.info("=" * 60)
    logger.info("采集模式: %s | 日期: %s", mode.upper(), today)
    logger.info("=" * 60)

    # ① 标的池
    specified = parse_symbol_list_from_env("DITING_SYMBOLS") or parse_symbol_list_from_env("INGEST_PRODUCTION_SYMBOLS")
    if specified:
        logger.info("指定标的: %s 只", len(specified))
    elif _ingest_enabled("UNIVERSE"):
        logger.info("刷新全 A 股 universe")
        run_ingest_universe()

    need = _ingest_enabled("OHLCV") or _ingest_enabled("INDUSTRY_REVENUE") or _ingest_enabled("NEWS") or _ingest_enabled("FINANCIAL")
    if not need:
        logger.info("所有维度均已关闭，无需采集")
        return 0

    symbols_full = specified or get_current_a_share_universe(force_refresh=False)
    if not symbols_full:
        logger.error("无标的")
        return 1
    symbols_raw = [s.split(".")[0] for s in symbols_full]
    total = len(symbols_raw)

    # ② 增量状态一次性批量查询
    logger.info("正在检测各维度数据状态（%s 只标的）…", total)
    ohlcv_latest = _ohlcv_latest_dates_batch(symbols_full) if _ingest_enabled("OHLCV") else {}
    industry_updated = _industry_updated_at_batch(symbols_full) if _ingest_enabled("INDUSTRY_REVENUE") else {}
    financial_updated = _financial_updated_at_batch(symbols_full) if _ingest_enabled("FINANCIAL") else {}
    news_latest = _news_latest_dates_batch(symbols_full) if _ingest_enabled("NEWS") else {}

    # 统计摘要
    if mode == "auto":
        ohlcv_skip = sum(1 for sf in symbols_full if not _decide_ohlcv(sf.strip().upper(), ohlcv_latest, mode, today)[0])
        ind_skip = sum(1 for sf in symbols_full if not _decide_industry(sf.strip().upper(), industry_updated, mode, now)[0])
        fin_skip = sum(1 for sf in symbols_full if not _decide_financial(sf.strip().upper(), financial_updated, mode, now)[0])
        news_skip = sum(1 for sf in symbols_full if not _decide_news(sf.strip().upper(), news_latest, mode)[0])
        logger.info(
            "增量检测: K线 需更新 %s/%s | 行业 需更新 %s/%s | 财务 需更新 %s/%s | 新闻 需更新 %s/%s",
            total - ohlcv_skip, total, total - ind_skip, total, total - fin_skip, total, total - news_skip, total,
        )

    # 全量模式参数
    days_back = _ohlcv_days_back() if _ingest_enabled("OHLCV") else 0
    unified_start, unified_end = _unified_date_range()
    news_days = _news_days_back()
    batch_size = _batch_size()
    batch_pause = _batch_pause_sec()
    delay_sec = _extra_delay_sec()

    # 全市场新闻（仅首次）
    if _ingest_enabled("NEWS"):
        if unified_start and unified_end:
            run_ingest_news(date_start=unified_start, date_end=unified_end)
        else:
            run_ingest_news(days_back=news_days if news_days > 0 else None)

    # ③ 按阶段执行
    if _separate_phases():
        _run_phases_separate(
            symbols_raw, symbols_full, total, mode, today, now,
            ohlcv_latest, industry_updated, financial_updated, news_latest,
            days_back, unified_start, unified_end, news_days,
            delay_sec, batch_size, batch_pause,
        )
    else:
        _run_phases_combined(
            symbols_raw, symbols_full, total, mode, today, now,
            ohlcv_latest, industry_updated, financial_updated, news_latest,
            days_back, unified_start, unified_end, news_days,
            delay_sec, batch_size, batch_pause,
        )

    _clear_progress()
    logger.info("=" * 60)
    logger.info("采集完成 (%s 模式)", mode.upper())
    logger.info("=" * 60)
    return 0


def _run_phases_separate(
    symbols_raw, symbols_full, total, mode, today, now,
    ohlcv_latest, industry_updated, financial_updated, news_latest,
    days_back, unified_start, unified_end, news_days,
    delay_sec, batch_size, batch_pause,
):
    phase_pause = _phase_pause_sec()

    # Phase 1: K 线
    if _ingest_enabled("OHLCV"):
        logger.info("=" * 50)
        logger.info("Phase 1: K 线 → L1（%s 只）", total)
        logger.info("=" * 50)
        cnt_done, cnt_skip = 0, 0
        for i, (sym_raw, sym_full) in enumerate(zip(symbols_raw, symbols_full)):
            sym_key = sym_full.strip().upper()
            should, inc_start, inc_end, reason = _decide_ohlcv(sym_key, ohlcv_latest, mode, today)
            if not should:
                cnt_skip += 1
                if i < 3 or (i + 1) == total:
                    logger.info("  [%s/%s] %s 跳过: %s", i + 1, total, sym_full, reason)
                continue
            logger.info("  [%s/%s] %s 采集: %s", i + 1, total, sym_full, reason)
            try:
                if inc_start and inc_end:
                    n = _ingest_ohlcv_incremental(sym_raw, inc_start, inc_end)
                else:
                    n = run_ingest_ohlcv(symbols=[sym_raw], days_back=days_back)
                cnt_done += 1
                logger.info("  [%s/%s] %s 写入 %s 行", i + 1, total, sym_full, n or 0)
            except Exception as e:
                logger.warning("  [%s/%s] %s 失败: %s", i + 1, total, sym_full, e)
            if delay_sec > 0 and i < total - 1:
                time.sleep(delay_sec)
            if batch_pause > 0 and (i + 1) % max(1, batch_size) == 0 and i < total - 1:
                time.sleep(batch_pause)
        logger.info("Phase 1 完成: 采集 %s 只，跳过 %s 只", cnt_done, cnt_skip)
        if phase_pause > 0:
            logger.info("阶段间暂停 %.0fs", phase_pause)
            time.sleep(phase_pause)

    # Phase 2: 行业
    if _ingest_enabled("INDUSTRY_REVENUE"):
        logger.info("=" * 50)
        logger.info("Phase 2: 行业/财务摘要 → L2（%s 只）", total)
        logger.info("=" * 50)
        cnt_done, cnt_skip = 0, 0
        for j, sym_full in enumerate(symbols_full):
            sym_key = sym_full.strip().upper()
            should, reason = _decide_industry(sym_key, industry_updated, mode, now)
            if not should:
                cnt_skip += 1
                if j < 3 or (j + 1) == total:
                    logger.info("  [%s/%s] %s 跳过: %s", j + 1, total, sym_full, reason)
                continue
            logger.info("  [%s/%s] %s 采集: %s", j + 1, total, sym_full, reason)
            try:
                run_ingest_industry_revenue(sym_full)
                cnt_done += 1
            except Exception as e:
                logger.warning("  [%s/%s] %s 失败: %s", j + 1, total, sym_full, e)
            if delay_sec > 0 and j < total - 1:
                time.sleep(delay_sec)
        logger.info("Phase 2 完成: 采集 %s 只，跳过 %s 只", cnt_done, cnt_skip)
        if phase_pause > 0:
            logger.info("阶段间暂停 %.0fs", phase_pause)
            time.sleep(phase_pause)

    # Phase 2.5: 财务报表
    if _ingest_enabled("FINANCIAL"):
        logger.info("=" * 50)
        logger.info("Phase 2.5: 财务报表 → L2（%s 只）", total)
        logger.info("=" * 50)
        cnt_done, cnt_skip = 0, 0
        for j, sym_full in enumerate(symbols_full):
            sym_key = sym_full.strip().upper()
            should, reason = _decide_financial(sym_key, financial_updated, mode, now)
            if not should:
                cnt_skip += 1
                if j < 3 or (j + 1) == total:
                    logger.info("  [%s/%s] %s 跳过: %s", j + 1, total, sym_full, reason)
                continue
            logger.info("  [%s/%s] %s 采集: %s", j + 1, total, sym_full, reason)
            try:
                n = run_ingest_financial(sym_full)
                cnt_done += 1
                logger.info("  [%s/%s] %s 写入 %s 期", j + 1, total, sym_full, n)
            except Exception as e:
                logger.warning("  [%s/%s] %s 失败: %s", j + 1, total, sym_full, e)
            if delay_sec > 0 and j < total - 1:
                time.sleep(delay_sec)
        logger.info("Phase 2.5 完成: 采集 %s 只，跳过 %s 只", cnt_done, cnt_skip)
        if phase_pause > 0:
            logger.info("阶段间暂停 %.0fs", phase_pause)
            time.sleep(phase_pause)

    # Phase 3: 新闻
    if _ingest_enabled("NEWS"):
        logger.info("=" * 50)
        logger.info("Phase 3: 新闻/公告 → L2（%s 只）", total)
        logger.info("=" * 50)
        cnt_done, cnt_skip = 0, 0
        for k, sym_full in enumerate(symbols_full):
            sym_key = sym_full.strip().upper()
            should, reason = _decide_news(sym_key, news_latest, mode)
            if not should:
                cnt_skip += 1
                if k < 3 or (k + 1) == total:
                    logger.info("  [%s/%s] %s 跳过: %s", k + 1, total, sym_full, reason)
                continue
            logger.info("  [%s/%s] %s 采集: %s", k + 1, total, sym_full, reason)
            try:
                if unified_start and unified_end:
                    run_ingest_news(symbol=sym_full, date_start=unified_start, date_end=unified_end)
                else:
                    run_ingest_news(symbol=sym_full, days_back=news_days if news_days > 0 else None)
                cnt_done += 1
            except Exception as e:
                logger.warning("  [%s/%s] %s 失败: %s", k + 1, total, sym_full, e)
            if delay_sec > 0 and k < total - 1:
                time.sleep(delay_sec)
        logger.info("Phase 3 完成: 采集 %s 只，跳过 %s 只", cnt_done, cnt_skip)


def _run_phases_combined(
    symbols_raw, symbols_full, total, mode, today, now,
    ohlcv_latest, industry_updated, financial_updated, news_latest,
    days_back, unified_start, unified_end, news_days,
    delay_sec, batch_size, batch_pause,
):
    completed = _read_progress()
    if completed >= total:
        _clear_progress()
        completed = 0

    batches_raw = [symbols_raw[i:i + batch_size] for i in range(0, total, batch_size)]
    batches_full = [symbols_full[i:i + batch_size] for i in range(0, total, batch_size)]
    n_batches = len(batches_raw)
    start_batch = completed // batch_size
    if completed > 0:
        logger.info("断点续跑: 已完成 %s/%s，从第 %s 批起", completed, total, start_batch + 1)

    completed = start_batch * batch_size
    for i in range(start_batch, n_batches):
        batch_raw = batches_raw[i]
        batch_full = batches_full[i]
        start_idx = i * batch_size
        logger.info(
            "======== 第 %s/%s 批（标的 %s～%s）========",
            i + 1, n_batches, start_idx + 1, start_idx + len(batch_raw),
        )
        for j, (sym_raw, sym_full) in enumerate(zip(batch_raw, batch_full)):
            sym_key = sym_full.strip().upper()
            did_any = False

            if _ingest_enabled("OHLCV"):
                should, inc_start, inc_end, reason = _decide_ohlcv(sym_key, ohlcv_latest, mode, today)
                if should:
                    try:
                        if inc_start and inc_end:
                            _ingest_ohlcv_incremental(sym_raw, inc_start, inc_end)
                        else:
                            run_ingest_ohlcv(symbols=[sym_raw], days_back=days_back)
                        did_any = True
                    except Exception as e:
                        logger.warning("K线 %s 失败: %s", sym_full, e)

            if _ingest_enabled("INDUSTRY_REVENUE"):
                should, reason = _decide_industry(sym_key, industry_updated, mode, now)
                if should:
                    try:
                        run_ingest_industry_revenue(sym_full)
                        did_any = True
                    except Exception as e:
                        logger.warning("行业 %s 失败: %s", sym_full, e)

            if _ingest_enabled("FINANCIAL"):
                should, reason = _decide_financial(sym_key, financial_updated, mode, now)
                if should:
                    try:
                        run_ingest_financial(sym_full)
                        did_any = True
                    except Exception as e:
                        logger.warning("财务 %s 失败: %s", sym_full, e)

            if _ingest_enabled("NEWS"):
                should, reason = _decide_news(sym_key, news_latest, mode)
                if should:
                    try:
                        if unified_start and unified_end:
                            run_ingest_news(symbol=sym_full, date_start=unified_start, date_end=unified_end)
                        else:
                            run_ingest_news(symbol=sym_full, days_back=news_days if news_days > 0 else None)
                        did_any = True
                    except Exception as e:
                        logger.warning("新闻 %s 失败: %s", sym_full, e)

            if not did_any:
                logger.info("标的 %s 各维度均跳过", sym_full)

            if delay_sec > 0 and j < len(batch_full) - 1:
                time.sleep(delay_sec)

        completed += len(batch_raw)
        _write_progress(completed)
        if batch_pause > 0 and i < n_batches - 1:
            logger.info("批间暂停 %.0fs", batch_pause)
            time.sleep(batch_pause)


if __name__ == "__main__":
    sys.exit(main())
