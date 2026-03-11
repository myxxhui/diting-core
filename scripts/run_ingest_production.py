#!/usr/bin/env python3
# [Ref: 04_阶段规划与实践/Stage2_数据采集与存储/06_生产级数据要求_实践.md]
# 全量生产级数据采集：先刷新全 A 股 universe，再按 universe 标的拉取单标≥5 年日线写入 L1；
# 与 06_ 设计、11_ 规约一致。步骤 8 必须执行本脚本（或 make ingest-production），不得以 ingest-test 代替。
# 工作目录: diting-core；需 .env 中 TIMESCALE_DSN 等。禁止 DITING_INGEST_MOCK=1 下执行（mock 不满足 5 年深度）。
#
# 最佳分批模式（绕过 AkShare 限流/断连）：按批拉取、批间暂停、断点续跑；数据源仍为 AkShare。
# 生产标准：每个标的均采集 量化（OHLCV）、行业/财务、新闻。

import logging
import os
import sys
import time
from pathlib import Path

root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

# 加载 .env；仅当变量未设置时才写入，未设置的变量才用脚本内默认参数（不在此处写死默认值）
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
)
from diting.ingestion.config import get_pg_l2_dsn, get_timescale_dsn
from diting.universe import get_current_a_share_universe, parse_symbol_list_from_env

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


def _count_ohlcv(symbol_ts: str, start_str: str, end_str: str) -> int:
    """L1 该标的在 [start_str,end_str] 区间内的 K 线条数（日线）。异常时返回 -1。"""
    if not start_str or not end_str:
        return 0
    try:
        import psycopg2
        conn = psycopg2.connect(get_timescale_dsn())
        try:
            cur = conn.cursor()
            # 与 ohlcv 写入一致：period 为 daily（DEFAULT_PERIOD），兼容历史可能存在的 day
            cur.execute(
                "SELECT COUNT(*) FROM ohlcv WHERE symbol = %s AND (period = %s OR period = %s) AND datetime::date >= %s::date AND datetime::date <= %s::date",
                (symbol_ts.strip().upper(), "day", "daily", start_str, end_str),
            )
            return cur.fetchone()[0] or 0
        finally:
            conn.close()
    except Exception as e:
        logger.info("无法检查 L1 K 线条数 %s（%s），将执行采集", symbol_ts, e)
        return -1


def _count_ohlcv_batch(symbols_full: list, start_str: str, end_str: str):
    """L1 批量查询：返回 symbol -> 区间内 K 线条数（成功时 dict，失败时 None 以便回退逐只查）。"""
    if not start_str or not end_str or not symbols_full:
        return None
    syms = [s.strip().upper() for s in symbols_full if (s or "").strip()]
    if not syms:
        return None
    try:
        import psycopg2
        conn = psycopg2.connect(get_timescale_dsn())
        try:
            cur = conn.cursor()
            cur.execute(
                """SELECT symbol, COUNT(*) FROM ohlcv
                   WHERE symbol = ANY(%s) AND (period = %s OR period = %s) AND datetime::date >= %s::date AND datetime::date <= %s::date
                   GROUP BY symbol""",
                (syms, "day", "daily", start_str, end_str),
            )
            return {row[0]: row[1] for row in cur.fetchall()}
        finally:
            conn.close()
    except Exception as e:
        logger.info("批量检查 L1 K 线失败（%s），将逐只检查或采集", e)
        return None


def _has_ohlcv(symbol_ts: str, start_str: str, end_str: str) -> bool:
    """L1 是否已有该标的在 [start_str,end_str] 区间内的 K 线（日线）。"""
    n = _count_ohlcv(symbol_ts, start_str, end_str)
    return n > 0


def _has_industry(symbol_ts: str) -> bool:
    """L2 是否已有该标的的行业/财务汇总且行业名非空（空则视为需补采或 fallback 回填）。"""
    try:
        import psycopg2
        conn = psycopg2.connect(get_pg_l2_dsn())
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM industry_revenue_summary WHERE symbol = %s AND industry_name IS NOT NULL AND TRIM(industry_name) <> '' LIMIT 1",
                (symbol_ts.strip().upper(),),
            )
            return cur.fetchone() is not None
        finally:
            conn.close()
    except Exception as e:
        logger.info("无法检查 L2 是否已有行业 %s（%s），将执行采集", symbol_ts, e)
        return False


def _symbols_with_industry_batch(symbols_full: list) -> set:
    """L2 批量：返回已有非空行业名的 symbol 集合。用于 Phase 2 一次查完。"""
    if not symbols_full:
        return set()
    syms = [s.strip().upper() for s in symbols_full if (s or "").strip()]
    if not syms:
        return set()
    try:
        import psycopg2
        conn = psycopg2.connect(get_pg_l2_dsn())
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT symbol FROM industry_revenue_summary WHERE symbol = ANY(%s) AND industry_name IS NOT NULL AND TRIM(industry_name) <> ''",
                (syms,),
            )
            return {row[0] for row in cur.fetchall()}
        finally:
            conn.close()
    except Exception as e:
        logger.info("批量检查 L2 行业失败（%s），将逐只检查或采集", e)
        return set()


def _symbols_with_news_batch(symbols_full: list) -> set:
    """L2 批量：返回已有新闻版本记录的 symbol 集合（version_id 形如 news_<symbol>_*）。一次查询后解析。"""
    if not symbols_full:
        return set()
    syms = set(s.strip().upper() for s in symbols_full if (s or "").strip())
    if not syms:
        return set()
    try:
        import psycopg2
        conn = psycopg2.connect(get_pg_l2_dsn())
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT version_id FROM data_versions WHERE data_type = %s AND version_id LIKE %s",
                ("news", "news_%"),
            )
            found = set()
            for (vid,) in cur.fetchall():
                if vid and vid.startswith("news_") and "_" in vid[5:]:
                    sym = vid[5:].split("_")[0]
                    if sym in syms:
                        found.add(sym)
            return found
        finally:
            conn.close()
    except Exception as e:
        logger.info("批量检查 L2 新闻失败（%s），将逐只检查或采集", e)
        return set()


def _has_news(symbol_ts: str) -> bool:
    """L2 是否已有该标的的新闻/公告版本记录。"""
    try:
        import psycopg2
        conn = psycopg2.connect(get_pg_l2_dsn())
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM data_versions WHERE data_type = %s AND version_id LIKE %s LIMIT 1",
                ("news", "news_{}_%".format(symbol_ts.strip())),
            )
            return cur.fetchone() is not None
        finally:
            conn.close()
    except Exception as e:
        logger.info("无法检查 L2 是否已有新闻 %s（%s），将执行采集", symbol_ts, e)
        return False

# 生产要求默认：K 线回溯天数（约 5 年），可由 INGEST_PRODUCTION_OHLCV_DAYS_BACK 覆盖
DAYS_BACK_DEFAULT = 5 * 365

# 分批进度文件（diting-core 根目录，不提交）；断点续跑时读写
PROGRESS_FILE = root / ".ingest_production_progress"


def _ingest_enabled(scope: str) -> bool:
    """生产采集开关：未设置或 true/1/yes = 开启（默认满足 AB 模块全量），false/0/no = 关闭。"""
    key = f"INGEST_PRODUCTION_{scope}"
    raw = (os.environ.get(key) or "").strip().lower()
    if raw in ("false", "0", "no"):
        return False
    return True  # 未设置或 true/1/yes 均视为开启，确保仅复制 prod.conn 时也跑满 universe/ohlcv/industry_revenue/news


def _unified_date_range():
    """
    统一拉取时间范围：当 INGEST_JQDATA_DATE_START 与 INGEST_JQDATA_DATE_END 均已配置时，
    返回 (start_str, end_str) 格式 YYYY-MM-DD；否则返回 (None, None)。
    与 INGEST_SOURCE 无关：无论 akshare 还是 jqdata，本脚本都用这两个变量决定 K 线/新闻的日期区间（变量名沿袭历史，实际是「统一日期范围」）。
    """
    start_raw = (os.environ.get("INGEST_JQDATA_DATE_START") or "").strip().replace(" ", "")
    end_raw = (os.environ.get("INGEST_JQDATA_DATE_END") or "").strip().replace(" ", "")
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
    """K 线拉取时间线：优先用 INGEST_JQDATA_DATE_* 统一范围的天数，否则 INGEST_PRODUCTION_OHLCV_DAYS_BACK，默认 5 年。"""
    if not _ingest_enabled("OHLCV"):
        return DAYS_BACK_DEFAULT
    start_str, end_str = _unified_date_range()
    if start_str and end_str:
        try:
            from datetime import datetime
            start_d = datetime.strptime(start_str, "%Y-%m-%d")
            end_d = datetime.strptime(end_str, "%Y-%m-%d")
            return max(1, (end_d - start_d).days)
        except (ValueError, TypeError):
            pass
    raw = (os.environ.get("INGEST_PRODUCTION_OHLCV_DAYS_BACK") or "").strip()
    try:
        n = int(raw)
        if n <= 0:
            return DAYS_BACK_DEFAULT
        return max(1, min(365 * 20, n))
    except ValueError:
        return DAYS_BACK_DEFAULT


def _news_days_back() -> int:
    """新闻拉取时间线：未使用统一日期范围时，由 INGEST_PRODUCTION_NEWS_DAYS_BACK 指定保留最近 N 天，默认 365。"""
    if not _ingest_enabled("NEWS"):
        return 0
    if _unified_date_range()[0]:
        return 0  # 使用统一范围时由 date_start/date_end 过滤，此处不生效
    raw = (os.environ.get("INGEST_PRODUCTION_NEWS_DAYS_BACK") or "").strip()
    try:
        n = int(raw)
        if n <= 0:
            return 365
        return max(1, min(365 * 5, n))
    except ValueError:
        return 365


def _batch_size() -> int:
    """每批标的数量，默认 15；可设为 1 以最小化断连（云服务器 IP 易被限流时）。"""
    raw = os.environ.get("INGEST_OHLCV_BATCH_SIZE", "15").strip()
    try:
        return max(1, min(500, int(raw)))
    except ValueError:
        return 15


def _batch_pause_sec() -> float:
    """批间暂停秒数，给 AkShare/东方财富接口“冷却”，默认 60 秒。"""
    raw = os.environ.get("INGEST_OHLCV_BATCH_PAUSE_SEC", "60").strip()
    try:
        return max(0.0, min(300.0, float(raw)))
    except ValueError:
        return 60.0


def _extra_delay_sec() -> float:
    """行业/新闻按标的前的标间延迟（秒），减轻限流，默认 2 秒。"""
    raw = os.environ.get("INGEST_EXTRA_DELAY_SEC", "2").strip()
    try:
        return max(0.0, min(30.0, float(raw)))
    except ValueError:
        return 2.0


def _separate_phases() -> bool:
    """INGEST_SEPARATE_PHASES=1 时：先全部 K 线 → 停顿 → 全部行业 → 停顿 → 全部新闻。便于阶段间长停顿、观察限流是否缓解。"""
    return os.environ.get("INGEST_SEPARATE_PHASES", "").strip().lower() in ("1", "true", "yes")


def _phase_pause_sec() -> float:
    """阶段间暂停秒数（仅 INGEST_SEPARATE_PHASES=1 时生效）。默认 30；东方财富行业接口在境外/限流下常首次请求即断连，长暂停无法避免，可保持较短以节省时间（行业会走 fallback）。"""
    raw = os.environ.get("INGEST_PHASE_PAUSE_SEC", "30").strip()
    try:
        return max(0.0, min(600.0, float(raw)))
    except ValueError:
        return 30.0


def _resume_enabled() -> bool:
    """是否启用断点续跑（读取/写入 .ingest_production_progress）。"""
    raw = os.environ.get("INGEST_PRODUCTION_RESUME", "1").strip().lower()
    return raw in ("1", "true", "yes")


def _read_progress() -> int:
    """返回已完成的标的数量（下次从 symbols[count:] 开始）。"""
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
        logger.warning("写入进度文件失败（不影响采集）: %s", e)


def _clear_progress() -> None:
    if PROGRESS_FILE.exists():
        try:
            PROGRESS_FILE.unlink()
            logger.info("已清除断点进度，下次全量将从头开始")
        except OSError:
            pass


def main() -> int:
    if os.environ.get("DITING_INGEST_MOCK", "").strip().lower() in ("1", "true", "yes"):
        logger.error("全量生产级采集禁止使用 DITING_INGEST_MOCK=1，请去掉该环境变量后执行")
        return 1
    try:
        import akshare  # noqa: F401
    except ImportError as e:
        logger.exception("全量采集 import akshare 失败（镜像内须已 pip install akshare + requirements-ingest-core.txt）: %s", e)
        logger.error("全量采集依赖 akshare，请先执行：make deps-ingest 或 pip install -r requirements-ingest-core.txt")
        return 1

    need_symbols = _ingest_enabled("OHLCV") or _ingest_enabled("INDUSTRY_REVENUE") or _ingest_enabled("NEWS")
    try:
        # 指定股票：环境变量 DITING_SYMBOLS（一套名单，采集与 AB 模块共用）；未设置时再读 INGEST_PRODUCTION_SYMBOLS
        specified = parse_symbol_list_from_env("DITING_SYMBOLS") or parse_symbol_list_from_env("INGEST_PRODUCTION_SYMBOLS")
        if specified:
            logger.info("指定股票采集: 共 %s 只（DITING_SYMBOLS / INGEST_PRODUCTION_SYMBOLS）", len(specified))
        else:
            logger.info("全量采集模式: step1 标的池仅一次；全量只分一次批，每批内依次执行 step2 K线 → step3 行业/财务 → step4 个股新闻")

        # ① 标的池：未指定股票且开启 UNIVERSE 时刷新全 A 股
        if not specified and _ingest_enabled("UNIVERSE"):
            logger.info("全量采集 step1: 刷新 universe（全A股）")
            run_ingest_universe()
        elif not specified:
            logger.info("全量采集 step1: 跳过 universe（INGEST_PRODUCTION_UNIVERSE 未开启）")
        # 指定股票时默认不刷新 universe

        if not need_symbols:
            logger.info("全量采集完成（OHLCV/行业/新闻均未开启）")
            return 0

        if specified:
            symbols_ts = specified
        else:
            symbols_ts = get_current_a_share_universe(force_refresh=False)
        if not symbols_ts:
            logger.error("无标的：指定模式下 INGEST_PRODUCTION_SYMBOLS 为空；全量模式下 universe 表无数据，请先执行 step1 或配置 INGEST_PRODUCTION_SYMBOLS")
            return 1
        symbols_full = symbols_ts  # 带后缀，供 step3/step4
        symbols_raw = [s.split(".")[0] for s in symbols_ts]
        total = len(symbols_raw)

        days_back = _ohlcv_days_back() if _ingest_enabled("OHLCV") else 0
        unified_start, unified_end = _unified_date_range()
        news_days = _news_days_back()
        batch_size = _batch_size()
        batch_pause = _batch_pause_sec()
        delay_sec = _extra_delay_sec()

        completed = _read_progress()
        if completed >= total:
            logger.info("进度文件显示已全部完成（%s），清除进度后重新全量", completed)
            _clear_progress()
            completed = 0
        # 只对全量分一次批，断点续跑从第 (completed // batch_size) 批开始，不对「剩余」再分批
        batches_raw = [symbols_raw[i : i + batch_size] for i in range(0, total, batch_size)]
        batches_full = [symbols_full[i : i + batch_size] for i in range(0, total, batch_size)]
        n_batches = len(batches_raw)
        start_batch = completed // batch_size
        if completed > 0:
            logger.info("断点续跑: 已完成 %s/%s 标（前 %s 批），本次从第 %s 批起", completed, total, start_batch, start_batch + 1)
        logger.info(
            "每批执行三阶段(step2→step3→step4) | 共 %s 只、%s 批，每批 %s 只，批间暂停 %.0fs",
            total, n_batches, batch_size, batch_pause,
        )
        if _ingest_enabled("OHLCV") and (unified_start and unified_end):
            logger.info("K 线日期范围 %s ～ %s（共 %s 天）", unified_start, unified_end, days_back)
        elif _ingest_enabled("OHLCV"):
            logger.info("K 线回溯 %s 天", days_back)

        # 新闻：全市场+宏观仅执行一次（在首批前，断点续跑从第 2 批起则不再执行）
        if _ingest_enabled("NEWS") and start_batch == 0:
            if unified_start and unified_end:
                logger.info("全量采集 step4（全市场）: 新闻 日期范围 %s ～ %s", unified_start, unified_end)
                run_ingest_news(date_start=unified_start, date_end=unified_end)
            else:
                logger.info("全量采集 step4（全市场）: 新闻 保留最近 %s 天", news_days if news_days > 0 else "全部")
                run_ingest_news(days_back=news_days if news_days > 0 else None)

        separate = _separate_phases()
        if separate:
            phase_pause = _phase_pause_sec()
            logger.info(
                "按阶段分开执行（INGEST_SEPARATE_PHASES=1）：已有数据则跳过；Phase1 结束后暂停 %.0fs 再 Phase2，Phase2 结束后再暂停 %.0fs 再 Phase3（届时会打「阶段间暂停」日志）",
                phase_pause, phase_pause,
            )
            if unified_start and unified_end:
                ohlcv_start, ohlcv_end = unified_start, unified_end
            else:
                from datetime import datetime, timedelta
                _end = datetime.now().date()
                _start = _end - timedelta(days=days_back)
                ohlcv_start, ohlcv_end = _start.isoformat(), _end.isoformat()
            # Phase 1：全部 K 线（批量查一次 L1，避免 27 次逐只查导致前面跳过过久）
            if _ingest_enabled("OHLCV"):
                logger.info("======== Phase 1/3：K 线（写入 L1）共 %s 只，区间 %s～%s ========", total, ohlcv_start, ohlcv_end)
                ohlcv_counts = _count_ohlcv_batch(symbols_full, ohlcv_start, ohlcv_end)
                for i, sym_raw in enumerate(symbols_raw):
                    sym_display = symbols_full[i] if i < len(symbols_full) else sym_raw
                    count = ohlcv_counts.get(sym_display.strip().upper(), 0) if ohlcv_counts is not None else _count_ohlcv(sym_display, ohlcv_start, ohlcv_end)
                    if count > 0:
                        if i == 0:
                            logger.info("Phase 1 第 1/%s 只 %s L1 已有 %s 条 K 线，跳过", total, sym_display, count)
                        else:
                            logger.info("Phase 1 第 %s/%s 只 %s K 线 已有数据，跳过", i + 1, total, sym_display)
                    else:
                        if i == 0 and count == 0:
                            logger.info("Phase 1 第 1/%s 只 %s L1 区间内 0 条 K 线（请确认 TIMESCALE_DSN 与写入库一致），将采集", total, sym_display)
                        else:
                            logger.info("Phase 1 第 %s/%s 只 %s 采集 K 线 → 写入 L1 …", i + 1, total, sym_display)
                        try:
                            n = run_ingest_ohlcv(symbols=[sym_raw], days_back=days_back)
                            logger.info("Phase 1 第 %s/%s 只 %s K 线 已完成（L1 写入 %s 行）", i + 1, total, sym_display, n or 0)
                        except Exception as e:
                            logger.warning("Phase 1 第 %s/%s 只 %s K 线 失败: %s", i + 1, total, sym_display, e)
                    if delay_sec > 0 and i < total - 1:
                        time.sleep(delay_sec)
                    if batch_pause > 0 and (i + 1) % max(1, batch_size) == 0 and i < total - 1:
                        time.sleep(batch_pause)
                if phase_pause > 0:
                    logger.info("阶段间暂停 %.0f 秒（K线 → 行业）", phase_pause)
                    time.sleep(phase_pause)
            # Phase 2：全部行业/财务（批量查一次 L2 已有非空行业，避免逐只查）
            if _ingest_enabled("INDUSTRY_REVENUE"):
                logger.info("======== Phase 2/3：行业/财务（写入 L2）共 %s 只 ========", total)
                industry_has = _symbols_with_industry_batch(symbols_full)
                for j, sym_full in enumerate(symbols_full):
                    if sym_full.strip().upper() in industry_has:
                        logger.info("Phase 2 第 %s/%s 只 %s 行业/财务 已有数据，跳过", j + 1, total, sym_full)
                    else:
                        logger.info("Phase 2 第 %s/%s 只 %s 采集 行业/财务 → 写入 L2 …", j + 1, total, sym_full)
                        try:
                            r = run_ingest_industry_revenue(sym_full)
                            logger.info("Phase 2 第 %s/%s 只 %s 行业/财务 已完成（%s）", j + 1, total, sym_full, "已写入 L2" if r else "无数据未写入")
                        except Exception as e:
                            logger.warning("Phase 2 第 %s/%s 只 %s 行业/财务 失败: %s", j + 1, total, sym_full, e)
                    if delay_sec > 0 and j < total - 1:
                        time.sleep(delay_sec)
                if phase_pause > 0:
                    logger.info("阶段间暂停 %.0f 秒（行业 → 新闻）", phase_pause)
                    time.sleep(phase_pause)
            # Phase 3：全部个股新闻（批量查一次 L2 已有新闻版本，避免逐只查）
            if _ingest_enabled("NEWS"):
                logger.info("======== Phase 3/3：个股新闻/公告（写入 L2）共 %s 只 ========", total)
                news_has = _symbols_with_news_batch(symbols_full)
                for k, sym_full in enumerate(symbols_full):
                    if sym_full.strip().upper() in news_has:
                        logger.info("Phase 3 第 %s/%s 只 %s 新闻/公告 已有数据，跳过", k + 1, total, sym_full)
                    else:
                        logger.info("Phase 3 第 %s/%s 只 %s 采集 新闻/公告 → 写入 L2 …", k + 1, total, sym_full)
                        try:
                            if unified_start and unified_end:
                                run_ingest_news(symbol=sym_full, date_start=unified_start, date_end=unified_end)
                            else:
                                run_ingest_news(symbol=sym_full, days_back=news_days if news_days > 0 else None)
                            logger.info("Phase 3 第 %s/%s 只 %s 新闻/公告 已完成（已写入 L2）", k + 1, total, sym_full)
                        except Exception as e:
                            logger.warning("Phase 3 第 %s/%s 只 %s 新闻/公告 失败: %s", k + 1, total, sym_full, e)
                    if delay_sec > 0 and k < total - 1:
                        time.sleep(delay_sec)
            _clear_progress()
            logger.info("全量采集完成（按阶段分开执行）")
            return 0
        # 原有：每批内 step2→step3→step4；已有数据则跳过该步，三项均有则跳过该标的
        if unified_start and unified_end:
            ohlcv_start, ohlcv_end = unified_start, unified_end
        else:
            from datetime import datetime, timedelta
            _end = datetime.now().date()
            _start = _end - timedelta(days=days_back)
            ohlcv_start, ohlcv_end = _start.isoformat(), _end.isoformat()
        completed = start_batch * batch_size
        for i in range(start_batch, n_batches):
            batch_raw = batches_raw[i]
            batch_full = batches_full[i]
            start_idx = i * batch_size
            logger.info(
                "======== 第 %s/%s 批（标的 %s～%s，共 %s 只）| step2 K线 → step3 行业/财务 → step4 个股新闻 ========",
                i + 1, n_batches, start_idx + 1, start_idx + len(batch_raw), len(batch_raw),
            )
            for j, (sym_raw, sym_full) in enumerate(zip(batch_raw, batch_full)):
                if _ingest_enabled("OHLCV") and _ingest_enabled("INDUSTRY_REVENUE") and _ingest_enabled("NEWS"):
                    if _has_ohlcv(sym_full, ohlcv_start, ohlcv_end) and _has_industry(sym_full) and _has_news(sym_full):
                        logger.info("第 %s/%s 批 标的 %s 三项均有数据，跳过", i + 1, n_batches, sym_full)
                        if delay_sec > 0 and j < len(batch_full) - 1:
                            time.sleep(delay_sec)
                        continue
                if _ingest_enabled("OHLCV"):
                    if _has_ohlcv(sym_full, ohlcv_start, ohlcv_end):
                        logger.info("标的 %s K 线已有数据，跳过", sym_full)
                    else:
                        run_ingest_ohlcv(symbols=[sym_raw], days_back=days_back)
                if _ingest_enabled("INDUSTRY_REVENUE"):
                    if _has_industry(sym_full):
                        logger.info("标的 %s 行业/财务已有数据，跳过", sym_full)
                    else:
                        try:
                            run_ingest_industry_revenue(sym_full)
                        except Exception as e:
                            logger.warning("industry_revenue symbol=%s failed: %s", sym_full, e)
                if _ingest_enabled("NEWS"):
                    if _has_news(sym_full):
                        logger.info("标的 %s 新闻/公告已有数据，跳过", sym_full)
                    else:
                        try:
                            if unified_start and unified_end:
                                run_ingest_news(symbol=sym_full, date_start=unified_start, date_end=unified_end)
                            else:
                                run_ingest_news(symbol=sym_full, days_back=news_days if news_days > 0 else None)
                        except Exception as e:
                            logger.warning("news symbol=%s failed: %s", sym_full, e)
                if delay_sec > 0 and j < len(batch_full) - 1:
                    time.sleep(delay_sec)
            completed += len(batch_raw)
            _write_progress(completed)
            if batch_pause > 0 and i < n_batches - 1:
                logger.info("批间暂停 %.0f 秒", batch_pause)
                time.sleep(batch_pause)
        _clear_progress()
        logger.info("全量采集完成（每批三阶段 step2→step3→step4 已跑完，符合 AB 模块生产要求）")
        return 0
    except Exception as e:
        logger.exception("ingest-production failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
