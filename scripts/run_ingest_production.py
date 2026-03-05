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
from diting.universe import get_current_a_share_universe

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

# 生产要求默认：K 线回溯天数（约 5 年），可由 INGEST_PRODUCTION_OHLCV_DAYS_BACK 覆盖
DAYS_BACK_DEFAULT = 5 * 365

# 分批进度文件（diting-core 根目录，不提交）；断点续跑时读写
PROGRESS_FILE = root / ".ingest_production_progress"


def _ingest_enabled(scope: str) -> bool:
    """生产采集开关：0 或不写 = 关闭；其它数字 = 开启（且该数字即数据长度，见 _ingest_days）。"""
    key = f"INGEST_PRODUCTION_{scope}"
    raw = (os.environ.get(key) or "").strip()
    return raw != "" and raw != "0"


def _ingest_days(scope: str, default: int = None, max_days: int = 365 * 20) -> int:
    """
    读取该类型的「数据长度」（天数）。仅当 _ingest_enabled(scope) 为 True 时有意义。
    0 或不写 = 关闭；正数 = 开启且表示回溯/拉取天数；非数字或未写时用 default。
    """
    key = f"INGEST_PRODUCTION_{scope}"
    raw = (os.environ.get(key) or "").strip()
    try:
        n = int(raw)
        if n <= 0:
            return default or DAYS_BACK_DEFAULT
        return max(1, min(max_days, n))
    except ValueError:
        return default or DAYS_BACK_DEFAULT


def _ohlcv_days_back() -> int:
    """K 线拉取时间线：INGEST_PRODUCTION_OHLCV 为正值时即回溯天数，否则默认 5 年。"""
    if not _ingest_enabled("OHLCV"):
        return DAYS_BACK_DEFAULT
    return _ingest_days("OHLCV", default=DAYS_BACK_DEFAULT, max_days=365 * 20)


def _news_days_back() -> int:
    """新闻拉取时间线：INGEST_PRODUCTION_NEWS 为正值时即只保留最近 N 天。"""
    if not _ingest_enabled("NEWS"):
        return 0
    return _ingest_days("NEWS", default=365, max_days=365 * 5)  # 未写数字时默认 365 天


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
        # ① 标的池：按开关决定是否刷新全 A 股 universe
        if _ingest_enabled("UNIVERSE"):
            logger.info("全量采集 step1: 刷新 universe（全A股）")
            run_ingest_universe()
        else:
            logger.info("全量采集 step1: 跳过 universe（INGEST_PRODUCTION_UNIVERSE=0）")

        # 若需 K 线/行业/新闻，从表内读取标的列表
        symbols_ts = []
        symbols_raw = []
        if need_symbols:
            symbols_ts = get_current_a_share_universe(force_refresh=False)
            if not symbols_ts:
                logger.error("universe 表无标的，请先开启 INGEST_PRODUCTION_UNIVERSE=1 或先执行一次全量标的拉取")
                return 1
            symbols_raw = [s.split(".")[0] for s in symbols_ts]
            total = len(symbols_raw)

        # ② K 线/量化：按开关与时间线拉取
        if _ingest_enabled("OHLCV"):
            days_back = _ohlcv_days_back()
            logger.info("全量采集 step2: K 线（OHLCV）| 回溯 %s 天，符合 AB 模块生产要求", days_back)
            completed = _read_progress()
            if completed > 0 and completed < total:
                symbols_raw_ohlcv = symbols_raw[completed:]
                logger.info("断点续跑: 已完成 %s/%s 标，本次从第 %s 标起，剩余 %s 只", completed, total, completed + 1, len(symbols_raw_ohlcv))
            elif completed >= total:
                logger.info("进度文件显示已全部完成（%s），清除进度后重新全量", completed)
                _clear_progress()
                completed = 0
                symbols_raw_ohlcv = symbols_raw
            else:
                symbols_raw_ohlcv = symbols_raw

            batch_size = _batch_size()
            batch_pause = _batch_pause_sec()
            batches = [symbols_raw_ohlcv[i : i + batch_size] for i in range(0, len(symbols_raw_ohlcv), batch_size)]
            n_batches = len(batches)
            logger.info(
                "K 线分批 | 共 %s 只标的，每批 %s 只，批间暂停 %s 秒，共 %s 批",
                len(symbols_raw_ohlcv), batch_size, batch_pause, n_batches,
            )
            for i, batch in enumerate(batches):
                start_idx = completed + i * batch_size
                logger.info("全量 OHLCV 第 %s/%s 批（标的 %s～%s，共 %s 只）", i + 1, n_batches, start_idx + 1, start_idx + len(batch), len(batch))
                run_ingest_ohlcv(symbols=batch, days_back=days_back)
                completed += len(batch)
                _write_progress(completed)
                if batch_pause > 0 and i < n_batches - 1:
                    logger.info("批间暂停 %.0f 秒", batch_pause)
                    time.sleep(batch_pause)
            _clear_progress()
        else:
            logger.info("全量采集 step2: 跳过 K 线（INGEST_PRODUCTION_OHLCV=0）")

        # ③ 行业/财务：按开关按标拉取
        symbols_full = [s.split(".")[0] for s in get_current_a_share_universe(force_refresh=False)] if need_symbols else []
        n_total = len(symbols_full)

        if _ingest_enabled("INDUSTRY_REVENUE") and n_total:
            batch_size_ir = _batch_size()
            batch_pause_ir = _batch_pause_sec()
            delay_sec = _extra_delay_sec()
            batches_ir = [symbols_full[i : i + batch_size_ir] for i in range(0, n_total, batch_size_ir)]
            logger.info(
                "全量采集 step3: 行业/财务 | 共 %s 只标的，每批 %s 只，标间延迟 %.1fs，批间暂停 %.0fs",
                n_total, batch_size_ir, delay_sec, batch_pause_ir,
            )
            for i, batch in enumerate(batches_ir):
                logger.info("行业/财务 第 %s/%s 批（共 %s 只）", i + 1, len(batches_ir), len(batch))
                for j, sym in enumerate(batch):
                    try:
                        run_ingest_industry_revenue(sym)
                    except Exception as e:
                        logger.warning("industry_revenue symbol=%s failed: %s", sym, e)
                    if delay_sec > 0 and j < len(batch) - 1:
                        time.sleep(delay_sec)
                if batch_pause_ir > 0 and i < len(batches_ir) - 1:
                    logger.info("批间暂停 %.0f 秒", batch_pause_ir)
                    time.sleep(batch_pause_ir)
        else:
            if _ingest_enabled("INDUSTRY_REVENUE") and not n_total:
                logger.warning("全量采集 step3: 行业/财务 已开启但无标的，请先拉取 universe")
            else:
                logger.info("全量采集 step3: 跳过行业/财务（INGEST_PRODUCTION_INDUSTRY_REVENUE=0）")

        # ④ 新闻：按开关与时间线（全市场 + 按标，可设只保留最近 N 天）
        news_days = _news_days_back()
        if _ingest_enabled("NEWS"):
            logger.info("全量采集 step4: 新闻 | 先全市场+宏观，再按标的个股新闻（保留最近 %s 天）", news_days if news_days > 0 else "全部")
            run_ingest_news(days_back=news_days if news_days > 0 else None)
            if n_total:
                batch_size_ir = _batch_size()
                batch_pause_ir = _batch_pause_sec()
                delay_sec = _extra_delay_sec()
                batches_news = [symbols_full[i : i + batch_size_ir] for i in range(0, n_total, batch_size_ir)]
                for i, batch in enumerate(batches_news):
                    logger.info("个股新闻 第 %s/%s 批（共 %s 只）", i + 1, len(batches_news), len(batch))
                    for j, sym in enumerate(batch):
                        try:
                            run_ingest_news(symbol=sym, days_back=news_days if news_days > 0 else None)
                        except Exception as e:
                            logger.warning("news symbol=%s failed: %s", sym, e)
                        if delay_sec > 0 and j < len(batch) - 1:
                            time.sleep(delay_sec)
                    if batch_pause_ir > 0 and i < len(batches_news) - 1:
                        logger.info("批间暂停 %.0f 秒", batch_pause_ir)
                        time.sleep(batch_pause_ir)
        else:
            logger.info("全量采集 step4: 跳过新闻（INGEST_PRODUCTION_NEWS=0）")

        logger.info("全量采集完成（按 .env 开关与时间线执行，符合 AB 模块生产要求）")
        return 0
    except Exception as e:
        logger.exception("ingest-production failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
