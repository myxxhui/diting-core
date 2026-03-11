#!/usr/bin/env python3
# 每次部署时执行：根据配置决定「指定标的」或「全量」，再根据 L1 是否过期决定全量/增量。
# - DITING_SYMBOLS 指向文件且含有效标的（或为逗号分隔列表）→ 指定标的模式：跑该列表的 OHLCV + 行业/财务 + 新闻。
# - 未设置或为空 → 全量模式：无数据/过期跑全 A 股生产；否则跑增量（测试集 15 标）。
# 工作目录: diting-core；需 TIMESCALE_DSN 等（.env 或 K8s Secret 注入）。

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

# 加载 .env（与 Makefile 一致）
env_file = root / ".env"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

# 超过此天数未更新则触发全量（默认 7 天）
FULL_DAYS_THRESHOLD = int(os.environ.get("INGEST_DEPLOY_FULL_DAYS_THRESHOLD", "7"))


def need_full_ingest(timescale_dsn: str) -> bool:
    """检查 L1 ohlcv 是否为空或最新数据过旧，是则需全量."""
    try:
        import psycopg2
    except ImportError:
        logger.warning("psycopg2 not available, assuming full ingest")
        return True
    try:
        conn = psycopg2.connect(timescale_dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ohlcv'"
            )
            if cur.fetchone() is None:
                conn.close()
                logger.info("L1 表 ohlcv 不存在，需要全量采集")
                return True
            # 时间列名可能为 datetime（规约）或 date，从 information_schema 取兼容
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'ohlcv' "
                "AND data_type IN ('timestamp with time zone', 'timestamp without time zone', 'date') "
                "ORDER BY ordinal_position LIMIT 1"
            )
            time_col_row = cur.fetchone()
            time_col = (time_col_row[0] if time_col_row else "datetime")
            if not (time_col and time_col.replace("_", "").isalnum()):
                time_col = "datetime"
            cur.execute(f"SELECT COUNT(*), MAX({time_col})::date FROM ohlcv")
            row = cur.fetchone()
        conn.close()
        count, max_date = row[0], row[1]
        if count == 0 or max_date is None:
            logger.info("L1 无数据，需要全量采集")
            return True
        cutoff = (datetime.now().date() - timedelta(days=FULL_DAYS_THRESHOLD))
        if max_date < cutoff:
            logger.info("L1 最新数据日期 %s 早于 %s，需要全量采集", max_date, cutoff)
            return True
        logger.info("L1 已有数据且最新日期 %s，执行增量采集（ingest-test）", max_date)
        return False
    except Exception as e:
        logger.warning("检查 L1 失败，默认执行全量: %s", e)
        return True


def main() -> int:
    import subprocess
    from diting.universe import parse_symbol_list_from_env

    timescale_dsn = os.environ.get("TIMESCALE_DSN")
    if not timescale_dsn:
        logger.error("TIMESCALE_DSN 未设置")
        return 1
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root)

    # 指定标的 vs 全量：由 DITING_SYMBOLS（文件或逗号列表）决定，与 Chart ConfigMap 挂载一致
    specified = parse_symbol_list_from_env("DITING_SYMBOLS")
    if specified:
        logger.info("指定标的采集: 共 %s 只（DITING_SYMBOLS）", len(specified))
        if need_full_ingest(timescale_dsn):
            logger.info("部署采集：指定标的全量（OHLCV + 行业/财务 + 新闻）")
            r = subprocess.run(
                [sys.executable, str(root / "scripts" / "run_ingest_production.py")],
                cwd=str(root), env=env,
            )
        else:
            logger.info("部署采集：指定标的增量（最近 N 天 OHLCV + 行业/财务 + 新闻）")
            r = subprocess.run(
                [sys.executable, str(root / "scripts" / "run_ingest_production_incremental.py")],
                cwd=str(root), env=env,
            )
        return r.returncode

    # 全量模式：无 DITING_SYMBOLS 或列表为空
    if need_full_ingest(timescale_dsn):
        logger.info("部署采集：执行全量（全 A 股 + 5 年日线）")
        r = subprocess.run(
            [sys.executable, str(root / "scripts" / "run_ingest_production.py")],
            cwd=str(root), env=env,
        )
        return r.returncode
    logger.info("部署采集：执行增量（测试集 15 标）")
    r = subprocess.run(
        [sys.executable, str(root / "scripts" / "run_ingest_test.py")],
        cwd=str(root), env=env,
    )
    return r.returncode


if __name__ == "__main__":
    sys.exit(main())
