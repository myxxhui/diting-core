#!/usr/bin/env python3
# 每次部署时执行：检查 L1 是否有数据/是否过期，决定全量或增量采集。
# 无数据或数据过旧（超过 INGEST_DEPLOY_FULL_DAYS_THRESHOLD 天）→ 全量；否则 → 增量（当前用 ingest-test 作为增量刷新）。
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
            cur.execute("SELECT COUNT(*), MAX(datetime)::date FROM ohlcv")
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
    timescale_dsn = os.environ.get("TIMESCALE_DSN")
    if not timescale_dsn:
        logger.error("TIMESCALE_DSN 未设置")
        return 1
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root)
    if need_full_ingest(timescale_dsn):
        logger.info("部署采集: 执行全量（ingest-production）")
        r = subprocess.run(
            [sys.executable, str(root / "scripts" / "run_ingest_production.py")],
            cwd=str(root), env=env,
        )
        return r.returncode
    logger.info("部署采集: 执行增量（ingest-test）")
    r = subprocess.run(
        [sys.executable, str(root / "scripts" / "run_ingest_test.py")],
        cwd=str(root), env=env,
    )
    return r.returncode


if __name__ == "__main__":
    sys.exit(main())
