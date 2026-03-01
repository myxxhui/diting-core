# [Ref: 03_原子目标与规约/_共享规约/11_数据采集与输入层规约] 全 A 股标的池读取
# 统一接口 get_current_a_share_universe() -> List[str]；内部「检查有效条件→若无效则触发更新→读表返回」

import logging
from datetime import date, datetime, timezone
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)

# 与 ingestion.universe 表名一致
TABLE_NAME = "a_share_universe"


def _get_conn():
    """延迟依赖，避免顶层导入 ingestion 造成循环。"""
    import psycopg2
    from diting.ingestion.config import get_timescale_dsn
    return psycopg2.connect(get_timescale_dsn())


def _is_valid_updated_at(updated_at: Optional[datetime]) -> bool:
    """
    有效条件：当日已更新（updated_at 为当日）。
    11_ 约定：默认建议「当日已更新」或「当前交易日已更新」；此处用 UTC 当日。
    """
    if updated_at is None:
        return False
    today = datetime.now(timezone.utc).date()
    if hasattr(updated_at, "date"):
        d = updated_at.astimezone(timezone.utc).date() if getattr(updated_at, "tzinfo", None) else updated_at.date()
        return d >= today
    return True


def get_current_a_share_universe(
    conn=None,
    refresh_callback: Optional[Callable[[], None]] = None,
    *,
    force_refresh: bool = False,
) -> List[str]:
    """
    获取当前全 A 股标的池（universe），与 09_/11_ 约定一致。

    行为：检查表内数据有效条件（默认：当日已更新）→ 若无效则触发更新（调用 refresh_callback
    或 run_ingest_universe）→ 读表返回 symbol 列表。

    :param conn: 可选，已打开的 DB 连接；不传则内部创建并关闭。
    :param refresh_callback: 可选，无效时调用的刷新函数；不传则调用 run_ingest_universe。
    :param force_refresh: 为 True 时先执行刷新再读表。
    :return: 标的代码列表，如 ["000001.SZ", "600000.SH", ...]
    """
    own_conn = False
    if conn is None:
        conn = _get_conn()
        own_conn = True

    try:
        cur = conn.cursor()
        try:
            if force_refresh:
                _do_refresh(refresh_callback, conn)
            else:
                cur.execute(f"SELECT MAX(updated_at) FROM {TABLE_NAME}")
                row = cur.fetchone()
                max_updated = row[0] if row else None
                if not _is_valid_updated_at(max_updated):
                    _do_refresh(refresh_callback, conn)

            cur.execute(f"SELECT symbol FROM {TABLE_NAME} ORDER BY symbol")
            symbols = [r[0] for r in cur.fetchall()]
            logger.info("get_current_a_share_universe: len(universe)=%s", len(symbols))
            return symbols
        finally:
            cur.close()
    finally:
        if own_conn:
            conn.close()


def _do_refresh(refresh_callback: Optional[Callable[[], None]], conn) -> None:
    """执行刷新：若提供 callback 则调用；否则调用 run_ingest_universe（会自建连接）。"""
    if refresh_callback is not None:
        refresh_callback()
        return
    from diting.ingestion.universe import run_ingest_universe
    run_ingest_universe()
