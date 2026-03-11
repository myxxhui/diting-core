# [Ref: 03_原子目标与规约/_共享规约/11_数据采集与输入层规约] 全 A 股标的池读取
# 统一接口 get_current_a_share_universe() -> List[str]；内部「检查有效条件→若无效则触发更新→读表返回」

import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


def normalize_symbol(s: str) -> str:
    """
    将代码规范为带交易所后缀：000001 / 000001.SZ -> 000001.SZ，600000 -> 600000.SH。
    沪市 6 开头用 .SH，其余用 .SZ。
    """
    s = (s or "").strip().upper()
    if not s:
        return ""
    if ".SH" in s or ".SZ" in s:
        code = s.split(".")[0]
        return f"{code}.SH" if s.endswith(".SH") else f"{code}.SZ"
    code = s.split(".")[0]
    # 沪市：6 开头、58/51/50 开头（ETF/基金等）
    if code.startswith("6") or code.startswith("58") or code.startswith("51") or code.startswith("50"):
        return f"{code}.SH"
    return f"{code}.SZ"


def parse_symbol_list_from_env(env_key: str) -> Optional[List[str]]:
    """
    从环境变量解析指定股票列表，供采集或 AB 模块使用。
    - 若未设置或为空：返回 None（调用方用全量 universe）。
    - 若为逗号分隔字符串：按逗号拆分并规范化。
    - 若为文件路径（存在且为文件）：按行读取，每行一个代码或 代码.后缀，去空、去重、规范化。
    :return: 规范化后的 symbol 列表 ["000001.SZ", "600000.SH", ...]，或 None
    """
    raw = (os.environ.get(env_key) or "").strip()
    if not raw:
        return None
    symbols: List[str] = []
    if os.path.isfile(raw):
        with open(raw, encoding="utf-8") as f:
            for line in f:
                line = line.strip().split("#")[0].strip()
                if line:
                    symbols.append(normalize_symbol(line))
    else:
        for part in raw.replace("，", ",").split(","):
            part = part.strip()
            if part:
                symbols.append(normalize_symbol(part))
    if not symbols:
        return None
    seen = set()
    out = []
    for sym in symbols:
        if sym and sym not in seen:
            seen.add(sym)
            out.append(sym)
    return out

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
            logger.info("从 L1 读取全 A 股标的数量: %s", len(symbols))
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
