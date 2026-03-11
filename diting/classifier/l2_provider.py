# [Ref: 01_语义分类器_实践] L2 行业/营收数据提供者，供 SemanticClassifier 按标的查询
# 当 PG_L2_DSN 可用时由 run.py 注入，对 universe 中每只标的从 L2 表 industry_revenue_summary 读取

import logging
from typing import Callable, Dict, List, Tuple

logger = logging.getLogger(__name__)

_DEFAULT_MISSING = ("未知", 0.0, 0.0, 0.0)


def get_l2_industry_revenue_batch(
    dsn: str, symbols: List[str]
) -> Dict[str, Tuple[str, float, float, float]]:
    """
    一次查询 L2 获取多只标的的行业/营收数据，返回 symbol -> (industry_name, revenue_ratio, rnd_ratio, commodity_ratio)。
    表不存在或某 symbol 无记录时，该 symbol 不在返回 dict 中（调用方用默认值填充）。
    """
    import psycopg2

    if not symbols:
        return {}
    out = {}
    try:
        conn = psycopg2.connect(dsn)
        try:
            cur = conn.cursor()
            # 统一大写，与库中 symbol 一致
            sym_list = [s.strip().upper() for s in symbols if (s or "").strip()]
            if not sym_list:
                return {}
            cur.execute(
                """
                SELECT symbol, industry_name, revenue_ratio, rnd_ratio, commodity_ratio
                FROM industry_revenue_summary
                WHERE symbol = ANY(%s)
                """,
                (sym_list,),
            )
            for row in cur.fetchall():
                if row and len(row) >= 5:
                    out[row[0]] = (
                        row[1] or "",
                        float(row[2] or 0),
                        float(row[3] or 0),
                        float(row[4] or 0),
                    )
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("L2 批量查询行业/营收失败: %s", e)
    return out


def get_l2_industry_revenue_provider(dsn: str) -> Callable[[str], Tuple[str, float, float, float]]:
    """
    返回基于 L2 的 provider：(symbol) -> (industry_name, revenue_ratio, rnd_ratio, commodity_ratio)。
    若表不存在或该 symbol 无记录，返回 ("未知", 0.0, 0.0, 0.0)。
    建议在 run 中优先使用 get_l2_industry_revenue_batch + 内存 dict，避免每只标的单独查库。
    """
    def provider(symbol: str) -> Tuple[str, float, float, float]:
        data = get_l2_industry_revenue_batch(dsn, [symbol])
        return data.get((symbol or "").strip().upper(), _DEFAULT_MISSING)
    return provider
