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
    一次查询 L2 获取多只标的的行业/营收数据。
    返回 symbol -> (industry_name, revenue_ratio, rnd_ratio, commodity_ratio)。

    字段语义：
      revenue_ratio   — 主营业务利润率
      rnd_ratio       — 三项费用比重（管理+销售+财务，非独立研发费率）
      commodity_ratio — 大宗商品营收占比（行业规则估算）

    分类器通过 industry_name 关键词匹配打标签，不依赖 rnd_ratio 阈值。
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
