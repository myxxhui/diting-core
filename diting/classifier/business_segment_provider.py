# [Ref: 12_右脑数据支撑与Segment规约] [Ref: 01_语义分类器_实践]
# 从 L2 symbol_business_profile 批量读取主营构成，转为 ClassifierOutput.segment_shares

import logging
from typing import Callable, Dict, List, Optional, Tuple

from diting.protocols.classifier_pb2 import SegmentShare

logger = logging.getLogger(__name__)


def get_business_segment_shares_batch(dsn: str, symbols: List[str]) -> Dict[str, List[SegmentShare]]:
    """
    一次查询 L2，返回 symbol.upper() -> [SegmentShare, ...]（按 revenue_share 降序）。
    表不存在或查询失败时返回空 dict。
    """
    if not dsn or not symbols:
        return {}
    try:
        import psycopg2
    except ImportError:
        return {}

    sym_list = [s.strip().upper() for s in symbols if (s or "").strip()]
    if not sym_list:
        return {}

    out: Dict[str, List[SegmentShare]] = {}
    try:
        conn = psycopg2.connect(dsn)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT symbol, segment_id, revenue_share, is_primary
                FROM symbol_business_profile
                WHERE symbol = ANY(%s)
                ORDER BY symbol, revenue_share DESC
                """,
                (sym_list,),
            )
            for sym, seg_id, rev, is_pri in cur.fetchall():
                key = (sym or "").strip().upper()
                out.setdefault(key, []).append(
                    SegmentShare(
                        segment_id=str(seg_id or ""),
                        revenue_share=float(rev or 0),
                        is_primary=bool(is_pri),
                    )
                )
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("symbol_business_profile 批量读取失败: %s", e)
        return {}

    return out


def get_segment_disclosure_names_batch(
    dsn: str, symbols: List[str], limit: int = 8
) -> Dict[str, List[str]]:
    """
    每只标的按营收占比降序的主营披露分部中文名列表（来自 segment_registry.name_cn）。
    用于电力兜底时从多行披露中解析水电/火电/售电等子类；无行或失败返回 {}。
    """
    if not dsn or not symbols or limit < 1:
        return {}
    try:
        import psycopg2
    except ImportError:
        return {}

    syms = [s.strip().upper() for s in symbols if (s or "").strip()]
    if not syms:
        return {}

    out: Dict[str, List[str]] = {}
    try:
        conn = psycopg2.connect(dsn)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT s.symbol, r.name_cn, s.revenue_share
                FROM symbol_business_profile s
                INNER JOIN segment_registry r ON r.segment_id = s.segment_id
                WHERE s.symbol = ANY(%s)
                ORDER BY s.symbol, s.revenue_share DESC
                """,
                (syms,),
            )
            for sym, name_cn, _rev in cur.fetchall():
                k = (sym or "").strip().upper()
                name = (name_cn or "").strip()
                if not name:
                    continue
                lst = out.setdefault(k, [])
                if name not in lst and len(lst) < limit:
                    lst.append(name)
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_segment_disclosure_names_batch: %s", e)
        return {}

    return out


def get_top_segment_disclosure_batch(dsn: str, symbols: List[str]) -> Dict[str, Tuple[str, float]]:
    """
    每只标的主营披露 Top1：symbol.upper() -> (segment_registry.name_cn, revenue_share)。
    用于 Module A 在申万仅「电力」时按披露细化标签；无行或失败返回 {}。
    """
    if not dsn or not symbols:
        return {}
    try:
        import psycopg2
    except ImportError:
        return {}

    syms = [s.strip().upper() for s in symbols if (s or "").strip()]
    if not syms:
        return {}

    out: Dict[str, Tuple[str, float]] = {}
    try:
        conn = psycopg2.connect(dsn)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT ON (s.symbol) s.symbol, r.name_cn, s.revenue_share
                FROM symbol_business_profile s
                INNER JOIN segment_registry r ON r.segment_id = s.segment_id
                WHERE s.symbol = ANY(%s)
                ORDER BY s.symbol, s.revenue_share DESC
                """,
                (syms,),
            )
            for sym, name_cn, rev in cur.fetchall():
                k = (sym or "").strip().upper()
                out[k] = ((name_cn or "").strip(), float(rev or 0.0))
            cur.close()
        finally:
            conn.close()
    except Exception as e:
        logger.debug("get_top_segment_disclosure_batch: %s", e)
        return {}

    return out


def make_business_segment_provider(
    dsn: str, symbols: List[str]
) -> Optional[Callable[[str], List[SegmentShare]]]:
    """构造 SemanticClassifier 用的 (symbol) -> List[SegmentShare]；无数据时返回 None 表示未注入。"""
    batch = get_business_segment_shares_batch(dsn, symbols)
    if not batch:
        return None

    def provider(symbol: str) -> List[SegmentShare]:
        return batch.get((symbol or "").strip().upper(), [])

    return provider
