# [Ref: 12_右脑数据支撑与Segment规约] [Ref: 11_数据采集与输入层规约]
# 主营构成：AkShare stock_zygc_em → L2 symbol_business_profile + segment_registry

from __future__ import annotations

import hashlib
import logging
import os
import re
from datetime import datetime, timezone
import time
from typing import Any, List, Optional, Sequence, Tuple

import psycopg2

from diting.ingestion.config import get_pg_l2_dsn
from diting.ingestion.industry_revenue import _apply_akshare_proxy
from diting.ingestion.segment_tier import DISCLOSURE_DEFAULT_TIER

logger = logging.getLogger(__name__)

_SOURCE_AKSHARE = "akshare_zygc"


def stable_segment_id(symbol: str, label_cn: str) -> str:
    """全局稳定 segment_id：同 symbol+披露分部名 唯一。"""
    raw = "%s|%s" % ((symbol or "").strip().upper(), (label_cn or "").strip())
    h = hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]
    return "seg_bp_%s" % h


def infer_domain_from_industry(industry_name: str) -> str:
    """由申万行业名粗映射到 农业/科技/宏观（与 12_、Module A 领域一致）。"""
    s = industry_name or ""
    if any(k in s for k in ("电子", "计算机", "通信", "半导体", "软件", "互联网", "传媒", "电池", "光伏", "风电")):
        return "科技"
    if any(k in s for k in ("农林牧渔", "种植", "养殖", "渔业", "农业", "粮食", "饲料")):
        return "农业"
    return "宏观"


def infer_sub_domain_from_industry(industry_name: str) -> str:
    """
    申万行业名 → segment_registry.sub_domain（赛道/板块，与三分类 domain 正交）。
    当前策略：取 industry_name 截断至 64 字符；后续可接归一化词典。
    """
    s = (industry_name or "").strip()
    if not s:
        return ""
    return s[:64] if len(s) > 64 else s


def _should_skip_row(label: str, share: float) -> bool:
    """过滤「其他(补充)」等噪声行。"""
    if not label or len(label.strip()) < 2:
        return True
    if re.match(r"^其中[:：]", label.strip()):
        return True
    if "补充" in label and share < 0.02:
        return True
    return False


def parse_zygc_dataframe(df: Any) -> Tuple[str, List[Tuple[str, float]]]:
    """
    从 AkShare stock_zygc_em 返回的 DataFrame 解析最新一期、按产品分类的主营分部。
    返回 (report_date_str, [(label_cn, revenue_share), ...])
    """
    if df is None or getattr(df, "empty", True):
        return "", []

    import pandas as pd

    if not isinstance(df, pd.DataFrame):
        return "", []

    d = df.copy()
    if "报告日期" not in d.columns or "主营构成" not in d.columns:
        return "", []

    d["报告日期"] = pd.to_datetime(d["报告日期"], errors="coerce")
    latest = d["报告日期"].max()
    if pd.isna(latest):
        return "", []
    sub = d[d["报告日期"] == latest]
    if "分类类型" in sub.columns:
        prod = sub[sub["分类类型"] == "按产品分类"]
        if not prod.empty:
            sub = prod

    raw_rows: List[Tuple[str, float]] = []
    for _, r in sub.iterrows():
        label = str(r.get("主营构成") or "").strip()
        try:
            share = float(r.get("收入比例") or 0.0)
        except (TypeError, ValueError):
            share = 0.0
        if _should_skip_row(label, share):
            continue
        raw_rows.append((label, max(0.0, min(1.0, share))))

    # 去重：同 label 保留最大占比，按占比降序（首行为最高主营）
    by_label: dict = {}
    for lab, sh in raw_rows:
        by_label[lab] = max(by_label.get(lab, 0.0), sh)
    rows = sorted(by_label.items(), key=lambda x: -x[1])

    rep = latest.strftime("%Y-%m-%d") if hasattr(latest, "strftime") else str(latest)[:10]
    return rep, rows


def fetch_akshare_zygc(symbol: str) -> Any:
    """拉取东方财富主营构成表（原始 DataFrame）。"""
    _apply_akshare_proxy()
    import socket

    import akshare as ak

    sym = (symbol or "").strip()
    # AkShare 底层 HTTP 默认可能无限等待；限制全局 socket 超时，避免批采卡死
    try:
        sec = float((os.environ.get("INGEST_AKSHARE_SOCKET_TIMEOUT_SEC") or "120").strip() or "120")
    except ValueError:
        sec = 120.0
    old = socket.getdefaulttimeout()
    socket.setdefaulttimeout(max(10.0, sec))
    try:
        try:
            return ak.stock_zygc_em(symbol=sym)
        except Exception as e:
            logger.warning("stock_zygc_em 接口异常 symbol=%s: %s", sym, e)
            return None
    finally:
        socket.setdefaulttimeout(old)


def _get_industry_name(conn, symbol: str) -> str:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COALESCE(TRIM(industry_name), '') FROM industry_revenue_summary WHERE symbol = %s",
            (symbol.upper(),),
        )
        row = cur.fetchone()
        return (row[0] or "") if row else ""
    finally:
        cur.close()


def upsert_business_profile_rows(
    conn,
    symbol: str,
    report_date: str,
    rows: Sequence[Tuple[str, float]],
    industry_name: str,
) -> int:
    """
    删除该 symbol 旧行，写入新行并 upsert segment_registry。
    返回写入 symbol_business_profile 行数。
    """
    sym = (symbol or "").strip().upper()
    if not sym or not rows:
        return 0

    domain = infer_domain_from_industry(industry_name)
    sub_domain = infer_sub_domain_from_industry(industry_name)
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM symbol_business_profile WHERE symbol = %s", (sym,))

        n = 0
        for i, (label, share) in enumerate(rows):
            seg_id = stable_segment_id(sym, label)
            is_pri = i == 0
            cur.execute(
                """
                INSERT INTO segment_registry (
                    segment_id, domain, name_cn, sub_domain, segment_tier, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (segment_id) DO UPDATE SET
                    domain = EXCLUDED.domain,
                    name_cn = EXCLUDED.name_cn,
                    sub_domain = EXCLUDED.sub_domain,
                    segment_tier = EXCLUDED.segment_tier,
                    updated_at = NOW()
                """,
                (seg_id, domain, label[:256], sub_domain or None, DISCLOSURE_DEFAULT_TIER),
            )
            cur.execute(
                """
                INSERT INTO symbol_business_profile
                  (symbol, segment_id, segment_label_cn, revenue_share, is_primary, report_date, source, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (sym, seg_id, label[:256], share, is_pri, report_date or None, _SOURCE_AKSHARE),
            )
            n += 1
        conn.commit()
        return n
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _ingest_business_profile_from_dataframe(conn, symbol: str, df: Any) -> int:
    """单只标的：解析 AkShare DataFrame 并写入 L2；返回写入行数。"""
    report_date, rows = parse_zygc_dataframe(df)
    if not rows:
        logger.info("business_profile: symbol=%s 无有效主营分部行", symbol)
        return 0
    iname = _get_industry_name(conn, symbol)
    n = upsert_business_profile_rows(conn, symbol, report_date, rows, iname)
    logger.info(
        "business_profile: symbol=%s report=%s rows=%s segments=%s",
        symbol,
        report_date,
        n,
        len(rows),
    )
    return n


def run_ingest_business_profile(symbol: Optional[str] = None) -> int:
    """
    采集单只标的的主营构成并写入 L2。
    依赖：L2 已存在 industry_revenue_summary（取 industry 映射 domain）、本表 DDL 已执行。
    返回写入行数；失败返回 0。
    """
    symbol = (symbol or "").strip()
    if not symbol:
        return 0

    if (get_pg_l2_dsn() or "").strip() == "":
        logger.warning("business_profile: 未配置 PG_L2_DSN，跳过")
        return 0

    df = fetch_akshare_zygc(symbol)
    if df is None:
        return 0

    dsn = get_pg_l2_dsn()
    conn = psycopg2.connect(dsn)
    try:
        return _ingest_business_profile_from_dataframe(conn, symbol, df)
    finally:
        conn.close()


def run_ingest_business_profile_batch(
    symbols: Sequence[str],
    pause_sec: float = 0.35,
) -> Tuple[int, int, int]:
    """
    顺序拉取多只标的的主营构成（AkShare），复用单库连接写 L2。
    返回 (写入行数>0 的标的数, 无数据或 0 行写入的标的数, 累计写入行数)。
    pause_sec：相邻请求间隔，降低源站限频概率；环境变量 INGEST_BUSINESS_BATCH_PAUSE_SEC 可覆盖。
    """
    syms = [s.strip() for s in symbols if (s or "").strip()]
    if not syms:
        return 0, 0, 0
    env_pause = os.environ.get("INGEST_BUSINESS_BATCH_PAUSE_SEC", "").strip()
    if env_pause:
        try:
            pause_sec = max(0.0, float(env_pause))
        except ValueError:
            pass
    dsn = (get_pg_l2_dsn() or "").strip()
    if not dsn:
        logger.warning("business_profile batch: 未配置 PG_L2_DSN，跳过")
        return 0, len(syms), 0

    ok_syms = 0
    zero_syms = 0
    total_rows = 0
    conn = psycopg2.connect(dsn)
    try:
        for i, sym in enumerate(syms):
            try:
                df = fetch_akshare_zygc(sym)
                if df is None:
                    zero_syms += 1
                    continue
                n = _ingest_business_profile_from_dataframe(conn, sym, df)
                if n > 0:
                    ok_syms += 1
                    total_rows += n
                else:
                    zero_syms += 1
            except Exception as e:
                logger.warning("business_profile batch symbol=%s: %s", sym, e)
                zero_syms += 1
            if pause_sec > 0 and i + 1 < len(syms):
                time.sleep(pause_sec)
    finally:
        conn.close()
    logger.info(
        "business_profile batch: 标的=%s 成功写入=%s 无行/失败=%s 累计行=%s",
        len(syms),
        ok_syms,
        zero_syms,
        total_rows,
    )
    return ok_syms, zero_syms, total_rows
