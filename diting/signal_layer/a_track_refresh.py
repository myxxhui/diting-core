# [Ref: 06_A轨 信号层] 标的 news + 申万行业 news 双路分析打标 → a_track_signal_cache
# [Ref: 07_行业新闻与标的新闻分离存储]

from __future__ import annotations

import json
import logging
import os
from typing import Dict, List, Optional, Set

import psycopg2

from diting.signal_layer.models import ATrackRefreshResult
from diting.signal_layer.news_fetch import fetch_industry_news_text, fetch_symbol_news_text
from diting.signal_layer.refresh import _build_understanding_config, _load_config
from diting.signal_layer.understanding.engine import is_llm_configured, understand_signal

logger = logging.getLogger(__name__)

_DEFAULT_TTL_SEC = 3600


def _norm_industry(name: str) -> str:
    """与 industry_revenue_summary.industry_name 一致，用于 cache_key / scope_id。"""
    return (name or "").strip()[:128]


def _cache_key_symbol(symbol: str) -> str:
    return "sym:%s" % (symbol or "").strip().upper()


def _cache_key_industry(ind: str) -> str:
    n = _norm_industry(ind)
    return "ind:%s" % n if n else "ind:"


def _check_ttl(conn, cache_key: str, ttl_sec: int) -> bool:
    if ttl_sec <= 0:
        return False
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT fetched_at FROM a_track_signal_cache
            WHERE cache_key = %s
              AND fetched_at >= NOW() - INTERVAL '1 second' * %s
            """,
            (cache_key, ttl_sec),
        )
        return cur.fetchone() is not None
    except Exception:
        return False
    finally:
        cur.close()


def _upsert_a_track_cache(
    conn,
    cache_key: str,
    track_scope: str,
    track_id: str,
    signal_summary: dict,
    ttl_sec: int = _DEFAULT_TTL_SEC,
) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO a_track_signal_cache (cache_key, track_scope, track_id, signal_summary, signal_at, fetched_at, ttl_sec)
            VALUES (%s, %s, %s, %s, NOW(), NOW(), %s)
            ON CONFLICT (cache_key) DO UPDATE SET
                signal_summary = EXCLUDED.signal_summary,
                signal_at = EXCLUDED.signal_at,
                fetched_at = NOW(),
                ttl_sec = EXCLUDED.ttl_sec
            """,
            (cache_key, track_scope, track_id, json.dumps(signal_summary, ensure_ascii=False), ttl_sec),
        )
        conn.commit()
    finally:
        cur.close()


def _industry_by_symbols(conn, symbols: List[str]) -> Dict[str, str]:
    """symbol -> industry_name（申万，与 industry_revenue_summary 同源）。"""
    syms = [str(s).strip().upper() for s in symbols if (s or "").strip()]
    if not syms:
        return {}
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT symbol, COALESCE(TRIM(industry_name), '')
            FROM industry_revenue_summary
            WHERE symbol = ANY(%s)
            """,
            (syms,),
        )
        return {str(r[0] or "").strip().upper(): str(r[1] or "").strip() for r in (cur.fetchall() or [])}
    except Exception as e:
        logger.warning("industry_revenue_summary 批量读取失败: %s", e)
        return {}
    finally:
        cur.close()


def refresh_a_track_signals_for_symbols(
    symbols: List[str],
    pg_l2_dsn: str,
    config: Optional[dict] = None,
    options: Optional[dict] = None,
) -> ATrackRefreshResult:
    """
    A 轨：对每只标的拉取「标的新闻/公告」+ 对其申万行业拉取「行业新闻/公告」，
    已配置大模型时打标写入 a_track_signal_cache（sym: / ind:）；未配置时不写（或仅失败兜底见 fallback_on_failure）。
    行业新闻在行业维度去重：同一批多只同属一行业只打标一次。
    """
    result = ATrackRefreshResult()
    opt = options or {}
    track = str(opt.get("track") or os.environ.get("DITING_TRACK", "a")).strip().lower() or "a"
    cfg = config or _load_config()
    sig_cfg = cfg.get("signal_layer") or cfg.get("signal_understanding") or cfg
    max_chars = int(sig_cfg.get("max_input_chars") or 4096)
    ttl_sec = int(sig_cfg.get("ttl_sec") or opt.get("ttl_sec") or _DEFAULT_TTL_SEC)
    days_back = int(sig_cfg.get("days_back") or opt.get("days_back") or 7)
    fallback = sig_cfg.get("fallback_on_failure", True)
    base_understanding_config = _build_understanding_config(cfg, track)
    llm_ok = is_llm_configured(base_understanding_config)

    syms = sorted({str(s).strip().upper() for s in symbols if (s or "").strip()})
    result.summary["total_symbols"] = len(syms)
    if not pg_l2_dsn or not syms:
        return result

    conn = psycopg2.connect(pg_l2_dsn)
    try:
        sym_to_ind = _industry_by_symbols(conn, syms)
        unique_industries: Set[str] = {v for v in sym_to_ind.values() if v}

        # —— 标的级 ——
        for sym in syms:
            ck = _cache_key_symbol(sym)
            if _check_ttl(conn, ck, ttl_sec):
                result.symbols_skipped_ttl.append(sym)
                continue
            raw = fetch_symbol_news_text(conn, sym, days_back=days_back, max_chars=max_chars)
            if not raw or len(raw.strip()) < 10:
                result.symbols_failed[sym] = "拉取无数据"
                continue
            tagged = understand_signal(raw, ck, base_understanding_config)
            if not tagged:
                if not llm_ok:
                    result.symbols_failed[sym] = "未配置大模型(SIGNAL_LAYER_API_KEY+SIGNAL_LAYER_MODEL_ID 或 YAML api_key+model_id)，已跳过打标"
                elif fallback:
                    tagged = {
                        "type": "policy",
                        "direction": "neutral",
                        "strength": 0.5,
                        "summary_cn": raw[:200] + ("…" if len(raw) > 200 else ""),
                        "risk_tags": [],
                        "signal_source": "fallback_neutral",
                    }
                else:
                    result.symbols_failed[sym] = "信号理解失败"
                if not tagged:
                    continue
            tagged = dict(tagged)
            tagged["source_scope"] = "symbol"
            tagged["symbol"] = sym
            _upsert_a_track_cache(conn, ck, "symbol", sym, tagged, ttl_sec)
            result.symbols_written.append(sym)

        # —— 申万行业级（批内去重）——
        for ind in sorted(unique_industries):
            ck = _cache_key_industry(ind)
            if _check_ttl(conn, ck, ttl_sec):
                result.industries_skipped_ttl.append(ind)
                continue
            raw = fetch_industry_news_text(conn, ind, days_back=days_back, max_chars=max_chars)
            if not raw or len(raw.strip()) < 10:
                result.industries_failed[ind] = "拉取无数据"
                continue
            tagged = understand_signal(raw, ck, base_understanding_config)
            if not tagged:
                if not llm_ok:
                    result.industries_failed[ind] = "未配置大模型，已跳过打标"
                elif fallback:
                    tagged = {
                        "type": "policy",
                        "direction": "neutral",
                        "strength": 0.5,
                        "summary_cn": raw[:200] + ("…" if len(raw) > 200 else ""),
                        "risk_tags": [],
                        "signal_source": "fallback_neutral",
                    }
                else:
                    result.industries_failed[ind] = "信号理解失败"
                if not tagged:
                    continue
            tagged = dict(tagged)
            tagged["source_scope"] = "industry"
            tagged["industry_name"] = ind
            _upsert_a_track_cache(conn, ck, "industry", ind, tagged, ttl_sec)
            result.industries_written.append(ind)

        result.summary["symbols_written"] = len(result.symbols_written)
        result.summary["industries_written"] = len(result.industries_written)
        result.summary["symbols_skipped_ttl"] = len(result.symbols_skipped_ttl)
        result.summary["industries_skipped_ttl"] = len(result.industries_skipped_ttl)
        result.summary["symbols_failed"] = len(result.symbols_failed)
        result.summary["industries_failed"] = len(result.industries_failed)
    finally:
        conn.close()
    return result
