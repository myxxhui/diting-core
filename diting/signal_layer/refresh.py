# [Ref: 12_右脑数据支撑与Segment规约] [Ref: 06_B轨_信号层生产级数据采集_设计]
# 信号层编排：解析标的→细分 → 按 segment 拉取 → 信号理解 → 写 segment_signal_cache

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import psycopg2

from diting.ingestion.segment_tier import tier_int_to_signal_key
from diting.signal_layer.adapters import get_adapter_for_segment
from diting.signal_layer.models import RefreshSegmentSignalsResult
from diting.signal_layer.understanding import understand_signal

logger = logging.getLogger(__name__)

_DEFAULT_TTL_SEC = 3600


def _build_understanding_config(cfg: dict, track: str = "a") -> dict:
    """合并 YAML + ENV 的 AI 配置；track 优先合并 tracks.{track}_track.understanding。"""
    sig_cfg = cfg.get("signal_layer") or cfg.get("signal_understanding") or cfg
    su_cfg = cfg.get("signal_understanding") or sig_cfg
    track_key = "a_track" if str(track).strip().lower() in ("a", "") else "b_track"
    track_cfg = (cfg.get("tracks") or {}).get(track_key) or {}
    track_su = track_cfg.get("understanding") or {}
    if track_su:
        su_cfg = {**su_cfg, **track_su}
    api_key = (
        os.environ.get("SIGNAL_LAYER_API_KEY") or
        os.environ.get("OPENAI_API_KEY") or
        su_cfg.get("api_key") or ""
    ).strip()
    model_id = (
        os.environ.get("SIGNAL_LAYER_MODEL_ID") or
        su_cfg.get("model_id") or
        su_cfg.get("provider") or ""
    ).strip()
    base_url = (
        os.environ.get("SIGNAL_LAYER_BASE_URL") or
        su_cfg.get("base_url") or ""
    ).strip()
    root = Path(__file__).resolve().parents[2]
    prompt_path = su_cfg.get("prompt_path") or ""
    if prompt_path and not Path(prompt_path).is_absolute():
        prompt_path = str(root / prompt_path)
    return {
        "mode": su_cfg.get("mode") or "rule_first_then_ai",
        "max_input_chars": int(su_cfg.get("max_input_chars") or sig_cfg.get("max_input_chars") or 4096),
        "model_id": model_id,
        "api_key": api_key,
        "base_url": base_url or None,
        "prompt_path": prompt_path or su_cfg.get("prompt_path"),
        "max_input_tokens": int(su_cfg.get("max_input_tokens") or 1024),
        "max_output_tokens": int(su_cfg.get("max_output_tokens") or 256),
        "retry_times": int(su_cfg.get("retry_times") or 2),
        "retry_backoff_sec": float(su_cfg.get("retry_backoff_sec") or 2),
        "timeout_sec": int(su_cfg.get("timeout_sec") or 30),
        "model_override_by_tier": su_cfg.get("model_override_by_tier") or {},
    }


def _load_config(config_path: Optional[str] = None) -> dict:
    import os
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    path = config_path or os.environ.get("SIGNAL_LAYER_CONFIG") or str(root / "config" / "signal_layer.yaml")
    if not Path(path).exists():
        return {}
    try:
        import yaml
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning("signal_layer 配置加载失败 %s: %s", path, e)
        return {}


def _parse_segments_from_symbols(
    conn,
    symbols: List[str],
) -> Tuple[Set[str], Dict[str, List[str]], Dict[str, str], Dict[str, Optional[int]], List[str]]:
    """
    从 symbol_business_profile 解析标的→细分。返回：
    (segment_ids, segment_to_symbols, segment_to_name_cn, segment_to_tier, symbols_without_segments)
    """
    syms = [str(s).strip().upper() for s in symbols if (s or "").strip()]
    if not syms:
        return set(), {}, {}, {}, list(symbols)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT s.symbol, s.segment_id, r.name_cn, r.segment_tier
            FROM symbol_business_profile s
            LEFT JOIN segment_registry r ON r.segment_id = s.segment_id
            WHERE s.symbol = ANY(%s)
            """,
            (syms,),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
    segment_to_symbols: Dict[str, List[str]] = {}
    segment_to_name_cn: Dict[str, str] = {}
    segment_to_tier: Dict[str, Optional[int]] = {}
    for sym, seg_id, name_cn, tier in (rows or []):
        sid = str(seg_id or "").strip()
        if not sid:
            continue
        if sid not in segment_to_symbols:
            segment_to_symbols[sid] = []
        if sym not in segment_to_symbols[sid]:
            segment_to_symbols[sid].append(sym)
        segment_to_name_cn[sid] = str(name_cn or "").strip() or sid
        if tier is not None:
            try:
                segment_to_tier[sid] = int(tier)
            except (TypeError, ValueError):
                segment_to_tier[sid] = None
        elif sid not in segment_to_tier:
            segment_to_tier[sid] = None
    found = set(segment_to_symbols.keys())
    symbols_without = [s for s in syms if not any(s in segment_to_symbols.get(seg, []) for seg in found)]
    return found, segment_to_symbols, segment_to_name_cn, segment_to_tier, symbols_without


def _check_ttl(conn, segment_id: str, ttl_sec: int) -> bool:
    """缓存未过期返回 True（跳过拉取）。"""
    if ttl_sec <= 0:
        return False
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT fetched_at FROM segment_signal_cache
            WHERE segment_id = %s AND fetched_at >= NOW() - INTERVAL '1 second' * %s
            """,
            (segment_id, ttl_sec),
        )
        return cur.fetchone() is not None
    except Exception:
        return False
    finally:
        cur.close()


def _check_audit_reuse_same_day(conn, segment_id: str) -> Optional[dict]:
    """同 segment 同一自然日已有成功结论则返回 signal_summary 复用，否则 None。"""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT signal_summary FROM segment_signal_cache
            WHERE segment_id = %s AND DATE(fetched_at) = CURRENT_DATE
            """,
            (segment_id,),
        )
        row = cur.fetchone()
        if row:
            try:
                return json.loads(row[0]) if isinstance(row[0], str) else row[0]
            except (TypeError, json.JSONDecodeError):
                pass
    except Exception:
        pass
    finally:
        cur.close()
    return None


def _write_audit(
    conn,
    segment_id: str,
    source_type: str,
    raw_snippet: Optional[str],
    model_conclusion_json: Optional[str],
    error_message: Optional[str],
) -> None:
    """写入 segment_signal_audit 一条。"""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO segment_signal_audit (segment_id, source_type, raw_snippet, model_conclusion_json, error_message)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (segment_id, source_type, raw_snippet or "", model_conclusion_json or "", error_message or ""),
        )
        conn.commit()
    except Exception as e:
        logger.warning("segment_signal_audit 写入失败: %s", e)
    finally:
        cur.close()


def _upsert_cache(
    conn,
    segment_id: str,
    signal_summary: dict,
    ttl_sec: int = _DEFAULT_TTL_SEC,
) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO segment_signal_cache (segment_id, signal_summary, signal_at, fetched_at, ttl_sec)
            VALUES (%s, %s, NOW(), NOW(), %s)
            ON CONFLICT (segment_id) DO UPDATE SET
                signal_summary = EXCLUDED.signal_summary,
                signal_at = EXCLUDED.signal_at,
                fetched_at = NOW(),
                ttl_sec = EXCLUDED.ttl_sec
            """,
            (segment_id, json.dumps(signal_summary, ensure_ascii=False), ttl_sec),
        )
        conn.commit()
    finally:
        cur.close()


def refresh_segment_signals_for_symbols(
    symbols: List[str],
    pg_l2_dsn: str,
    config: Optional[dict] = None,
    options: Optional[dict] = None,
) -> RefreshSegmentSignalsResult:
    """
    按候选标的解析细分 → 按 segment 拉取生产级数据 → 信号理解打标 → 写 segment_signal_cache。
    全部生产级数据，无 Mock。
    """
    result = RefreshSegmentSignalsResult()
    opt = options or {}
    track = str(opt.get("track") or os.environ.get("DITING_TRACK", "a")).strip().lower() or "a"
    result.summary["track"] = track
    result.summary["total_symbols"] = len([s for s in symbols if (s or "").strip()])
    if not pg_l2_dsn or not symbols:
        return result
    cfg = config or _load_config()
    sig_cfg = cfg.get("signal_layer") or cfg.get("signal_understanding") or cfg
    max_chars = int(sig_cfg.get("max_input_chars") or 4096)
    ttl_sec = int(sig_cfg.get("ttl_sec") or opt.get("ttl_sec") or _DEFAULT_TTL_SEC)
    fallback = sig_cfg.get("fallback_on_failure", True)
    audit_enabled = sig_cfg.get("audit_enabled", False)
    su_cfg = cfg.get("signal_understanding") or sig_cfg
    audit_reuse_same_day = su_cfg.get("audit_reuse_same_day", False)
    base_understanding_config = _build_understanding_config(cfg, track)
    conn = psycopg2.connect(pg_l2_dsn)
    try:
        all_segments, seg_to_syms, seg_to_name, seg_to_tier, symbols_without = _parse_segments_from_symbols(conn, symbols)
        result.symbols_without_segments = symbols_without
        result.summary["symbols_with_segments"] = result.summary["total_symbols"] - len(symbols_without)
        result.summary["segments_resolved"] = len(all_segments)
        segments_without_adapter = []
        segments_skipped = []
        segments_written = []
        segments_failed = {}
        adapter_prefix = cfg.get("adapter_by_prefix") or {}
        for seg_id in sorted(all_segments):
            adapter = get_adapter_for_segment(seg_id, cfg)
            if adapter is None:
                segments_without_adapter.append(seg_id)
                continue
            if _check_ttl(conn, seg_id, ttl_sec):
                segments_skipped.append(seg_id)
                continue
            understanding_config = dict(base_understanding_config)
            tier_key = tier_int_to_signal_key(seg_to_tier.get(seg_id), seg_id)
            override = base_understanding_config.get("model_override_by_tier") or {}
            if tier_key in override and override[tier_key]:
                understanding_config["model_id"] = str(override[tier_key]).strip()
            if audit_reuse_same_day:
                reused = _check_audit_reuse_same_day(conn, seg_id)
                if reused:
                    _upsert_cache(conn, seg_id, reused, ttl_sec)
                    segments_written.append(seg_id)
                    continue
            syms = seg_to_syms.get(seg_id, [])
            name_cn = seg_to_name.get(seg_id, "")
            ctx = {
                "symbols": syms,
                "name_cn": name_cn,
                "pg_l2_dsn": pg_l2_dsn,
                "max_input_chars": max_chars,
                "days_back": int(sig_cfg.get("days_back") or 7),
            }
            raw = adapter.fetch_raw(seg_id, ctx)
            if not raw or len(raw.strip()) < 10:
                segments_failed[seg_id] = "拉取无数据"
                continue

            def _audit_cb(seg: str, src: str, snip: Optional[str], concl: Optional[str], err: Optional[str]) -> None:
                if audit_enabled:
                    _write_audit(conn, seg, src, snip, concl, err)

            tagged = understand_signal(raw, seg_id, understanding_config, audit_callback=_audit_cb)
            if not tagged:
                if fallback:
                    tagged = {
                        "type": "policy",
                        "direction": "neutral",
                        "strength": 0.5,
                        "summary_cn": raw[:200] + ("…" if len(raw) > 200 else ""),
                        "risk_tags": [],
                    }
                    if audit_enabled:
                        _write_audit(conn, seg_id, "fallback", raw[:2048], json.dumps(tagged, ensure_ascii=False), "信号理解失败，使用中性兜底")
                    _upsert_cache(conn, seg_id, tagged, ttl_sec)
                    segments_written.append(seg_id)
                else:
                    segments_failed[seg_id] = "信号理解失败"
                continue
            try:
                _upsert_cache(conn, seg_id, tagged, ttl_sec)
                segments_written.append(seg_id)
            except Exception as e:
                segments_failed[seg_id] = str(e)
        result.segments_without_adapter = segments_without_adapter
        result.segments_skipped_ttl = segments_skipped
        result.segments_written = segments_written
        result.segments_failed = segments_failed
        result.summary["segments_skipped_ttl"] = len(segments_skipped)
        result.summary["segments_written"] = len(segments_written)
        result.summary["segments_failed"] = len(segments_failed)
        # 有细分信号的标的：至少 1 个细分已写入或 TTL 内有效（缓存未过期），均可供 C 使用
        symbols_with_signal_set: Set[str] = set()
        for seg_id in segments_written + segments_skipped:
            for sym in seg_to_syms.get(seg_id, []):
                if sym:
                    symbols_with_signal_set.add(sym)
        result.symbols_with_signal = sorted(symbols_with_signal_set)
    finally:
        conn.close()
    return result
