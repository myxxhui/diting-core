#!/usr/bin/env python3
# [Ref: 04_A轨_MoE议会_实践] 一键本地运行 Module C：Router + unified_opinion，可选写入 L2
#
# B↔C（L2）：
#   - 已配 PG_L2_DSN 且未设 MOE_PIPELINE 时默认从 L2 读 B，不重扫。
#   - 未设 MOE_QUANT_BATCH_ID / MOE_CLASSIFIER_BATCH_ID 时，自动使用 L2 中「最近一次写入」的整批 batch_id；可选环境变量覆盖以锁定历史批。
#
# 工作流（效率与 B 对齐）：
#   已配 PG_L2_DSN 且未显式设置 MOE_PIPELINE 时，默认 snapshot：从 L2 读 A+B，不重扫，与 make run-module-b 写入只数一致。
#   MOE_PIPELINE=full：内存 A + QuantScanner 当场重算 B（K 线/分位会变，与上次 B 终端可能不一致）；无 PG_L2_DSN 且未设 MOE_PIPELINE 时默认此项。
#
# 终端：精简摘要 + 明细默认全量（与 B 一致：先确认档再预警档，同档 technical_score 降序）；限制行数设 MOE_C_PRINT_ALL=0 与 MOE_C_PRINT_MAX。
# 生产约定：Module C 只处理「确认档 ∪ 预警档」标的（MOE_C_SCOPE=snapshot，默认），与 B 写入 quant_signal_snapshot 一致。
# MOE_C_SCOPE=passed 仅确认档（不含仅预警）；=all 全 universe，仅建议本地调试。
# 细分占位：默认 0（生产，使用真实 segment_signal_cache）；MOE_STUB_SEGMENT_SIGNALS=1 仅供联调，不得作为生产依据。
# L2 写入 moe_run_metadata（stub、批次、A/B 行数告警等）见 04_A轨_MoE议会_实践「生产级运行与 L2 元数据」。
# 环境摘要见 .env.template「Module C」节。

import os
import sys
import unicodedata
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)

_env = Path(ROOT) / ".env"
if _env.exists():
    with open(_env, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and os.environ.get(k) is None:
                    os.environ[k] = v


def _pipeline_quiet() -> bool:
    return (os.environ.get("PIPELINE_QUIET") or "").strip().lower() in ("1", "true", "yes")


def _domain_tags_zh(out) -> List[str]:
    from diting.protocols.classifier_pb2 import DomainTag

    names: List[str] = []
    for t in out.tags:
        dt = int(getattr(t, "domain_tag", 0))
        if dt == DomainTag.DOMAIN_CUSTOM and (getattr(t, "domain_label", None) or "").strip():
            names.append((t.domain_label or "").strip()[:64])
        elif dt == DomainTag.AGRI:
            names.append("农业")
        elif dt == DomainTag.TECH:
            names.append("科技")
        elif dt == DomainTag.GEO:
            names.append("宏观")
        elif dt == DomainTag.UNKNOWN:
            names.append("未知")
        else:
            names.append("未知")
    return names


def _segment_list_from_classifier(out) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for s in getattr(out, "segment_shares", None) or []:
        rows.append(
            {
                "segment_id": getattr(s, "segment_id", ""),
                "revenue_share": float(getattr(s, "revenue_share", 0) or 0),
                "is_primary": bool(getattr(s, "is_primary", False)),
            }
        )
    return rows


def _stub_segment_signals(segment_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for s in segment_list:
        if s.get("is_primary") and s.get("segment_id"):
            out[str(s["segment_id"])] = {
                "direction": "bullish",
                "strength": 0.75,
                "risk_tags": [],
            }
            break
    if not out and segment_list:
        sid = str(segment_list[0].get("segment_id") or "")
        if sid:
            out[sid] = {"direction": "bullish", "strength": 0.75, "risk_tags": []}
    return out


def _direction_cn(d: int) -> str:
    from diting.moe.experts import SIGNAL_BEARISH, SIGNAL_BULLISH, SIGNAL_NEUTRAL

    try:
        di = int(d)
    except (TypeError, ValueError):
        return "?"
    if di == SIGNAL_BULLISH:
        return "偏多"
    if di == SIGNAL_BEARISH:
        return "偏空"
    if di == SIGNAL_NEUTRAL:
        return "中性"
    return str(di)


def _parse_moe_c_scope() -> str:
    """
    snapshot（默认）：与 write_quant_signal_snapshot 一致 — confirmed_passed 或 alert_passed（无双档时回退 passed）。
    passed：仅确认档 passed=True。
    all：不按 B 门控，对 universe 全量跑 MoE。
    """
    raw = (os.environ.get("MOE_C_SCOPE") or "snapshot").strip().lower()
    if raw in ("all", "full", "universe"):
        return "all"
    if raw in ("passed", "confirmed"):
        return "passed"
    return "snapshot"


def _quant_in_moe_scope(q: Dict[str, Any], scope_mode: str) -> bool:
    if scope_mode == "all":
        return True
    if scope_mode == "passed":
        return bool(q.get("passed"))
    if q.get("alert_passed") is not None or q.get("confirmed_passed") is not None:
        return bool(q.get("confirmed_passed") or q.get("alert_passed"))
    return bool(q.get("passed"))


def _display_width(s: str) -> int:
    w = 0
    for ch in s:
        if unicodedata.east_asian_width(ch) in ("F", "W"):
            w += 2
        else:
            w += 1
    return w


def _truncate_display(s: str, max_dw: int) -> str:
    """按终端显示宽度截断（中日文等宽）。"""
    if not s:
        return "-"
    if _display_width(s) <= max_dw:
        return s
    out: List[str] = []
    w = 0
    for ch in s:
        cw = 2 if unicodedata.east_asian_width(ch) in ("F", "W") else 1
        if w + cw > max_dw - 1:
            out.append("…")
            break
        out.append(ch)
        w += cw
    return "".join(out)


def _moe_reason_floor_and_cap() -> Tuple[int, int]:
    """摘要列：MOE_C_REASON_WIDTH 为最小列宽；MOE_C_REASON_MAX 为硬上限（0=不截断、按本批最长撑开）。"""
    raw = (os.environ.get("MOE_C_REASON_WIDTH") or "").strip()
    floor = 12
    if raw:
        try:
            floor = max(8, min(240, int(raw, 10)))
        except ValueError:
            pass
    elif (os.environ.get("MOE_C_VERBOSE") or "").strip().lower() in ("1", "true", "yes"):
        floor = 40
    cap_raw = (os.environ.get("MOE_C_REASON_MAX") or "").strip()
    if not cap_raw:
        cap = 0
    else:
        try:
            cap = max(0, int(cap_raw, 10))
        except ValueError:
            cap = 0
    return floor, cap


def _moe_reason_col_width(rows: List[List[str]], floor: int, cap: int) -> int:
    """表末列宽度：至少 floor，至多本批最长摘要；cap>0 时列宽不超过 cap。"""
    if not rows:
        return floor
    m = max(_display_width(r[-1]) for r in rows)
    w = max(floor, m)
    if cap > 0:
        w = min(w, cap)
    return w


def _rows_with_reason_cap(rows: List[List[str]], cap: int) -> List[List[str]]:
    """cap>0 时对摘要列截断以便与列宽一致。"""
    if cap <= 0:
        return rows
    return [r[:-1] + [_truncate_display(r[-1], cap)] for r in rows]


def _pad_cell(s: object, width: int, align: str = "left") -> str:
    t = str(s) if s is not None else ""
    extra = width - _display_width(t)
    if extra < 0:
        out = []
        w = 0
        for ch in t:
            cw = 2 if unicodedata.east_asian_width(ch) in ("F", "W") else 1
            if w + cw > width - 1:
                out.append("…")
                break
            out.append(ch)
            w += cw
        t = "".join(out)
        extra = width - _display_width(t)
    pad = max(0, extra)
    if align == "right":
        return " " * pad + t
    if align == "center":
        l = pad // 2
        r = pad - l
        return " " * l + t + " " * r
    return t + " " * pad


def _moe_table_line(cells: List[str], widths: List[int], aligns: List[str]) -> str:
    parts = [_pad_cell(c, w, a) for c, w, a in zip(cells, widths, aligns)]
    return "  " + " ".join(parts)


def _fmt_tags_short(tags: List[str], max_len: int = 28) -> str:
    s = ",".join(tags) if tags else "-"
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def _b_tier_cell(q: Dict[str, Any]) -> str:
    if q.get("confirmed_passed"):
        return "确认"
    if q.get("alert_passed"):
        return "预警"
    return "-"


def _scan_b_tier_counts(sym_to_quant: Dict[str, Any]) -> Tuple[int, int, int]:
    """全量扫描结果：确认档数、仅预警数、snapshot 档数（与 C 门控 snapshot 一致）。"""
    n_conf = 0
    n_alert_only = 0
    for q in sym_to_quant.values():
        c = bool(q.get("confirmed_passed"))
        a = bool(q.get("alert_passed"))
        if c:
            n_conf += 1
        elif a:
            n_alert_only += 1
    n_snap = sum(1 for q in sym_to_quant.values() if _quant_in_moe_scope(q, "snapshot"))
    return n_conf, n_alert_only, n_snap


def _ab_alignment_warnings(
    use_l2_or_a_snap: bool,
    n_a_rows: int,
    n_b_rows: int,
    a_rows: Dict[str, Any],
    sym_to_quant: Dict[str, Any],
    universe: List[str],
) -> List[str]:
    """
    A/B L2 行数或 universe 内 symbol 交集不一致时的可读说明（不阻断运行）。
    前缀 [设计口径]：与「A 全覆盖 universe、B 仅对产出 scan 行的标的有 quant」一致，非批次漂移。
    """
    out: List[str] = []
    if not use_l2_or_a_snap:
        return out

    u_set = {str(s).strip().upper() for s in universe if s and str(s).strip()} if universe else set()
    ak = set(a_rows.keys()) & u_set if a_rows and u_set else set()
    bk = set(sym_to_quant.keys()) & u_set if sym_to_quant and u_set else set()
    only_a = len(ak - bk)
    only_b = len(bk - ak)

    explained = False
    if n_a_rows != n_b_rows:
        if n_a_rows > n_b_rows and only_b == 0 and only_a == (n_a_rows - n_b_rows):
            out.append(
                "[设计口径] L2 classifier=%s 行、quant_scan_all=%s 行，差 %s：与 universe 内「仅 classifier 有、B 无 scan 行」=%s 只一致（多为 L1 不足/未写入 scan_all），非 A/B 批次漂移"
                % (n_a_rows, n_b_rows, n_a_rows - n_b_rows, only_a)
            )
            explained = True
        elif n_b_rows > n_a_rows and only_a == 0 and only_b == (n_b_rows - n_a_rows):
            out.append(
                "[设计口径] quant_scan_all=%s 行多于 classifier=%s 行，差 %s：与 universe 内「仅 B 有」=%s 只一致；请核对是否误读 B 批次或 classifier 未覆盖"
                % (n_b_rows, n_a_rows, n_b_rows - n_a_rows, only_b)
            )
            explained = True
        else:
            out.append(
                "L2 classifier 行数(%s) ≠ quant_scan_all(%s)：请核对 MOE_CLASSIFIER_BATCH_ID / MOE_QUANT_BATCH_ID 是否与 A、B 同一业务跑批一致，或重跑 make run-module-a / make run-module-b"
                % (n_a_rows, n_b_rows)
            )

    if only_a or only_b:
        skip_drift = False
        if explained:
            if n_a_rows > n_b_rows and only_b == 0 and only_a == (n_a_rows - n_b_rows):
                skip_drift = True
            if n_b_rows > n_a_rows and only_a == 0 and only_b == (n_b_rows - n_a_rows):
                skip_drift = True
        if not skip_drift:
            out.append(
                "universe 内 A 快照与 B 量化 symbol 不一致：仅 A 有 %s 只、仅 B 有 %s 只（批次或名单漂移）"
                % (only_a, only_b)
            )
    return out


def _moe_run_metadata_dict(
    *,
    stub: bool,
    enable_vc: bool,
    require_quant_passed: bool,
    pipeline: str,
    scope_mode: str,
    seg_src: str,
    snapshot_batch_hint: Optional[str],
    quant_batch_hint: Optional[str],
    write_batch_id: str,
    n_a_rows: int,
    n_b_rows: int,
    n_sym: int,
    n_universe: int,
    ab_warnings: List[str],
    track: str = "a",
) -> Dict[str, Any]:
    """写入 L2 moe_run_metadata，供判官/审计区分联调与生产、追溯批次。"""
    return {
        "track": track,
        "stub_segment_signals": stub,
        "vc_agent_enabled": enable_vc,
        "require_quant_passed": require_quant_passed,
        "pipeline": pipeline,
        "scope": scope_mode,
        "moe_segment_source": seg_src,
        "classifier_batch_id": snapshot_batch_hint or "",
        "quant_batch_id": quant_batch_hint or "",
        "output_batch_id": write_batch_id or "",
        "l2_classifier_row_count": n_a_rows,
        "l2_quant_scan_all_row_count": n_b_rows,
        "processed_symbols": n_sym,
        "universe_symbols": n_universe,
        "alignment_warnings": ab_warnings,
    }


def _moe_detail_sort_key(q: Dict[str, Any], sym: str) -> Tuple[int, float, str]:
    """明细排序：先确认档再预警档，同档按 technical_score 降序（对齐 B 模块风控表）。"""
    c = bool(q.get("confirmed_passed"))
    a = bool(q.get("alert_passed"))
    if c:
        tier = 0
    elif a:
        tier = 1
    else:
        tier = 2
    return (tier, -float(q.get("technical_score") or 0), sym)


def _short_unsupported_reason(
    router_domain: Optional[str], reasoning: str
) -> str:
    """依实际情况输出短轨不予支持的原因，路由失败单独处理。"""
    if router_domain is None:
        return "路由失败，不予支持"
    r = (reasoning or "").strip()
    if "无法归类或未映射" in r:
        return "路由失败，不予支持"
    if "主营构成为空" in r or "标的主营构成" in r:
        return "主营为空，不予支持"
    if "主营细分无信号" in r:
        return "主营无信号，不予支持"
    if "主营细分利空" in r:
        return "主营利空，不予支持"
    if "全部细分无垂直信号" in r:
        return "无细分信号，不予支持"
    if "利好与主营未对齐" in r or "未对齐" in r:
        return "主营未对齐，不予支持"
    if "量化" in r and ("门控" in r or "未进入" in r):
        return "量化未过，不予支持"
    # 截取首句或前 20 字 + 不予支持
    head = r.split("；")[0].split("。")[0].strip()
    if len(head) > 18:
        head = head[:16] + "…"
    return ("%s，不予支持" % head) if head else "不予支持"


def _moe_detail_cells(
    sym: str,
    bucket: str,
    vertical: List[str],
    router_display: str,
    quant_signal: Dict[str, Any],
    opinions: List[Any],
    enable_vc: bool,
    stub: bool,
    router_domain: Optional[str] = None,
    vert_share_str: str = "",
    primary_rev_wan: float = 0.0,
    secondary_rev_wan: float = 0.0,
) -> List[str]:
    from diting.protocols.brain_pb2 import TIME_HORIZON_LONG_TERM

    tech = float(quant_signal.get("technical_score") or 0.0)
    lt = bool(quant_signal.get("long_term_candidate"))
    tier = _b_tier_cell(quant_signal)
    vert_str = _fmt_tags_short(vertical, 28) if vertical else (bucket or "—")
    rd_disp = router_display or "—"

    vc_s = "-"
    if enable_vc and lt:
        vcs = [o for o in opinions if int(getattr(o, "horizon", 0) or 0) == TIME_HORIZON_LONG_TERM]
        if vcs:
            vc_s = "有" if getattr(vcs[0], "is_supported", False) else "有/否"
        else:
            vc_s = "异常"
    elif lt:
        vc_s = "关"

    short_op = opinions[-1] if opinions else None
    if short_op is None:
        short_txt = "-"
    elif getattr(short_op, "is_supported", False):
        short_txt = "%s %.2f" % (_direction_cn(getattr(short_op, "direction", 0)), float(getattr(short_op, "confidence", 0) or 0))
    else:
        short_txt = _short_unsupported_reason(
            router_domain,
            getattr(short_op, "reasoning_summary", None) or "",
        )

    raw_rs = (getattr(short_op, "reasoning_summary", None) or "").strip() if short_op else ""
    if stub:
        prefix = "【占位】"
        rs_in = prefix + (raw_rs or "联调占位")
    else:
        rs_in = raw_rs or "-"

    pr_str = "%.0f" % primary_rev_wan if primary_rev_wan else "-"
    sr_str = "%.0f" % secondary_rev_wan if secondary_rev_wan else "-"
    vs_str = vert_share_str[:18] if vert_share_str else "-"
    return [
        sym[:12],
        tier,
        bucket[:8] if bucket else "—",
        vert_str,
        vs_str,
        pr_str,
        sr_str,
        rd_disp,
        "%.2f" % tech,
        "是" if lt else "否",
        vc_s,
        _truncate_display(short_txt, 16),
        rs_in,
    ]


def _moe_detail_layout(reason_w: int) -> Tuple[List[int], List[str], List[str]]:
    """表头与列宽：大类/垂直分列，垂直占比、主营/次营营收，路由=垂直细分。"""
    widths = [12, 6, 8, 22, 18, 10, 10, 8, 7, 4, 6, 16, reason_w]
    aligns = ["left", "center", "center", "left", "left", "right", "right", "center", "right", "center", "center", "left", "left"]
    headers = ["标的", "B档", "大类", "垂直", "垂直占比%", "主营(万)", "次营(万)", "路由", "技分", "长候", "长轨", "短轨", "摘要"]
    return widths, aligns, headers


def main() -> int:
    from diting.classifier.business_segment_provider import (
        get_latest_revenue_batch,
        get_segment_labels_and_shares_batch,
    )
    from diting.classifier.snapshot_reader import (
        domain_bucket_and_vertical_from_tags_json,
        domain_tags_zh_from_tags_json,
        fetch_latest_classifier_batch_id,
        fetch_snapshot_rows_batch,
        resolve_moe_classifier_batch_id,
        segment_list_from_segment_shares_json,
    )
    from diting.moe.opinion_writer import write_moe_expert_opinion_snapshot
    from diting.moe.router import _load_moe_config, resolve_router_domain_tag, route_and_collect_opinions
    from diting.protocols.brain_pb2 import TIME_HORIZON_LONG_TERM
    from diting.scanner.quant_snapshot_reader import (
        fetch_latest_quant_batch_id,
        fetch_quant_signal_scan_all_map,
        resolve_moe_quant_batch_id,
    )
    from diting.scanner.quant import QuantScanner
    from diting.universe import parse_symbol_list_from_env

    pg_l2 = (os.environ.get("PG_L2_DSN") or "").strip()
    track = (os.environ.get("DITING_TRACK") or "a").strip().lower()
    universe: List[str] = []
    if track == "b" and pg_l2:
        try:
            import psycopg2
            conn = psycopg2.connect(pg_l2)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT symbol FROM b_track_candidate_snapshot
                WHERE batch_id = (SELECT batch_id FROM b_track_candidate_snapshot ORDER BY created_at DESC LIMIT 1)
                ORDER BY symbol
                """
            )
            rows = cur.fetchall()
            cur.close()
            conn.close()
            universe = [str(r[0] or "").strip() for r in rows if r and (r[0] or "").strip()]
            if universe:
                print("DITING_TRACK=b: 从 b_track_candidate_snapshot 取 %d 只标的" % len(universe))
        except Exception as e:
            print("[提示] DITING_TRACK=b 但 b_track_candidate_snapshot 读取失败: %s；回退 A 轨" % e, file=sys.stderr)
    if not universe:
        universe = parse_symbol_list_from_env("DITING_SYMBOLS") or parse_symbol_list_from_env("MODULE_AB_SYMBOLS")
    if not universe:
        from diting.classifier.run import _default_universe_from_diting_symbols

        universe = _default_universe_from_diting_symbols()
    if not universe:
        print("错误: 未获取到标的列表", file=sys.stderr)
        return 1
    env_pipeline = (os.environ.get("MOE_PIPELINE") or "").strip()
    if env_pipeline:
        pipeline = env_pipeline.lower()
    else:
        # 已连 L2 且未显式指定 MOE_PIPELINE：默认从 L2 读 B，不重扫，与 B 批次对齐；无 L2 则只能本机全量扫描
        pipeline = "snapshot" if pg_l2 else "full"
    use_l2_only = pipeline in ("snapshot", "l2", "l2_only", "from_l2")

    seg_src = (os.environ.get("MOE_SEGMENT_SOURCE") or "classifier").strip().lower()
    use_a_snapshot = use_l2_only or seg_src in ("snapshot", "l2", "l2_snapshot")
    batch_id = str(uuid.uuid4())
    snapshot_batch_hint: Optional[str] = None
    quant_batch_hint: Optional[str] = None
    n_a_rows = 0
    n_b_rows = 0

    clf_results: List[Any] = []
    sym_to_quant: Dict[str, Any] = {}
    a_rows: Dict[str, Any] = {}
    _mcfg = _load_moe_config()
    _mr = _mcfg.get("moe_router") or _mcfg
    tag_to_router = (_mr.get("tag_to_router_domain") or {}) if isinstance(_mr, dict) else {}

    class _Snap:
        __slots__ = (
            "symbol", "domain_tags", "segment_list", "bucket", "vertical",
            "vert_share_str", "primary_rev_wan", "secondary_rev_wan",
        )

        def __init__(
            self,
            symbol: str,
            domain_tags: List[str],
            segment_list: List[Dict[str, Any]],
            bucket: str = "未知",
            vertical: Optional[List[str]] = None,
            vert_share_str: str = "",
            primary_rev_wan: float = 0.0,
            secondary_rev_wan: float = 0.0,
        ):
            self.symbol = symbol
            self.domain_tags = domain_tags
            self.segment_list = segment_list
            self.bucket = bucket or "未知"
            self.vertical = vertical or []
            self.vert_share_str = vert_share_str or ""
            self.primary_rev_wan = primary_rev_wan or 0.0
            self.secondary_rev_wan = secondary_rev_wan or 0.0

    if use_l2_only:
        if not pg_l2:
            print("错误: MOE_PIPELINE=snapshot 需要 PG_L2_DSN（读 A/B L2 快照）", file=sys.stderr)
            return 1
        snapshot_batch_hint = resolve_moe_classifier_batch_id(None)
        if not snapshot_batch_hint:
            snapshot_batch_hint = fetch_latest_classifier_batch_id(pg_l2)
        quant_batch_hint = resolve_moe_quant_batch_id(None)
        if not quant_batch_hint:
            quant_batch_hint = fetch_latest_quant_batch_id(pg_l2)
        a_rows = fetch_snapshot_rows_batch(pg_l2, universe, batch_id=snapshot_batch_hint)
        n_a_rows = len(a_rows)
        sym_to_quant = fetch_quant_signal_scan_all_map(pg_l2, universe, batch_id=quant_batch_hint)
        n_b_rows = len(sym_to_quant)
        if qb_ids := [v.get("quant_batch_id") for v in sym_to_quant.values() if v.get("quant_batch_id")]:
            batch_id = (qb_ids[0] or batch_id)
        elif a_rows:
            any_b = next(iter(a_rows.values())).get("batch_id") or ""
            if any_b:
                batch_id = any_b
        for sym in universe:
            sk = (sym or "").strip().upper()
            r = a_rows.get(sk)
            if r:
                dt = domain_tags_zh_from_tags_json(r.get("tags_json"))
                sl = segment_list_from_segment_shares_json(r.get("segment_shares_json"))
                bucket, vert = domain_bucket_and_vertical_from_tags_json(r.get("tags_json"), tag_to_router)
            else:
                dt = ["未知"]
                sl = []
                bucket, vert = "未知", []
            clf_results.append(_Snap(sym, dt, sl, bucket, vert))
    else:
        if use_a_snapshot:
            if not pg_l2:
                print("错误: MOE_SEGMENT_SOURCE=snapshot 需要 PG_L2_DSN", file=sys.stderr)
                return 1
            snapshot_batch_hint = resolve_moe_classifier_batch_id(None)
            if not snapshot_batch_hint:
                snapshot_batch_hint = fetch_latest_classifier_batch_id(pg_l2)
            rows = fetch_snapshot_rows_batch(pg_l2, universe, batch_id=snapshot_batch_hint)
            a_rows = rows
            n_a_rows = len(rows)
            if rows:
                any_b = next(iter(rows.values())).get("batch_id") or ""
                if any_b:
                    batch_id = any_b
            for sym in universe:
                sk = (sym or "").strip().upper()
                r = rows.get(sk)
                if r:
                    dt = domain_tags_zh_from_tags_json(r.get("tags_json"))
                    sl = segment_list_from_segment_shares_json(r.get("segment_shares_json"))
                    bucket, vert = domain_bucket_and_vertical_from_tags_json(r.get("tags_json"), tag_to_router)
                else:
                    dt = ["未知"]
                    sl = []
                    bucket, vert = "未知", []
                clf_results.append(_Snap(sym, dt, sl, bucket, vert))
        else:
            from diting.classifier import SemanticClassifier
            from diting.classifier.semantic import load_rules

            industry_provider = None
            business_segment_provider = None
            segment_top1_name_provider = None
            segment_disclosure_names_provider = None
            if pg_l2 and universe:
                try:
                    from diting.classifier.business_segment_provider import (
                        get_segment_disclosure_names_batch,
                        get_top_segment_disclosure_batch,
                        make_business_segment_provider,
                    )
                    from diting.classifier.l2_provider import get_l2_industry_revenue_batch
                    from diting.ingestion.industry_revenue import (
                        _load_industry_fallback,
                        industry_name_needs_fallback,
                    )

                    l2_data = get_l2_industry_revenue_batch(pg_l2, universe)
                    missing = ("未知", 0.0, 0.0, 0.0)
                    merged = {}
                    for s in universe:
                        key = (s or "").strip().upper()
                        t = l2_data.get(key, ("", 0.0, 0.0, 0.0))
                        if not industry_name_needs_fallback(t[0]):
                            merged[key] = t
                        else:
                            iname = _load_industry_fallback(s) or "未知"
                            merged[key] = (iname, float(t[1] or 0), float(t[2] or 0), float(t[3] or 0))
                    industry_provider = lambda sym, m=merged, mis=missing: m.get((sym or "").strip().upper(), mis)
                    business_segment_provider = make_business_segment_provider(pg_l2, universe)
                    _disc = get_top_segment_disclosure_batch(pg_l2, universe)
                    _names_by_sym = get_segment_disclosure_names_batch(pg_l2, universe)

                    def _top1_name(sym: str):
                        row = _disc.get((sym or "").strip().upper())
                        if not row:
                            return None
                        n = (row[0] or "").strip()
                        return n or None

                    def _segment_disclosure_names(sym: str):
                        return _names_by_sym.get((sym or "").strip().upper(), [])

                    segment_top1_name_provider = _top1_name
                    segment_disclosure_names_provider = _segment_disclosure_names
                except Exception:
                    pass

            clf_kw = dict(
                rules=load_rules(),
                industry_revenue_provider=industry_provider,
                business_segment_provider=business_segment_provider,
            )
            if segment_top1_name_provider is not None:
                clf_kw["segment_top1_name_provider"] = segment_top1_name_provider
            if segment_disclosure_names_provider is not None:
                clf_kw["segment_disclosure_names_provider"] = segment_disclosure_names_provider
            clf = SemanticClassifier(**clf_kw)
            batch_id = str(uuid.uuid4())
            clf_results = clf.classify_batch(universe, correlation_id=batch_id)

        if not use_l2_only:
            ohlcv_dsn = (os.environ.get("TIMESCALE_DSN") or "").strip() or None
            scanner = QuantScanner()
            scan_out = scanner.scan_market(universe, ohlcv_dsn=ohlcv_dsn, correlation_id=batch_id, return_all=True)
            sym_to_quant = {str(x.get("symbol", "")).strip().upper(): x for x in scan_out}
            n_b_rows = len(sym_to_quant)

    seg_extra: Dict[str, Tuple[str, float, float]] = {}
    if pg_l2 and universe:
        try:
            seg_labels = get_segment_labels_and_shares_batch(pg_l2, list(universe), 3)
            rev_wan = get_latest_revenue_batch(pg_l2, list(universe))
            for sk in set((s or "").strip().upper() for s in universe if (s or "").strip()):
                rows_s = seg_labels.get(sk, [])
                rv = rev_wan.get(sk, 0.0)
                vs = ",".join("%s%d%%" % ((l or "其他").strip() or "其他", round(sh * 100)) for l, sh in rows_s[:3]) if rows_s else ""
                pr = rv * rows_s[0][1] if rows_s else 0.0
                sr = rv * rows_s[1][1] if len(rows_s) > 1 else 0.0
                seg_extra[sk] = (vs, pr, sr)
        except Exception:
            pass

    # 生产默认关占位（走真实细分信号；无信号时认知边界→不支持）。本地联调显式 MOE_STUB_SEGMENT_SIGNALS=1
    stub = os.environ.get("MOE_STUB_SEGMENT_SIGNALS", "0").strip().lower() in ("1", "true", "yes")
    enable_vc = os.environ.get("MOE_ENABLE_VC_AGENT", "1").strip().lower() not in ("0", "false", "no")
    _mcfg = _load_moe_config()
    _mr = _mcfg.get("moe_router") or _mcfg
    require_qp = bool(_mr.get("require_quant_passed"))
    ab_warnings = _ab_alignment_warnings(
        use_l2_only or use_a_snapshot,
        n_a_rows,
        n_b_rows,
        a_rows,
        sym_to_quant,
        universe,
    )
    reason_floor, reason_cap = _moe_reason_floor_and_cap()

    scope_mode = _parse_moe_c_scope()
    if track == "b":
        scope_mode = "all"
    n_universe = len(clf_results)
    n_in_scope = 0
    for out in clf_results:
        sym_u = str(out.symbol or "").strip().upper()
        q0 = sym_to_quant.get(sym_u) or {}
        if _quant_in_moe_scope(q0, scope_mode):
            n_in_scope += 1

    # [Ref: 05_C模块_输出检测与问题根治最佳实践] stub=0 时探测 segment_signal_cache 是否为空，非阻断性提示
    if not stub and pg_l2 and n_in_scope > 0:
        all_seg_ids = []
        for out in clf_results:
            sym_u = str(out.symbol or "").strip().upper()
            if not _quant_in_moe_scope(sym_to_quant.get(sym_u) or {}, scope_mode):
                continue
            sl = getattr(out, "segment_list", None) or []
            for s in sl:
                sid = str(s.get("segment_id") or "").strip()
                if sid:
                    all_seg_ids.append(sid)
        all_seg_ids = list(dict.fromkeys(all_seg_ids))
        if all_seg_ids:
            try:
                from diting.moe.segment_signal_reader import fetch_segment_signals_for_segments
                probe = fetch_segment_signals_for_segments(pg_l2, all_seg_ids[:20])
                if not probe:
                    ab_warnings.append(
                        "segment_signal_cache 无本批 segment 数据；建议先 make refresh-segment-signals。无数据时 C 将输出「主营细分无信号」。"
                    )
            except Exception:
                pass

    packed: List[Tuple[Dict[str, Any], str, Any, Optional[str], List[str]]] = []

    ind_by_sym: Dict[str, str] = {}
    if track == "a" and pg_l2:
        try:
            from diting.classifier.l2_provider import get_l2_industry_revenue_batch

            _syms_all = [str(x.symbol or "").strip().upper() for x in clf_results]
            ind_by_sym = {
                k: (v[0] or "").strip()
                for k, v in get_l2_industry_revenue_batch(pg_l2, _syms_all).items()
            }
        except Exception:
            pass

    for out in clf_results:
        sym = str(out.symbol or "").strip().upper()
        quant_signal = sym_to_quant.get(sym) or {}
        if not _quant_in_moe_scope(quant_signal, scope_mode):
            continue
        if use_a_snapshot or use_l2_only:
            domain_tags = getattr(out, "domain_tags", []) or ["未知"]
            segment_list = getattr(out, "segment_list", []) or []
            bucket = getattr(out, "bucket", None) or "未知"
            vertical = getattr(out, "vertical", None) or []
        else:
            domain_tags = _domain_tags_zh(out)
            segment_list = _segment_list_from_classifier(out)
            router_domain_tmp = resolve_router_domain_tag(domain_tags, None)
            bucket = router_domain_tmp or "未知"
            vertical = [t for t in domain_tags if t not in ("农业", "科技", "宏观", "未知")][:3]

        router_display = vertical[0] if vertical else (bucket or "—")

        segment_signals: Dict[str, Any] = {}
        if stub:
            segment_signals = _stub_segment_signals(segment_list)
        elif pg_l2 and segment_list:
            seg_ids = [str(s.get("segment_id") or "").strip() for s in segment_list if s.get("segment_id")]
            if seg_ids:
                from diting.moe.segment_signal_reader import fetch_segment_signals_for_segments
                segment_signals = fetch_segment_signals_for_segments(pg_l2, seg_ids)

        if not stub and track == "a" and pg_l2:
            try:
                from diting.moe.a_track_signal_reader import (
                    fetch_a_track_signals_for_symbol,
                    merge_a_track_into_segment_signals,
                )

                _at = fetch_a_track_signals_for_symbol(pg_l2, sym, ind_by_sym.get(sym, ""))
                segment_list, segment_signals = merge_a_track_into_segment_signals(
                    track,
                    segment_list,
                    segment_signals,
                    _at.get("symbol"),
                    _at.get("industry"),
                )
            except Exception:
                pass

        router_domain = resolve_router_domain_tag(domain_tags, None)

        opinions = route_and_collect_opinions(
            sym,
            quant_signal=quant_signal,
            domain_tags=domain_tags,
            segment_list=segment_list,
            segment_signals=segment_signals,
            enable_vc_agent=enable_vc,
            track=track,
        )
        vs, pr, sr = seg_extra.get(sym, ("", 0.0, 0.0))
        packed.append(
            (
                quant_signal,
                sym,
                opinions,
                router_domain,
                _moe_detail_cells(
                    sym, bucket, vertical, router_display,
                    quant_signal, opinions, enable_vc, stub,
                    router_domain=router_domain,
                    vert_share_str=vs,
                    primary_rev_wan=pr,
                    secondary_rev_wan=sr,
                ),
            )
        )

    packed.sort(key=lambda t: _moe_detail_sort_key(t[0], t[1]))

    all_rows = [(t[1], t[2]) for t in packed]
    detail_rows = [t[4] for t in packed]
    n_sym = len(packed)

    n_lt_cand = 0
    n_vc_emitted = 0
    n_short_ok = 0
    n_short_unsupported = 0
    domain_ok_counter: Counter = Counter()

    for quant_signal, _sym, opinions, router_domain, _cells in packed:
        if quant_signal.get("long_term_candidate"):
            n_lt_cand += 1
        if any(int(getattr(o, "horizon", 0) or 0) == TIME_HORIZON_LONG_TERM for o in opinions):
            n_vc_emitted += 1
        last = opinions[-1] if opinions else None
        if last is not None:
            if getattr(last, "is_supported", False):
                n_short_ok += 1
                if router_domain:
                    domain_ok_counter[router_domain] += 1
            else:
                n_short_unsupported += 1

    n_b_conf, n_b_alert_only, n_b_snap = _scan_b_tier_counts(sym_to_quant)

    a_src = "L2" if use_a_snapshot else "内存分类"
    b_src = "L2" if use_l2_only else "本机 QuantScanner"
    if _pipeline_quiet():
        from diting.pipeline_io import pipeline_frame_quiet

        pipeline_frame_quiet()
    print()
    print("────────────────────────────────────────────────────────")
    print("  Module C（MoE）")
    print(
        "    本批 %s 只    universe %s    scope=%s    pipeline=%s    track=%s"
        % (n_sym, n_universe, scope_mode, pipeline, track)
    )
    print("    数据源  A:%s    B:%s" % (a_src, b_src))
    if scope_mode == "all":
        print("    [警告] MOE_C_SCOPE=all 仅宜调试")
    elif scope_mode == "passed":
        print("    [提示] passed 不含仅预警；与 B 快照不一致时请改 snapshot")
    print()
    print("    批次 id（未设 MOE_*_BATCH_ID 时用 L2 最近一次整批）")
    print("      A  %s" % (snapshot_batch_hint or "（无）"))
    print("      B  %s" % (quant_batch_hint or "（无）"))
    print("      本 run  %s" % (batch_id if batch_id else "（无）"))
    if env_pipeline:
        print("    说明  MOE_PIPELINE 由环境指定")
    elif pg_l2:
        print("    说明  未设 MOE_PIPELINE 且已配 PG_L2_DSN → 默认 snapshot；无 L2 则 full")
    if not use_l2_only and pg_l2:
        print("    说明  当前 B 为本机重算；与库内一致请 snapshot")
    print()
    print("    B 档（全截面）    确认 %s    仅预警 %s    快照口径 %s" % (n_b_conf, n_b_alert_only, n_b_snap))
    if scope_mode == "snapshot":
        print("    本批应对齐        入 C = 快照 %s 只" % n_b_snap)
    else:
        print("    本批应对齐        符合 scope 的标的 %s 只" % n_in_scope)
    if scope_mode == "snapshot" and n_in_scope == 0:
        print("    [警告] 无 snapshot 标的 → 先 make run-module-b 或 MOE_C_SCOPE=all")
    print()
    print(
        "    MoE 产出    长候 %s    长轨 %s    短轨 OK %s    不予支持 %s"
        % (n_lt_cand, n_vc_emitted, n_short_ok, n_short_unsupported)
    )
    if domain_ok_counter:
        print(
            "    路由落域    %s"
            % "    ".join("%s %s" % (k, domain_ok_counter[k]) for k in sorted(domain_ok_counter.keys()))
        )
    print()
    print(
        "    开关    stub=%s    VC=%s    量化门控=%s"
        % (stub, enable_vc, "开" if require_qp else "关")
    )
    rc_note = "摘要不截断（按本批最长撑列）" if reason_cap == 0 else "摘要硬上限 %s（超长省略）" % reason_cap
    print("    摘要列    最小列宽 %s    %s" % (reason_floor, rc_note))
    if use_l2_only:
        print("    L2 行数    classifier %s    quant_scan_all %s" % (n_a_rows, n_b_rows))
    elif use_a_snapshot:
        print("    L2 / B    classifier %s    实时扫描 %s" % (n_a_rows, n_b_rows))
    else:
        ohlcv_dsn = (os.environ.get("TIMESCALE_DSN") or "").strip() or None
        print("    TIMESCALE_DSN    %s" % ("已配置" if ohlcv_dsn else "未配置（逐标慢）"))
    if stub:
        print()
        print("    ╔═══════════════════════════════════════════════════════════════════════╗")
        print("    ║ [警告] stub=开：细分信号为占位数据，非真实 segment_signal_cache；     ║")
        print("    ║        仅供本地联调，不得作为生产右脑依据。生产请 stub=0。           ║")
        print("    ╚═══════════════════════════════════════════════════════════════════════╝")
        print()
    else:
        print(
            "    [提示] stub=关：使用真实 segment_signal_cache；无数据时输出「上游无数据」"
        )
    for aw in ab_warnings:
        if aw.startswith("[设计口径]"):
            print("    %s" % aw)
        else:
            print("    [警告] %s" % aw)
    print("────────────────────────────────────────────────────────")

    if _pipeline_quiet():
        print()
        print("  ┌─ 模块 C 准出（设计对照 · 可判断是否满足 Stage2 1:1:1）────────────────")
        print("  │ ① 数据路径: pipeline=%s | A/B 来源=%s/%s" % (pipeline, a_src, b_src))
        print(
            "  │ ② 策略门控: scope=%s | stub=%s | VC=%s | 量化门控=%s"
            % (scope_mode, stub, enable_vc, "开" if require_qp else "关")
        )
        print(
            "  │ ③ L2 行数: classifier=%s | quant_scan_all=%s（与 B 终端「全量条数」同源）"
            % (n_a_rows, n_b_rows)
        )
        print(
            "  │ ④ B 档结构: 确认=%s | 仅预警=%s | 快照合计=%s（MoE snapshot 应对齐③④）"
            % (n_b_conf, n_b_alert_only, n_b_snap)
        )
        print(
            "  │ ⑤ 本批 MoE: 处理明细=%s 条 | universe=%s | 短轨 OK=%s / 不予支持=%s"
            % (n_sym, n_universe, n_short_ok, n_short_unsupported)
        )
        if scope_mode == "snapshot":
            mismatch = n_b_snap != n_sym and n_sym > 0
            print(
                "  │ ⑥ snapshot 准出: B 快照合计 %s vs MoE 条数 %s → %s %s"
                % (
                    n_b_snap,
                    n_sym,
                    "通过" if not mismatch else "不通过",
                    "" if not mismatch else "（查 MOE_QUANT_BATCH_ID / 门控）",
                )
            )
        n_warn = sum(1 for w in ab_warnings if not w.startswith("[设计口径]"))
        n_note = len(ab_warnings) - n_warn
        print("  │ ⑦ 对齐说明: 设计口径条=%s | 真告警条=%s（见上文）" % (n_note, n_warn))
        print("  └──────────────────────────────────────────────────────────────")

    env_pa = (os.environ.get("MOE_C_PRINT_ALL") or "").strip().lower()
    if env_pa in ("0", "false", "no"):
        print_all = False
        try:
            print_max = max(1, int((os.environ.get("MOE_C_PRINT_MAX") or "20").strip()))
        except ValueError:
            print_max = 20
    else:
        print_all = True
        print_max = 20
    to_show = detail_rows if print_all else detail_rows[:print_max]
    reason_w = _moe_reason_col_width(to_show, reason_floor, reason_cap)
    display_rows = _rows_with_reason_cap(to_show, reason_cap)
    detail_title = (
        "共 %s 条，确认档→预警档、技分降序" % n_sym
        if print_all
        else "前 %s / 共 %s 条（确认→预警，技分↓）；全量勿设 MOE_C_PRINT_ALL=0" % (len(to_show), n_sym)
    )
    print()
    print("======== 明细（%s）========" % detail_title)
    if n_sym == 0:
        print("  （无明细：当前门控下无标的）")
    else:
        mw, ma, mh = _moe_detail_layout(reason_w)
        print(_moe_table_line(mh, mw, ma))
        sep_w = sum(mw) + (len(mw) - 1) * 1
        print("  " + "-" * min(sep_w, 260))
        for row in display_rows:
            print(_moe_table_line(row, mw, ma))
        if not print_all and n_sym > len(to_show):
            print("  ... 其余 %s 条略（默认打全量；限制行数设 MOE_C_PRINT_ALL=0）" % (n_sym - len(to_show)))
    print()

    n_written = 0
    write_id = ""
    if pg_l2 and all_rows:
        write_id = (os.environ.get("MOE_OUTPUT_BATCH_ID") or "").strip() or batch_id
        run_meta = _moe_run_metadata_dict(
            stub=stub,
            enable_vc=enable_vc,
            require_quant_passed=require_qp,
            pipeline=pipeline,
            scope_mode=scope_mode,
            seg_src=seg_src,
            snapshot_batch_hint=snapshot_batch_hint,
            quant_batch_hint=quant_batch_hint,
            write_batch_id=write_id,
            n_a_rows=n_a_rows,
            n_b_rows=n_b_rows,
            n_sym=n_sym,
            n_universe=n_universe,
            ab_warnings=ab_warnings,
            track=track,
        )
        n_written = write_moe_expert_opinion_snapshot(
            pg_l2,
            all_rows,
            batch_id=write_id,
            correlation_id=write_id,
            run_metadata=run_meta,
        )
        print("======== 写入 L2（moe_expert_opinion_snapshot）========  ")
        if n_written > 0:
            print("  写入行数=%s  batch_id=%s" % (n_written, write_id))
        else:
            print("  未写入（请先 make init-l2-moe-opinion-table）")
    else:
        print("======== 写入 L2 ========  ")
        print("  跳过（未配置 PG_L2_DSN 或无可写行）")
    print()
    expect_ok = bool(pg_l2 and (n_sym == 0 or (n_written > 0 and n_written == n_sym)))
    print("======== 准出 ======")
    exit_note = "符合预期" if expect_ok else "异常（查 PG_L2_DSN、表、MOE_C_SCOPE、L2 返回）"
    if expect_ok and stub:
        exit_note = "符合预期（stub 联调；生产请关 stub）"
    print("  处理=%s 只 | 短轨 OK=%s | L2 写入=%s | %s" % (n_sym, n_short_ok, n_written, exit_note))
    if _pipeline_quiet():
        print("  ── 全链路可追溯 ──")
        print("    · L2 汇总 A/B/信号层/C 批次与对齐告警: make query-full-pipeline-result")
        if write_id:
            print("    · 本批写入 batch_id=%s（表 moe_expert_opinion_snapshot）" % write_id)
        if quant_batch_hint:
            print("    · 对齐的 B quant_batch_id=%s" % quant_batch_hint)
        if snapshot_batch_hint:
            print("    · 对齐的 A classifier_batch_id=%s" % snapshot_batch_hint)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
