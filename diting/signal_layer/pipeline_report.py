# [Ref: 06_B轨_信号层生产级数据采集_实践] 信号层 refresh 终端工作结果表（L2 只读聚合）
from __future__ import annotations

import json
import os
import textwrap
from collections import Counter
from typing import Any, Dict, List, Set, Tuple

from diting.ingestion.segment_tier import tier_int_to_label_cn
from diting.signal_layer.models import RefreshSegmentSignalsResult

# 默认 88：常见 80～100 列终端下不易折行；可用 PIPELINE_SEGMENT_REPORT_WIDTH 覆盖
_DEFAULT_CONTENT_W = 88


def _report_width() -> int:
    try:
        w = int((os.environ.get("PIPELINE_SEGMENT_REPORT_WIDTH") or "").strip() or _DEFAULT_CONTENT_W)
    except ValueError:
        w = _DEFAULT_CONTENT_W
    return max(64, min(132, w))


def _truncate(s: str, n: int) -> str:
    s = (s or "").replace("\n", " ").strip()
    if len(s) <= n:
        return s
    return s[: n - 1] + "…"


def _ascii_table(headers: List[str], rows: List[List[str]], *, max_cell: int = 48) -> List[str]:
    """等宽 ASCII 表格（中文按字符数近似列宽，便于终端复制）。"""
    n = len(headers)
    if not rows:
        return []
    widths = [len(h) for h in headers]
    for row in rows:
        for i in range(n):
            c = str(row[i] if i < len(row) else "")
            widths[i] = max(widths[i], min(max_cell, len(c)))
    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    out: List[str] = []
    head = (
        "|"
        + "|".join(" %s " % _truncate(h, widths[i]).ljust(widths[i]) for i, h in enumerate(headers))
        + "|"
    )
    out.append(sep)
    out.append(head)
    out.append(sep)
    for row in rows:
        cells = []
        for i in range(n):
            c = str(row[i] if i < len(row) else "")
            c = _truncate(c, widths[i])
            cells.append(" %s " % c.ljust(widths[i]))
        out.append("|" + "|".join(cells) + "|")
    out.append(sep)
    return out


def _a_track_signal_conclusion_cell(b: Dict[str, Any]) -> str:
    """无 LLM 有效信号时统一「无信号」；有 signal_source=llm 时展示方向/强度等（截断）。"""
    if b.get("llm_tag_yes") and (b.get("tag_fields_line") or "").strip():
        return _truncate(b.get("tag_fields_line") or "", 56)
    return "无信号"


def _symbol_name_short(sym: str, name_by_sym: Dict[str, str]) -> str:
    n = (name_by_sym.get(sym) or "").strip()
    return n if n else "—"


def _map_direction(d: str) -> str:
    d = (d or "").strip().lower()
    return {
        "bullish": "利多",
        "bearish": "利空",
        "neutral": "中性",
    }.get(d, d or "—")


def _map_type(t: str) -> str:
    t = (t or "").strip().lower()
    return {
        "policy": "政策",
        "price": "价格",
        "order": "订单",
        "rnd": "研发",
    }.get(t, t or "—")


def _segment_status(seg_id: str, result: RefreshSegmentSignalsResult) -> str:
    if seg_id in result.segments_written:
        return "本批写入"
    if seg_id in result.segments_skipped_ttl:
        return "TTL命中"
    if seg_id in result.segments_failed:
        return "失败:" + _truncate(str(result.segments_failed.get(seg_id, "")), 28)
    if seg_id in (result.segments_without_adapter or []):
        return "无适配器"
    return "—"


def _parse_summary(raw: Any) -> Tuple[str, str, str, str, str, str, str]:
    """signal_summary JSON → type, direction, risk, summary_cn(短), summary_full, strength_str, signal_source"""
    if raw is None:
        return "", "", "", "", "", "", ""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return "", "", "", _truncate(raw, 80), raw, "", ""
    if not isinstance(raw, dict):
        s = str(raw)
        return "", "", "", s[:80], s, "", ""
    t = str(raw.get("type") or "")
    d = str(raw.get("direction") or "")
    rt = raw.get("risk_tags") or []
    rs = ",".join(str(x) for x in rt) if isinstance(rt, list) else str(rt)
    sm = str(raw.get("summary_cn") or "")
    st = raw.get("strength")
    ss = ""
    if st is not None and isinstance(st, (int, float)):
        ss = "%.2f" % float(st)
    src = str(raw.get("signal_source") or "").strip()
    return t, d, rs, _truncate(sm, 72), sm, ss, src


def print_segment_refresh_work_table(
    dsn: str,
    symbols: List[str],
    result: RefreshSegmentSignalsResult,
    *,
    days_back: int = 7,
    revenue_floor: float = 0.30,
    max_rows: int = 80,
) -> None:
    """
    打印信号层主要工作结果：分块展示（大分类/细分/营收/新闻/缓存/信号/摘要），便于终端扫描。
    """
    try:
        import psycopg2
    except ImportError:
        print("  [工作结果表] 未安装 psycopg2，跳过")
        return

    syms = sorted({str(s).strip().upper() for s in symbols if (s or "").strip()})
    if not syms:
        return

    days_back = max(1, min(90, int(days_back)))
    rf = max(0.0, min(1.0, float(revenue_floor)))

    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol, COUNT(*)::bigint
            FROM news_content
            WHERE symbol = ANY(%s)
              AND published_at >= NOW() - INTERVAL '1 day' * %s
            GROUP BY symbol
            """,
            (syms, days_back),
        )
        news_by_sym = {str(r[0] or "").strip().upper(): int(r[1] or 0) for r in cur.fetchall()}

        cur.execute(
            """
            SELECT p.symbol, p.segment_id, p.segment_label_cn, p.revenue_share, p.is_primary,
                   COALESCE(r.domain, ''), COALESCE(r.name_cn, ''),
                   COALESCE(r.sub_domain, ''), r.segment_tier,
                   COALESCE(i.industry_name, '')
            FROM symbol_business_profile p
            LEFT JOIN segment_registry r ON r.segment_id = p.segment_id
            LEFT JOIN industry_revenue_summary i ON i.symbol = p.symbol
            WHERE p.symbol = ANY(%s)
            ORDER BY p.symbol, p.revenue_share DESC NULLS LAST
            """,
            (syms,),
        )
        prof_rows = cur.fetchall()

        seg_ids: Set[str] = set()
        for row in prof_rows or []:
            seg_id = row[1]
            if seg_id:
                seg_ids.add(str(seg_id).strip())

        cache_map: Dict[str, Tuple[Any, Any]] = {}
        if seg_ids:
            cur.execute(
                """
                SELECT segment_id, signal_summary, fetched_at
                FROM segment_signal_cache
                WHERE segment_id = ANY(%s)
                """,
                (list(seg_ids),),
            )
            for sid, summ, fat in cur.fetchall() or []:
                cache_map[str(sid or "").strip()] = (summ, fat)

        cur.close()
        conn.close()
    except Exception as e:
        print("  [工作结果表] L2 查询失败: %s" % e)
        return

    by_sym: Dict[str, List[Tuple]] = {}
    for row in prof_rows or []:
        sym = str(row[0] or "").strip().upper()
        by_sym.setdefault(sym, []).append(row)

    rows_out: List[Dict[str, Any]] = []
    for sym in syms:
        rows = by_sym.get(sym) or []
        picked: List[Tuple] = []
        for r in rows:
            share = float(r[3] or 0)
            if share >= rf:
                picked.append(r)
        if not picked and rows:
            picked = [rows[0]]
        if not picked:
            rows_out.append(
                {
                    "symbol": sym,
                    "seg_id": "",
                    "domain": "—",
                    "seg_name": "—",
                    "label": "无主营行",
                    "share": "",
                    "news": news_by_sym.get(sym, 0),
                    "status": "—",
                    "stype": "",
                    "sdir": "",
                    "risk": "",
                    "ai_sum_full": "",
                    "strength": "",
                    "fetched": "",
                    "industry_sw": "—",
                    "sub_domain": "—",
                    "tier_label": "—",
                }
            )
            continue
        for r in picked:
            seg_id = str(r[1] or "").strip()
            label_cn = str(r[2] or "").strip()
            share = float(r[3] or 0)
            domain = str(r[5] or "").strip() or "—"
            name_cn = str(r[6] or "").strip() or seg_id
            sub_dom = str(r[7] or "").strip()
            try:
                tier_v = int(r[8]) if r[8] is not None else None
            except (TypeError, ValueError):
                tier_v = None
            industry_sw = str(r[9] or "").strip() or "—"
            summ, fat = cache_map.get(seg_id, (None, None))
            st, sd, rk, _sm_short, sm_full, ss, _src = _parse_summary(summ)
            rows_out.append(
                {
                    "symbol": sym,
                    "seg_id": seg_id,
                    "domain": domain,
                    "seg_name": name_cn,
                    "label": label_cn,
                    "share": "%.0f%%" % (share * 100) if share else "—",
                    "news": news_by_sym.get(sym, 0),
                    "status": _segment_status(seg_id, result),
                    "stype": st,
                    "sdir": sd,
                    "risk": rk,
                    "ai_sum_full": sm_full,
                    "strength": ss,
                    "fetched": str(fat)[:19] if fat else "",
                    "industry_sw": industry_sw,
                    "sub_domain": sub_dom if sub_dom else "—",
                    "tier_label": tier_int_to_label_cn(tier_v),
                }
            )

    total = len(rows_out)
    content_w = _report_width()
    bar_len = min(content_w + 4, 120)
    sym_total = Counter(str(o["symbol"]) for o in rows_out)
    sym_ord: Dict[str, int] = {}

    ttl_rows = sum(1 for o in rows_out if (o.get("status") or "") == "TTL命中")
    wrt_rows = sum(1 for o in rows_out if (o.get("status") or "") == "本批写入")
    fail_rows = sum(1 for o in rows_out if "失败" in str(o.get("status") or ""))
    other_rows = total - ttl_rows - wrt_rows - fail_rows

    print()
    print("  ══ 信号层 · 工作结果（共 %s 条分部行；主营≥%.0f%%；news_content 近 %d 天）══" % (total, rf * 100, days_back))
    print(
        "  状态分布（仅本表分部行）: TTL命中 %s 行 │ 本批写入 %s 行 │ 失败 %s 行 │ 其他 %s 行"
        % (ttl_rows, wrt_rows, fail_rows, other_rows)
    )
    print("  [提示] 上方「失败细分数」为全任务；本表「失败」行仅统计出现在本表中的分部。")
    print("  图例: 领域=segment_registry.domain（三分类）；赛道=sub_domain（申万行业，与领域正交）；层级=segment_tier→L1/L2/L3；")
    print("      细分注册名/披露=主营分部；新闻=该标的近 %d 天条数；缓存=本批是否重写 segment_signal_cache；信号=规则/AI JSON。" % days_back)
    print()

    n = 0
    for o in rows_out:
        if n >= max_rows:
            print("  … 余下 %s 行省略（PIPELINE_SEGMENT_TABLE_MAX=%s）" % (total - n, max_rows))
            break
        n += 1
        sym = o["symbol"]
        sym_ord[sym] = sym_ord.get(sym, 0) + 1
        sk = sym_ord[sym]
        stot = sym_total.get(sym, 1)
        bar = "  " + "─" * bar_len
        print(bar)
        ind = _truncate(str(o.get("industry_sw") or "—"), 14)
        tier_lb = o.get("tier_label") or "—"
        if stot > 1:
            print(
                "  [ %s / %s ]  %s  │  分部 %s/%s  │  申万: %-14s  │  领域: %-4s  │  层级: %-8s  │  营收: %-5s  │  近%dd新闻: %s  │  缓存: %s"
                % (
                    n,
                    total,
                    sym,
                    sk,
                    stot,
                    ind,
                    o.get("domain") or "—",
                    tier_lb,
                    o.get("share") or "—",
                    days_back,
                    o["news"],
                    o.get("status") or "—",
                )
            )
        else:
            print(
                "  [ %s / %s ]  %s  │  申万: %-14s  │  领域: %-4s  │  层级: %-8s  │  营收: %-5s  │  近%dd新闻: %s  │  缓存: %s"
                % (
                    n,
                    total,
                    sym,
                    ind,
                    o.get("domain") or "—",
                    tier_lb,
                    o.get("share") or "—",
                    days_back,
                    o["news"],
                    o.get("status") or "—",
                )
            )
        sid = o.get("seg_id") or ""
        if sid:
            print("  segment_id     %s" % sid)
        sn = (o.get("seg_name") or "").strip()
        lb = (o.get("label") or "").strip()
        if sn and lb and sn == lb:
            print("  细分/披露（同）  %s" % sn)
        else:
            print("  细分注册名       %s" % (sn or "—"))
            print("  披露标签         %s" % (lb or "—"))
        mt = _map_type(o.get("stype") or "")
        md = _map_direction(o.get("sdir") or "")
        rk = (o.get("risk") or "").strip() or "—"
        ss = (o.get("strength") or "").strip()
        sig_line = "  信号理解         类型=%s  方向=%s  强度=%s  风险标签=%s" % (
            mt,
            md,
            ss if ss else "—",
            rk,
        )
        print(sig_line)
        if o.get("fetched"):
            print("  缓存时间         %s" % o["fetched"])
        sm = (o.get("ai_sum_full") or "").strip()
        if sm:
            print("  摘要:")
            for ln in textwrap.wrap(sm, width=content_w, break_long_words=False, replace_whitespace=False):
                print("    %s" % ln)
        else:
            stt = o.get("status") or ""
            if "失败" in stt or "拉取无数据" in str(stt):
                print("  摘要: （无：上游未拉取到有效文本或理解失败）")
            else:
                print("  摘要: （无）")
        print()

    print("  " + "═" * bar_len)
    print("  说明: TTL命中=缓存未过期本批未重写，仍可读 segment_signal_cache；失败:拉取无数据=news_content 等原文不足。")
    print("  宽度: 摘要换行宽=%d（环境 PIPELINE_SEGMENT_REPORT_WIDTH，默认 %d）。" % (content_w, _DEFAULT_CONTENT_W))
    print()


def _a_track_signal_display_bundle(raw: Any) -> Dict[str, Any]:
    """
    解析 a_track_signal_cache.signal_summary。**仅当 signal_source=llm 时** 才视为「大模型打标/摘要」；
    无字段或旧规则缓存不得显示为「AI 已打标」。
    """
    empty: Dict[str, Any] = {
        "has_cache": False,
        "signal_origin_cn": "无（本标的 sym: 未写入 a_track_signal_cache）",
        "llm_tag_yes": False,
        "llm_summary_yes": False,
        "tag_fields_line": "",
        "risk_note_cn": "",
        "summary_full": "",
        "direction": "",
        "type": "",
        "strength_str": "",
        "signal_source": "",
    }
    if raw is None:
        return empty
    t, d, rs, _sm_short, sm_full, ss, src = _parse_summary(raw)
    src_l = (src or "").strip().lower()
    is_llm = src_l in ("llm", "ai") or "llm" in src_l
    is_fb = "fallback" in src_l
    is_rule = "rule" in src_l

    if is_llm:
        origin = "大模型（signal_source=llm）"
    elif is_fb:
        origin = "中性兜底（signal_source=fallback_neutral，非 LLM）"
    elif is_rule:
        origin = "旧规则关键词（已废弃，signal_source 含 rule）"
    elif (src or "").strip():
        origin = "其它（signal_source=%s）" % _truncate(src, 40)
    else:
        origin = "未标注（历史缓存：JSON 无 signal_source，非本次 LLM 写入）"

    sm = (sm_full or "").strip()
    llm_tag_yes = bool(is_llm)
    llm_summary_yes = bool(is_llm and sm)

    risk_s = (rs or "").strip()
    if risk_s:
        risk_disp = _truncate(risk_s, 48)
        risk_note = ""
    else:
        risk_disp = "—"
        if is_llm:
            risk_note = "（模型 JSON 中 risk_tags 为空）"
        elif is_fb or is_rule or not (src or "").strip():
            risk_note = "（非 LLM 或未写风险字段）"
        else:
            risk_note = "（本条无风险标签）"

    # 仅 LLM 写入展示方向/强度/类型；旧缓存或规则兜底若打印「利多」易与「大模型打标:否」矛盾
    tag_fields = (
        "方向 %s | 强度 %s | 类型 %s | 风险 %s%s"
        % (_map_direction(d), ss or "—", _map_type(t), risk_disp, risk_note)
        if is_llm
        else ""
    )

    return {
        "has_cache": True,
        "signal_origin_cn": origin,
        "llm_tag_yes": llm_tag_yes,
        "llm_summary_yes": llm_summary_yes,
        "tag_fields_line": tag_fields,
        "risk_note_cn": risk_note,
        "summary_full": sm,
        "direction": d,
        "type": t,
        "strength_str": ss,
        "signal_source": src,
    }


def print_a_track_symbol_news_summary(
    dsn: str,
    symbols: List[str],
    *,
    days_back: int = 7,
    max_rows: int = 120,
) -> None:
    """
    A 轨专用：主营披露（与 Module A 同口径：优先财报分部名+占比 TopN）、
    领域/层级/申万、近 7 天新闻条数、本批写入的 a_track_signal_cache（双路打标摘要）。
    """
    try:
        import psycopg2
    except ImportError:
        print("  [A 轨观测] 未安装 psycopg2，跳过")
        return

    syms = sorted({str(s).strip().upper() for s in symbols if (s or "").strip()})
    if not syms:
        return

    days_back = max(1, min(90, int(days_back)))
    max_rows = max(8, min(500, int(max_rows)))

    name_by_sym: Dict[str, str] = {}
    try:
        from diting.scanner.symbol_names import get_symbol_names

        name_by_sym = get_symbol_names(list(syms), dsn=dsn, skip_akshare=True)
    except Exception:
        name_by_sym = {}

    try:
        from diting.classifier.business_segment_provider import get_segment_labels_and_shares_batch

        labels_by_sym = get_segment_labels_and_shares_batch(dsn, syms, top_n=5)
    except Exception:
        labels_by_sym = {}

    try:
        import psycopg2

        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        # 兼容 scope=symbol：按 scope_id 或 symbol 聚合近 N 天条数
        cur.execute(
            """
            SELECT UPPER(TRIM(COALESCE(NULLIF(scope_id, ''), symbol))) AS sym_key,
                   COUNT(*)::bigint
            FROM news_content
            WHERE published_at >= NOW() - INTERVAL '1 day' * %s
              AND (
                (scope = 'symbol' AND UPPER(TRIM(scope_id)) = ANY(%s))
                OR (
                  UPPER(TRIM(symbol)) = ANY(%s)
                  AND (scope IS NULL OR scope = 'symbol')
                  AND symbol IS NOT NULL AND TRIM(symbol) <> ''
                )
              )
            GROUP BY sym_key
            """,
            (days_back, syms, syms),
        )
        news_by_sym = {str(r[0] or "").strip().upper(): int(r[1] or 0) for r in cur.fetchall()}

        news_by_industry: Dict[str, int] = {}
        try:
            cur.execute(
                """
                SELECT TRIM(scope_id) AS ind_key, COUNT(*)::bigint
                FROM news_content
                WHERE published_at >= NOW() - INTERVAL '1 day' * %s
                  AND scope = 'industry'
                  AND scope_id IS NOT NULL
                  AND TRIM(scope_id) <> ''
                GROUP BY TRIM(scope_id)
                """,
                (days_back,),
            )
            for row in cur.fetchall() or []:
                k = str(row[0] or "").strip()
                if k:
                    news_by_industry[k] = int(row[1] or 0)
        except Exception:
            news_by_industry = {}

        cur.execute(
            """
            SELECT DISTINCT ON (p.symbol)
                   p.symbol,
                   COALESCE(r.domain, ''),
                   COALESCE(r.sub_domain, ''),
                   r.segment_tier,
                   COALESCE(i.industry_name, '')
            FROM symbol_business_profile p
            LEFT JOIN segment_registry r ON r.segment_id = p.segment_id
            LEFT JOIN industry_revenue_summary i ON i.symbol = p.symbol
            WHERE p.symbol = ANY(%s)
            ORDER BY p.symbol, p.revenue_share DESC NULLS LAST
            """,
            (syms,),
        )
        meta: Dict[str, Tuple[str, str, Any, str]] = {}
        for sym, dom, subd, tier, ind in cur.fetchall() or []:
            k = str(sym or "").strip().upper()
            ti = tier
            try:
                if ti is not None:
                    ti = int(ti)
            except (TypeError, ValueError):
                ti = None
            meta[k] = (
                str(dom or "").strip(),
                str(subd or "").strip(),
                ti,
                str(ind or "").strip(),
            )

        sym_keys = ["sym:%s" % s for s in syms]
        cache_bundle: Dict[str, Dict[str, Any]] = {}
        try:
            cur.execute(
                """
                SELECT cache_key, signal_summary
                FROM a_track_signal_cache
                WHERE cache_key = ANY(%s)
                """,
                (sym_keys,),
            )
            for ck, summ in cur.fetchall() or []:
                key = str(ck or "").strip()
                if key.startswith("sym:"):
                    cache_bundle[key[4:].strip().upper()] = _a_track_signal_display_bundle(summ)
        except Exception:
            pass

        cur.close()
        conn.close()
    except Exception as e:
        print("  [A 轨观测] L2 查询失败: %s" % e)
        return

    n_print = min(len(syms), max_rows)
    w = _report_width()
    # 默认：汇总表 + 逐标的明细；仅表时可设 PIPELINE_A_TRACK_TABLE_ONLY=1
    table_only = (os.environ.get("PIPELINE_A_TRACK_TABLE_ONLY") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    syms_slice = syms[:n_print]

    table_rows: List[List[str]] = []
    for sym in syms_slice:
        dom, subd, tier, ind = meta.get(sym, ("", "", None, ""))
        dom_s = dom if dom else "—"
        sub_s = subd if subd else "—"
        tier_s = tier_int_to_label_cn(tier)
        ind_s = ind if ind else "—"
        parts = labels_by_sym.get(sym) or []
        if parts:
            top3 = " > ".join(
                "%s（%.1f%%）" % (_truncate(p[0], 16), float(p[1] or 0) * 100.0) for p in parts[:3]
            )
        else:
            top3 = "—"
        n_sym = news_by_sym.get(sym, 0)
        ind_key = ind_s.strip() if ind_s and ind_s != "—" else ""
        n_ind = news_by_industry.get(ind_key, 0) if ind_key else -1
        b = cache_bundle.get(sym) or _a_track_signal_display_bundle(None)
        if n_ind < 0:
            n_ind_cell = "—"
        else:
            n_ind_cell = str(n_ind)
        origin_short = _truncate(b.get("signal_origin_cn") or "—", 26)
        table_rows.append(
            [
                sym,
                _truncate(_symbol_name_short(sym, name_by_sym), 14),
                ind_s if ind_s != "—" else "—",
                dom_s,
                sub_s if sub_s != "—" else "—",
                tier_s,
                str(n_sym),
                n_ind_cell,
                "是" if b.get("llm_tag_yes") else "否",
                _a_track_signal_conclusion_cell(b),
                origin_short,
                _truncate(top3, 36),
            ]
        )

    print()
    print("  ══ A 轨 · 信号层观测（表格式：新闻条数 / 是否 LLM / 信号结论）══")
    print(
        "  · signal_source=llm：`signal_summary`（JSON）内字段，表示本条缓存由谁写入。"
        "仅当值为 llm 时，方向/强度/类型/风险才是可采纳的模型信号；否则「信号结论」列一律为「无信号」（含历史 JSON、未配 API）。"
    )
    print(
        "  · 主营：财报分部营收 Top3（降序）。近 %d 天：标的= news_content scope=symbol；行业= scope=industry 且 scope_id=申万行业名（不一致则行业列为 0）。"
        % days_back
    )
    print()
    hdr = [
        "标的",
        "简称",
        "申万",
        "领域",
        "子域",
        "层级",
        "近%dd标的" % days_back,
        "近%dd行业" % days_back,
        "LLM",
        "信号结论",
        "信号来源(截断)",
        "主营Top3(截断)",
    ]
    for ln in _ascii_table(hdr, table_rows, max_cell=56):
        print("  " + ln)
    print()

    if not table_only:
        print("  ── 逐标的明细（完整主营 / 信号来源；仅 LLM 时打印模型摘要，不打印非 AI 缓存正文）──")
        for sym in syms_slice:
            dom, subd, tier, ind = meta.get(sym, ("", "", None, ""))
            dom_s = dom if dom else "—"
            sub_s = subd if subd else "—"
            tier_s = tier_int_to_label_cn(tier)
            ind_s = ind if ind else "—"
            nn = (name_by_sym.get(sym) or "").strip()
            parts = labels_by_sym.get(sym) or []
            if parts:
                top3 = " > ".join(
                    "%s（%.1f%%）" % (_truncate(p[0], 20), float(p[1] or 0) * 100.0) for p in parts[:3]
                )
            else:
                top3 = "—"
            n_sym = news_by_sym.get(sym, 0)
            ind_key = ind_s.strip() if ind_s and ind_s != "—" else ""
            n_ind = news_by_industry.get(ind_key, 0) if ind_key else -1
            b = cache_bundle.get(sym) or _a_track_signal_display_bundle(None)
            if nn:
                print("  —— %s · %s ——" % (sym, nn))
            else:
                print("  —— %s ——" % sym)
            print(
                "    分类: 领域=%s | 申万=%s | 层级=%s | 子域=%s"
                % (dom_s, ind_s if ind_s != "—" else "—", tier_s, sub_s if sub_s != "—" else "—")
            )
            if n_ind < 0:
                print(
                    "    近%dd: 标的新闻/公告 %d 条 | 行业新闻/公告 —（无申万行业名）"
                    % (days_back, n_sym)
                )
            else:
                print(
                    "    近%dd: 标的新闻/公告 %d 条 | 行业新闻/公告 %d 条（申万「%s」）"
                    % (days_back, n_sym, n_ind, ind_key)
                )
            print("    主营（营收 Top3）: %s" % top3)
            print("    信号来源: %s" % b.get("signal_origin_cn", "—"))
            print("    大模型打标: %s" % ("是（signal_source=llm）" if b.get("llm_tag_yes") else "否"))
            print("    大模型摘要: %s" % ("是" if b.get("llm_summary_yes") else "否"))
            if b.get("has_cache") and b.get("llm_tag_yes") and (b.get("tag_fields_line") or "").strip():
                print("    模型标签（signal_source=llm）: %s" % b.get("tag_fields_line"))
            sm_full = (b.get("summary_full") or "").strip()
            if sm_full and b.get("llm_summary_yes"):
                sm_wrap = textwrap.fill(
                    sm_full,
                    width=max(48, w - 6),
                    initial_indent="      ",
                    subsequent_indent="      ",
                )
                print("    模型摘要全文:")
                print(sm_wrap)
            elif b.get("has_cache") and b.get("llm_tag_yes") and not sm_full:
                print("    摘要: —")
            print()

    if len(syms) > n_print:
        print("  … 余下 %d 只省略（PIPELINE_SEGMENT_TABLE_MAX=%s）" % (len(syms) - n_print, max_rows))
    print(
        "  （完）B 轨：DITING_TRACK=b → segment_signal_cache。"
        + (
            " 仅要汇总表可设 PIPELINE_A_TRACK_TABLE_ONLY=1。"
            if not table_only
            else ""
        )
    )
    print()
