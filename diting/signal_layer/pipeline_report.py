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


def _parse_summary(raw: Any) -> Tuple[str, str, str, str, str]:
    """signal_summary JSON → type, direction, risk, summary_cn(短), summary_full, strength_str"""
    if raw is None:
        return "", "", "", "", "", ""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return "", "", "", _truncate(raw, 80), raw, ""
    if not isinstance(raw, dict):
        s = str(raw)
        return "", "", "", s[:80], s, ""
    t = str(raw.get("type") or "")
    d = str(raw.get("direction") or "")
    rt = raw.get("risk_tags") or []
    rs = ",".join(str(x) for x in rt) if isinstance(rt, list) else str(rt)
    sm = str(raw.get("summary_cn") or "")
    st = raw.get("strength")
    ss = ""
    if st is not None and isinstance(st, (int, float)):
        ss = "%.2f" % float(st)
    return t, d, rs, _truncate(sm, 72), sm, ss


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
            st, sd, rk, _sm_short, sm_full, ss = _parse_summary(summ)
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


def print_a_track_symbol_news_summary(
    dsn: str,
    symbols: List[str],
    *,
    days_back: int = 7,
    max_rows: int = 120,
) -> None:
    """
    A 轨专用：主营最高营收行对应的「领域 domain」+ 申万行业 + L2 news_content 标的条数。
    不读 segment_signal_cache，不展示/处理细分 segment 行。
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
            SELECT DISTINCT ON (p.symbol)
                   p.symbol,
                   COALESCE(r.domain, ''),
                   COALESCE(r.name_cn, ''),
                   COALESCE(i.industry_name, '')
            FROM symbol_business_profile p
            LEFT JOIN segment_registry r ON r.segment_id = p.segment_id
            LEFT JOIN industry_revenue_summary i ON i.symbol = p.symbol
            WHERE p.symbol = ANY(%s)
            ORDER BY p.symbol, p.revenue_share DESC NULLS LAST
            """,
            (syms,),
        )
        primary: Dict[str, Tuple[str, str, str]] = {}
        for sym, dom, nm, ind in cur.fetchall() or []:
            primary[str(sym or "").strip().upper()] = (
                str(dom or "").strip(),
                str(nm or "").strip(),
                str(ind or "").strip(),
            )

        cur.close()
        conn.close()
    except Exception as e:
        print("  [A 轨观测] L2 查询失败: %s" % e)
        return

    n_print = min(len(syms), max_rows)
    print()
    print("  ══ A 轨 · 信号层观测（领域 + 申万 + 标的新闻；无细分 segment refresh）══")
    print(
        "  说明: 领域=主营营收占比最高行的 segment_registry.domain（三分类）；"
        "申万=industry_revenue_summary.industry_name；"
        "新闻=L2 news_content 近 %d 天按标的聚合（非细分拉取）。"
        % days_back
    )
    print()
    bar = "  " + "─" * min(92, 120)
    print(bar)
    print("  %-12s  │  %-6s  │  %-14s  │  %-20s  │  近%dd新闻" % ("标的", "领域", "申万行业", "主营注册名(首行)", days_back))
    print(bar)
    for i, sym in enumerate(syms[:n_print], start=1):
        dom, nm, ind = primary.get(sym, ("", "", ""))
        dom_s = dom if dom else "—"
        ind_s = _truncate(ind if ind else "—", 14)
        nm_s = (nm[:20] + "…") if len(nm) > 20 else (nm or "—")
        print("  %-12s  │  %-6s  │  %-14s  │  %-20s  │  %5d 条" % (sym, dom_s, ind_s, nm_s, news_by_sym.get(sym, 0)))
    if len(syms) > n_print:
        print("  … 余下 %d 只省略（PIPELINE_SEGMENT_TABLE_MAX=%s）" % (len(syms) - n_print, max_rows))
    print(bar)
    print("  A 轨：a_track_signal_cache（标的+申万行业双路）；细分 segment_signal_cache 请 DITING_TRACK=b。")
    print()
