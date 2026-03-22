#!/usr/bin/env python3
# [Ref: 02_量化扫描引擎_实践] 一键查询 B 模块写入的数据：L2 表 quant_signal_snapshot / quant_signal_scan_all
# 用法：在 diting-core 根目录 make query-module-b-output 或 PYTHONPATH=. python3 scripts/query_scanner_output.py
# 输出按「汇总 → 明细」分层；列宽按 CJK 显示宽度对齐，避免中文错位与终端折行难看
# 通过表明细默认只展示「最新一批次」行，避免同一标的跨历史批次重复出现；若要看混合时间线：
#   QUERY_SCANNER_SNAPSHOT_ALL_BATCHES=1 make query-module-b-output
# 按「某日最后一次写入」查（上海时区日历日；与 [2][3] 同一 batch_id）：
#   make query-module-b-output 3-22
#   或 QUERY_SCANNER_DATE=3-22 QUERY_SCANNER_BATCH_INDEX=0 make query-module-b-output
# BATCH_INDEX：该日内第几批（0=当日最晚一批，1=倒数第二批）；不设 DATE 时表示全局最近第几批。

import json
import os
import re
import sys
import unicodedata
from datetime import date
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

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


def _disp_width(s) -> int:
    """终端显示宽度（中日韩等按双宽）。"""
    w = 0
    for ch in str(s):
        e = unicodedata.east_asian_width(ch)
        w += 2 if e in ("F", "W", "A") else 1
    return w


def _truncate_disp(s, max_w: int) -> str:
    s = str(s)
    if _disp_width(s) <= max_w:
        return s
    out = []
    w = 0
    for ch in s:
        dw = 2 if unicodedata.east_asian_width(ch) in ("F", "W", "A") else 1
        if w + dw > max_w:
            break
        out.append(ch)
        w += dw
    return "".join(out)


def _pad_cell(s, width: int, align: str = "l") -> str:
    """将单元格填充/截断到固定显示宽度 width。"""
    s = _truncate_disp(s, width)
    pad = width - _disp_width(s)
    if pad < 0:
        pad = 0
    if align == "r":
        return " " * pad + s
    return s + " " * pad


def _rule(char: str = "─", n: int = 88) -> str:
    return char * n


def _format_time_short(created):
    if created is None:
        return ""
    try:
        if getattr(created, "tzinfo", None):
            from datetime import timedelta, timezone

            utc8 = timezone(timedelta(hours=8))
            local = created.astimezone(utc8)
        else:
            local = created
        return local.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(created)[:16]


def _fmt_price_cell(x) -> str:
    """L2 入场/止损价终端展示。"""
    if x is None:
        return "—"
    try:
        return "%.4f" % float(x)
    except (TypeError, ValueError):
        return "—"


def _tp_list_from_raw(raw):
    """解析 take_profit_json（或已解析列表）为浮点价列表。"""
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        xs = list(raw)
    else:
        s = str(raw).strip()
        if not s or s == "[]":
            return []
        try:
            xs = json.loads(s)
        except (json.JSONDecodeError, TypeError, ValueError):
            return []
    out = []
    for x in xs:
        try:
            out.append(float(x))
        except (TypeError, ValueError):
            pass
    return out


def _fmt_tp_json_cell(raw) -> str:
    """L2 take_profit_json：紧凑展示多档止盈价。"""
    xs = _tp_list_from_raw(raw)
    if not xs:
        if raw is not None and str(raw).strip() not in ("", "[]"):
            return _truncate_disp(str(raw), 24)
        return "—"
    parts = []
    for x in xs[:4]:
        parts.append("%.2f" % float(x))
    out = ",".join(parts)
    if len(xs) > 4:
        out += "…"
    return _truncate_disp(out, 24)


def _pct_vs_entry(entry: object, price: object):
    """相对参考入场：(price-entry)/entry×100，与 run_module_b_local._pct_vs_entry 一致。"""
    try:
        e = float(entry)
        p = float(price)
        if e <= 1e-12:
            return None
        return (p - e) / e * 100.0
    except (TypeError, ValueError):
        return None


def _fmt_pct_cell(p: object) -> str:
    if p is None:
        return "—"
    try:
        return "%+.2f%%" % float(p)
    except (TypeError, ValueError):
        return "—"


def _risk_pct_cells(entry, stop, tp_raw):
    """止损%、止盈1%、止盈2%（相对入场；不入库，由价推算）。"""
    tps = _tp_list_from_raw(tp_raw)
    sl = _pct_vs_entry(entry, stop)
    tp1 = _pct_vs_entry(entry, tps[0]) if len(tps) > 0 else None
    tp2 = _pct_vs_entry(entry, tps[1]) if len(tps) > 1 else None
    return _fmt_pct_cell(sl), _fmt_pct_cell(tp1), _fmt_pct_cell(tp2)


# 与 l2_snapshot_writer / run_module_b_local POOL_NAMES 一致（VARCHAR 入库为大写英文枚举）
_STRATEGY_CN = {
    "UNSPECIFIED": "未指定",
    "TREND": "趋势主导",
    "REVERSION": "反转主导",
    "BREAKOUT": "突破主导",
    "MOMENTUM": "动量主导",
}


def _strategy_display(src) -> str:
    """策略来源：库内为英文枚举，终端展示统一中文。"""
    src_raw = (src or "").strip()
    if not src_raw:
        return "—"
    u = src_raw.upper()
    if u.startswith("UNSPEC"):
        return "未指定"
    if u in _STRATEGY_CN:
        return _STRATEGY_CN[u]
    return _truncate_disp(src_raw, 10)


def _has_symbol_names_table(cur) -> bool:
    try:
        cur.execute("SELECT to_regclass('public.symbol_names')")
        return cur.fetchone()[0] is not None
    except Exception:
        return False


def _load_csv_symbol_names():
    try:
        from diting.scanner.symbol_names import load_symbol_names_csv_only

        return load_symbol_names_csv_only(root=ROOT, names_csv="config/symbol_names.csv")
    except Exception:
        return {}


def _snap_name_sql(join_sn: bool):
    """返回 (简称表达式, FROM … JOIN)。"""
    if join_sn:
        expr = (
            "COALESCE(NULLIF(TRIM(q.symbol_name), ''), NULLIF(TRIM(sn.name_cn), ''), '')"
        )
        fr = (
            "FROM quant_signal_snapshot q "
            "LEFT JOIN symbol_names sn ON sn.symbol = q.symbol"
        )
    else:
        expr = "COALESCE(NULLIF(TRIM(q.symbol_name), ''), '')"
        fr = "FROM quant_signal_snapshot q"
    return expr, fr


def _scan_name_sql(join_sn: bool):
    if join_sn:
        expr = (
            "COALESCE(NULLIF(TRIM(a.symbol_name), ''), NULLIF(TRIM(sn.name_cn), ''), '')"
        )
        fr = (
            "FROM quant_signal_scan_all a "
            "LEFT JOIN symbol_names sn ON sn.symbol = a.symbol"
        )
    else:
        expr = "COALESCE(NULLIF(TRIM(a.symbol_name), ''), '')"
        fr = "FROM quant_signal_scan_all a"
    return expr, fr


def _fill_short_name(symbol, name_from_row: str, csv_map: dict) -> str:
    s = (name_from_row or "").strip()
    if s:
        return s
    return (csv_map.get(symbol or "", "") or "").strip()


def _short_uuid(u, prefix: int = 8) -> str:
    if not u:
        return ""
    u = str(u)
    return u[:prefix] + "…" if len(u) > prefix else u


def _print_table_row(cells, widths, aligns=None):
    n = len(widths)
    if aligns is None:
        aligns = ["l"] * n
    elif len(aligns) < n:
        # 与 widths 不对齐时 zip 会截断列，曾导致末列「写入时间」丢失
        aligns = list(aligns) + ["l"] * (n - len(aligns))
    parts = [_pad_cell(c, w, a) for c, w, a in zip(cells, widths, aligns)]
    print("  " + " │ ".join(parts))


def _parse_query_calendar_day(raw: str):
    """
    解析 3-22 / 03-22 / 2026-3-22 / 2026-03-22（月-日 或 年-月-日），返回 date；无效则 None。
    未写年份时默认当前年。
    """
    if not raw or not str(raw).strip():
        return None
    s = str(raw).strip()
    parts = [p for p in re.split(r"[-/]", s) if p]
    try:
        if len(parts) == 2:
            month, day = int(parts[0]), int(parts[1])
            year = date.today().year
            return date(year, month, day)
        if len(parts) == 3:
            year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
            return date(year, month, day)
    except (ValueError, TypeError):
        pass
    return None


def _batch_index_from_env() -> int:
    v = os.environ.get("QUERY_SCANNER_BATCH_INDEX", "0").strip()
    try:
        i = int(v, 10)
        return max(0, i)
    except ValueError:
        return 0


def _tz_sql():
    """created_at 按此时区划日历日；与 _format_time_short 展示一致（默认上海）。"""
    return os.environ.get("QUERY_SCANNER_TZ", "Asia/Shanghai").strip() or "Asia/Shanghai"


def main():
    dsn = os.environ.get("PG_L2_DSN", "").strip()
    if not dsn:
        print("未配置 PG_L2_DSN，无法查询 L2。请在 .env 中配置 PG_L2_DSN。", file=sys.stderr)
        sys.exit(1)
    try:
        import psycopg2
    except ImportError:
        print("未安装 psycopg2，无法查询。pip install psycopg2-binary", file=sys.stderr)
        sys.exit(1)

    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        join_sn = _has_symbol_names_table(cur)
        snap_nm, snap_from = _snap_name_sql(join_sn)
        scan_nm, scan_from = _scan_name_sql(join_sn)
        csv_names = _load_csv_symbol_names()

        try:
            cur.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'quant_signal_snapshot'
                  AND column_name = 'evaluation_source' LIMIT 1
                """
            )
            snap_has_eval_cols = cur.fetchone() is not None
        except Exception:
            snap_has_eval_cols = False
        try:
            cur.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'quant_signal_scan_all'
                  AND column_name = 'evaluation_source' LIMIT 1
                """
            )
            scan_has_eval_cols = cur.fetchone() is not None
        except Exception:
            scan_has_eval_cols = False

        snap_sel_eval = (
            ", COALESCE(q.evaluation_source,'FRESH'), COALESCE(q.scanner_rules_fingerprint,'')"
            if snap_has_eval_cols
            else ""
        )
        scan_sel_eval = (
            ", COALESCE(a.evaluation_source,'FRESH'), COALESCE(a.scanner_rules_fingerprint,'')"
            if scan_has_eval_cols
            else ""
        )

        W_SNAP_LEGACY = (
            6,
            10,
            11,
            8,
            8,
            10,
            6,
            6,
            6,
            6,
            6,
            6,
            10,
            10,
            26,
            8,
            8,
            8,
            16,
        )
        W_SNAP = W_SNAP_LEGACY[:-4] + (8, 10) + W_SNAP_LEGACY[-4:] if snap_has_eval_cols else W_SNAP_LEGACY
        W_SCAN_LEGACY = (
            11,
            8,
            8,
            10,
            6,
            6,
            6,
            6,
            6,
            10,
            10,
            26,
            8,
            8,
            8,
            8,
            16,
        )
        W_SCAN = W_SCAN_LEGACY[:-2] + (8, 10) + W_SCAN_LEGACY[-2:] if scan_has_eval_cols else W_SCAN_LEGACY
        RULE_SNAP_SCAN = 176 if (snap_has_eval_cols or scan_has_eval_cols) else 158

        print()
        print(_rule("═", 88))
        print("  B 模块 L2 查询（通过表 → 供下游 C 模块；全量表 → 自查 / 对照）")
        print(_rule("═", 88))

        all_batches_snap = os.environ.get("QUERY_SCANNER_SNAPSHOT_ALL_BATCHES", "").strip().lower() in (
            "1",
            "true",
            "yes",
        )

        qdate = None
        if len(sys.argv) > 1:
            qdate = _parse_query_calendar_day(sys.argv[1])
        if qdate is None:
            qdate = _parse_query_calendar_day(os.environ.get("QUERY_SCANNER_DATE", "").strip())
        batch_idx = _batch_index_from_env()
        tz = _tz_sql()
        focus_scan_by_batch = (qdate is not None) or (batch_idx > 0)

        # --- 通过表 batch 汇总 ---
        if qdate:
            cur.execute(
                """
                SELECT batch_id, COUNT(*), MAX(created_at)
                FROM quant_signal_snapshot
                WHERE DATE(created_at AT TIME ZONE %s) = %s
                GROUP BY batch_id
                ORDER BY MAX(created_at) DESC
                LIMIT 30
                """,
                (tz, qdate),
            )
        else:
            cur.execute(
                """
                SELECT batch_id, COUNT(*), MAX(created_at)
                FROM quant_signal_snapshot
                GROUP BY batch_id
                ORDER BY MAX(created_at) DESC
                LIMIT 30
                """
            )
        rows = cur.fetchall()
        latest_snap_batch = rows[batch_idx][0] if len(rows) > batch_idx else None
        scan_batch_filter = latest_snap_batch if focus_scan_by_batch else None

        print()
        if qdate:
            print(
                "  筛选：日历日 %s（时区 %s） BATCH_INDEX=%s → [2][3] 明细对齐该日内第 %s 新批次（0=当日最晚一批）"
                % (qdate, tz, batch_idx, batch_idx + 1)
            )
        elif batch_idx > 0:
            print(
                "  筛选：全局第 %s 新批次（BATCH_INDEX=%s）；[4] 全量表明细与 [2] 同批"
                % (batch_idx + 1, batch_idx)
            )
        if qdate is None and batch_idx == 0:
            disp = rows[:5]
        else:
            disp = rows[:20]
        sec1_title = (
            "▶ [1] 通过表（表名 quant_signal_snapshot）— 指定日内各批次"
            if qdate
            else "▶ [1] 通过表（表名 quant_signal_snapshot）— 最近 5 次扫描批次"
        )
        print(sec1_title)
        print("  说明：B 模块每跑完一轮扫描会写入一批结果，并为该批分配一个「批次号」（UUID）。")
        print("  本块按批次汇总：每行 = 一次跑批；行数 = 该批写入通过表的记录条数；末列可复制完整批次号。")
        print("  " + _rule("─", 86))
        if not rows:
            print("  （空）请先 make run-module-b；表未建则 make init-l2-quant-signal-table")
            if qdate:
                print("  （指定日 %s 在 quant_signal_snapshot 中无写入记录）" % qdate)
        else:
            if latest_snap_batch is None:
                print(
                    "  （批次不足：当前 BATCH_INDEX=%s，该条件下仅有 %s 个批次）"
                    % (batch_idx, len(rows))
                )
            print(
                "  %s │ %s │ %s │ %s"
                % (
                    _pad_cell("序", 4),
                    _pad_cell("行数", 6, "r"),
                    _pad_cell("最新写入时间", 16),
                    "批次号（完整 UUID）",
                )
            )
            print("  " + _rule("─", 86))
            for i, (batch_id, cnt, created) in enumerate(disp, 1):
                mark = ""
                if latest_snap_batch and batch_id == latest_snap_batch:
                    mark = "  ← [2] 本批"
                print(
                    "  %s │ %s │ %s │ %s%s"
                    % (
                        _pad_cell(str(i), 4),
                        _pad_cell(str(cnt), 6, "r"),
                        _pad_cell(_format_time_short(created), 16),
                        batch_id or "",
                        mark,
                    )
                )

        # --- 通过表明细（简称：快照列 + L2 symbol_names + 本地 config/symbol_names.csv 展示补全）---
        skip_focused = focus_scan_by_batch and latest_snap_batch is None
        if skip_focused:
            detail = []
            has_pool_scores = True
            has_percentile = True
        if not skip_focused:
            try:
                if not all_batches_snap and latest_snap_batch:
                    cur.execute(
                        f"""
                        SELECT q.id, q.batch_id, q.symbol, {snap_nm},
                               q.technical_score, q.strategy_source, q.sector_strength,
                               COALESCE(q.trend_score,0), COALESCE(q.reversion_score,0), COALESCE(q.breakout_score,0), COALESCE(q.momentum_score,0),
                               COALESCE(q.technical_score_percentile, 0),
                               q.entry_reference_price, q.stop_loss_price, q.take_profit_json{snap_sel_eval}, q.created_at
                        {snap_from}
                        WHERE q.batch_id = %s
                        ORDER BY q.technical_score DESC, q.symbol ASC
                        LIMIT 500
                        """,
                        (latest_snap_batch,),
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT q.id, q.batch_id, q.symbol, {snap_nm},
                               q.technical_score, q.strategy_source, q.sector_strength,
                               COALESCE(q.trend_score,0), COALESCE(q.reversion_score,0), COALESCE(q.breakout_score,0), COALESCE(q.momentum_score,0),
                               COALESCE(q.technical_score_percentile, 0),
                               q.entry_reference_price, q.stop_loss_price, q.take_profit_json{snap_sel_eval}, q.created_at
                        {snap_from}
                        ORDER BY q.created_at DESC
                        LIMIT 30
                        """
                    )
                detail = cur.fetchall()
                has_pool_scores = True
                has_percentile = True
            except Exception:
                has_percentile = False
                try:
                    if not all_batches_snap and latest_snap_batch:
                        cur.execute(
                            f"""
                            SELECT q.id, q.batch_id, q.symbol, {snap_nm},
                                   q.technical_score, q.strategy_source, q.sector_strength,
                                   q.entry_reference_price, q.stop_loss_price, q.take_profit_json{snap_sel_eval}, q.created_at
                            {snap_from}
                            WHERE q.batch_id = %s
                            ORDER BY q.technical_score DESC, q.symbol ASC
                            LIMIT 500
                            """,
                            (latest_snap_batch,),
                        )
                    else:
                        cur.execute(
                            f"""
                            SELECT q.id, q.batch_id, q.symbol, {snap_nm},
                                   q.technical_score, q.strategy_source, q.sector_strength,
                                   q.entry_reference_price, q.stop_loss_price, q.take_profit_json{snap_sel_eval}, q.created_at
                            {snap_from}
                            ORDER BY q.created_at DESC
                            LIMIT 30
                            """
                        )
                    detail = cur.fetchall()
                    has_pool_scores = False
                except Exception:
                    _legacy_nm = (
                        "COALESCE(NULLIF(TRIM(sn.name_cn), ''), '')"
                        if join_sn
                        else "''"
                    )
                    _legacy_fr = (
                        "FROM quant_signal_snapshot q "
                        "LEFT JOIN symbol_names sn ON sn.symbol = q.symbol"
                        if join_sn
                        else "FROM quant_signal_snapshot q"
                    )
                    if not all_batches_snap and latest_snap_batch:
                        cur.execute(
                            f"""
                            SELECT q.id, q.batch_id, q.symbol, {_legacy_nm},
                                   q.technical_score, q.strategy_source, q.sector_strength,
                                   q.entry_reference_price, q.stop_loss_price, q.take_profit_json{snap_sel_eval}, q.created_at
                            {_legacy_fr}
                            WHERE q.batch_id = %s
                            ORDER BY q.technical_score DESC, q.symbol ASC
                            LIMIT 500
                            """,
                            (latest_snap_batch,),
                        )
                    else:
                        cur.execute(
                            f"""
                            SELECT q.id, q.batch_id, q.symbol, {_legacy_nm},
                                   q.technical_score, q.strategy_source, q.sector_strength,
                                   q.entry_reference_price, q.stop_loss_price, q.take_profit_json{snap_sel_eval}, q.created_at
                            {_legacy_fr}
                            ORDER BY q.created_at DESC
                            LIMIT 30
                            """
                        )
                    detail = cur.fetchall()
                    has_pool_scores = False
                has_percentile = False

        print()
        if all_batches_snap:
            print("▶ [2] 通过表明细（跨批次：按写入时间倒序，至多 30 条；同一标的多行 = 不同批次历史）")
        else:
            print("▶ [2] 通过表明细（仅上表「序 1」对应批次；总分高 → 低；每标的至多一行）")
        print(
            "  说明：「简称」优先快照写入时的名称；为空时用 L2 表 symbol_names，"
            "仍空则读本地 config/symbol_names.csv（仅终端补全，不改库）。"
            "「批次简码」为批次号前 8 位，完整号见 [1] 末列。"
            "入场参考/止损/止盈来自 L2：entry_reference_price、stop_loss_price、take_profit_json（与 run-module-b 风控一致）。"
            "止损%%、止盈1%%、止盈2%% 不入库，由价相对入场推算：(价-入场)/入场×100%%，与「风控参考」表一致。"
            " 若本轮曾跳过冷却标的，run-module-b 会将 L2 中该标的上条「通过表」快照沿用写入本 batch_id，"
            "本块为该批完整通过集（沿用行分数为止损表上次写入值、本轮未重算）。"
        )
        if all_batches_snap:
            print("  提示：此为时间线模式；关闭请 unset QUERY_SCANNER_SNAPSHOT_ALL_BATCHES。")
        else:
            print("  提示：跨批次混排请 QUERY_SCANNER_SNAPSHOT_ALL_BATCHES=1 make query-module-b-output")
        print("  " + _rule("─", RULE_SNAP_SCAN))
        if detail and has_pool_scores:
            if snap_has_eval_cols:
                hdr = (
                    "记录ID",
                    "批次简码",
                    "标的",
                    "简称",
                    "总分",
                    "策略",
                    "板块",
                    "趋势",
                    "反转",
                    "突破",
                    "动量",
                    "分位",
                    "入场参考",
                    "止损",
                    "止盈",
                    "止损%",
                    "止盈1%",
                    "止盈2%",
                    "来源",
                    "规则指纹",
                    "写入时间",
                )
            else:
                hdr = (
                    "记录ID",
                    "批次简码",
                    "标的",
                    "简称",
                    "总分",
                    "策略",
                    "板块",
                    "趋势",
                    "反转",
                    "突破",
                    "动量",
                    "分位",
                    "入场参考",
                    "止损",
                    "止盈",
                    "止损%",
                    "止盈1%",
                    "止盈2%",
                    "写入时间",
                )
            _print_table_row(hdr, W_SNAP)
            print("  " + _rule("─", RULE_SNAP_SCAN))
            for r in detail:
                ev = fp = None
                if snap_has_eval_cols and len(r) >= 18:
                    rid, bid, sym, sym_name, score, src, sector, t, rv, br, m, pct, entry, stop, tpj, ev, fp, created = r
                elif has_percentile and len(r) >= 16:
                    rid, bid, sym, sym_name, score, src, sector, t, rv, br, m, pct, entry, stop, tpj, created = r
                elif has_percentile and len(r) >= 13:
                    rid, bid, sym, sym_name, score, src, sector, t, rv, br, m, pct, created = r
                    entry, stop, tpj = None, None, None
                else:
                    rid, bid, sym, sym_name, score, src, sector, t, rv, br, m, created = r[:12]
                    pct = 0.0
                    entry, stop, tpj = None, None, None
                strat = _strategy_display(src)
                disp_nm = _fill_short_name(sym, sym_name, csv_names)
                sl_pct_s, tp1_pct_s, tp2_pct_s = _risk_pct_cells(entry, stop, tpj)
                src_disp = (
                    "沿用"
                    if (ev is not None and str(ev).upper().strip() == "CARRYOVER")
                    else "重算"
                )
                fp_disp = _truncate_disp(str(fp or ""), 10) if snap_has_eval_cols else ""
                cells_core = (
                    rid,
                    _short_uuid(bid, 8),
                    sym or "",
                    disp_nm,
                    "%.2f" % (score or 0),
                    strat,
                    "%.2f" % (sector or 0),
                    "%.2f" % (t or 0),
                    "%.2f" % (rv or 0),
                    "%.2f" % (br or 0),
                    "%.2f" % (m or 0),
                    "%.2f" % (pct or 0),
                    _fmt_price_cell(entry),
                    _fmt_price_cell(stop),
                    _fmt_tp_json_cell(tpj),
                    sl_pct_s,
                    tp1_pct_s,
                    tp2_pct_s,
                )
                if snap_has_eval_cols:
                    cells = cells_core + (src_disp, fp_disp, _format_time_short(created))
                    aligns = [
                        "r",
                        "l",
                        "l",
                        "l",
                        "r",
                        "l",
                        "r",
                        "r",
                        "r",
                        "r",
                        "r",
                        "r",
                        "r",
                        "r",
                        "r",
                        "r",
                        "r",
                        "l",
                        "l",
                        "l",
                    ]
                else:
                    cells = cells_core + (_format_time_short(created),)
                    aligns = [
                        "r",
                        "l",
                        "l",
                        "l",
                        "r",
                        "l",
                        "r",
                        "r",
                        "r",
                        "r",
                        "r",
                        "r",
                        "r",
                        "r",
                        "r",
                        "r",
                        "r",
                        "l",
                    ]
                _print_table_row(cells, W_SNAP, aligns)
        elif detail:
            sample = detail[0]
            if len(sample) >= 11:
                w11 = (6, 10, 11, 8, 8, 12, 6, 10, 10, 26, 8, 8, 8, 16)
                _print_table_row(
                    (
                        "记录ID",
                        "批次简码",
                        "标的",
                        "简称",
                        "总分",
                        "策略",
                        "板块",
                        "入场参考",
                        "止损",
                        "止盈",
                        "止损%",
                        "止盈1%",
                        "止盈2%",
                        "写入时间",
                    ),
                    w11,
                )
                print("  " + _rule("─", RULE_SNAP_SCAN))
                for r in detail:
                    rid, bid, sym, sym_name, score, src, sector, entry, stop, tpj, created = r[:11]
                    _strat_s = _strategy_display(src)
                    if len(_strat_s) > 12:
                        _strat_s = _truncate_disp(_strat_s, 12)
                    disp_nm = _fill_short_name(sym, sym_name, csv_names)
                    sl_pct_s, tp1_pct_s, tp2_pct_s = _risk_pct_cells(entry, stop, tpj)
                    _print_table_row(
                        (
                            rid,
                            _short_uuid(bid, 8),
                            sym or "",
                            disp_nm,
                            "%.2f" % (score or 0),
                            _strat_s,
                            "%.2f" % (sector or 0),
                            _fmt_price_cell(entry),
                            _fmt_price_cell(stop),
                            _fmt_tp_json_cell(tpj),
                            sl_pct_s,
                            tp1_pct_s,
                            tp2_pct_s,
                            _format_time_short(created),
                        ),
                        w11,
                        ["r", "l", "l", "l", "r", "l", "r", "r", "r", "r", "r", "r", "l"],
                    )
            else:
                w = (6, 10, 11, 8, 8, 12, 6, 16)
                _print_table_row(("记录ID", "批次简码", "标的", "简称", "总分", "策略", "板块", "写入时间"), w)
                print("  " + _rule("─", 86))
                for r in detail:
                    if len(r) >= 8:
                        rid, bid, sym, sym_name, score, src, sector, created = r[:8]
                    else:
                        rid, bid, sym, score, src, sector, created = r
                        sym_name = ""
                    _strat_s = _strategy_display(src)
                    if len(_strat_s) > 12:
                        _strat_s = _truncate_disp(_strat_s, 12)
                    disp_nm = _fill_short_name(sym, sym_name, csv_names)
                    _print_table_row(
                        (
                            rid,
                            _short_uuid(bid, 8),
                            sym or "",
                            disp_nm,
                            "%.2f" % (score or 0),
                            _strat_s,
                            "%.2f" % (sector or 0),
                            _format_time_short(created),
                        ),
                        w,
                        ["r", "l", "l", "l", "r", "l", "r", "l"],
                    )
        else:
            if skip_focused:
                print(
                    "  （无明细：[2] 当前筛选下无可用批次；请减小 QUERY_SCANNER_BATCH_INDEX 或换有数据的日期）"
                )
            else:
                print("  （无明细行）")

        # --- 全量表 ---
        try:
            if qdate:
                cur.execute(
                    """
                    SELECT batch_id, COUNT(*), SUM(CASE WHEN passed THEN 1 ELSE 0 END), MAX(created_at)
                    FROM quant_signal_scan_all
                    WHERE DATE(created_at AT TIME ZONE %s) = %s
                    GROUP BY batch_id
                    ORDER BY MAX(created_at) DESC
                    LIMIT 20
                    """,
                    (tz, qdate),
                )
            else:
                cur.execute(
                    """
                    SELECT batch_id, COUNT(*), SUM(CASE WHEN passed THEN 1 ELSE 0 END), MAX(created_at)
                    FROM quant_signal_scan_all
                    GROUP BY batch_id
                    ORDER BY MAX(created_at) DESC
                    LIMIT 5
                    """
                )
            all_rows = cur.fetchall()
            print()
            print(
                "▶ [3] 全量表（表名 quant_signal_scan_all）— "
                + ("指定日内各批次" if qdate else "最近 5 次扫描批次")
            )
            print("  说明：每批包含「全市场扫描」全部标的；末两列表示该批总条数、其中标记为通过的条数。")
            print("  " + _rule("─", 86))
            if all_rows:
                print(
                    "  %s │ %s │ %s │ %s │ %s"
                    % (
                        _pad_cell("序", 4),
                        _pad_cell("全量", 6, "r"),
                        _pad_cell("通过", 6, "r"),
                        _pad_cell("最新写入时间", 16),
                        "批次号（完整 UUID）",
                    )
                )
                print("  " + _rule("─", 86))
                for i, (batch_id, total, passed_cnt, created) in enumerate(all_rows, 1):
                    mark = ""
                    if scan_batch_filter and batch_id == scan_batch_filter:
                        mark = "  ← [4] 本批"
                    print(
                        "  %s │ %s │ %s │ %s │ %s%s"
                        % (
                            _pad_cell(str(i), 4),
                            _pad_cell(str(total), 6, "r"),
                            _pad_cell(str(passed_cnt), 6, "r"),
                            _pad_cell(_format_time_short(created), 16),
                            batch_id or "",
                            mark,
                        )
                    )

                scan_wh = ""
                scan_qp = ()
                if scan_batch_filter:
                    scan_wh = " WHERE a.batch_id = %s "
                    scan_qp = (scan_batch_filter,)

                try:
                    cur.execute(
                        f"""
                        SELECT a.symbol, {scan_nm}, a.technical_score, a.strategy_source,
                               COALESCE(a.trend_score,0), COALESCE(a.reversion_score,0), COALESCE(a.breakout_score,0), COALESCE(a.momentum_score,0),
                               COALESCE(a.technical_score_percentile, 0),
                               a.entry_reference_price, a.stop_loss_price, a.take_profit_json{scan_sel_eval},
                               a.passed, a.created_at
                        {scan_from}
                        {scan_wh}
                        ORDER BY a.created_at DESC, a.technical_score DESC
                        LIMIT 30
                        """,
                        scan_qp,
                    )
                    scan_detail = cur.fetchall()
                    scan_has_pool = True
                    scan_has_percentile = True
                except Exception:
                    scan_has_percentile = False
                    try:
                        cur.execute(
                            f"""
                            SELECT a.symbol, {scan_nm}, a.technical_score, a.strategy_source,
                                   a.entry_reference_price, a.stop_loss_price, a.take_profit_json{scan_sel_eval},
                                   a.passed, a.created_at
                            {scan_from}
                            {scan_wh}
                            ORDER BY a.created_at DESC, a.technical_score DESC
                            LIMIT 30
                            """,
                            scan_qp,
                        )
                        scan_detail = cur.fetchall()
                        scan_has_pool = False
                    except Exception:
                        _sc_leg_nm = (
                            "COALESCE(NULLIF(TRIM(sn.name_cn), ''), '')"
                            if join_sn
                            else "''"
                        )
                        _sc_leg_fr = (
                            "FROM quant_signal_scan_all a "
                            "LEFT JOIN symbol_names sn ON sn.symbol = a.symbol"
                            if join_sn
                            else "FROM quant_signal_scan_all a"
                        )
                        cur.execute(
                            f"""
                            SELECT a.symbol, {_sc_leg_nm}, a.technical_score, a.strategy_source,
                                   a.entry_reference_price, a.stop_loss_price, a.take_profit_json{scan_sel_eval},
                                   a.passed, a.created_at
                            {_sc_leg_fr}
                            {scan_wh}
                            ORDER BY a.created_at DESC, a.technical_score DESC
                            LIMIT 30
                            """,
                            scan_qp,
                        )
                        scan_detail = cur.fetchall()
                        scan_has_pool = False
                    scan_has_percentile = False

                if skip_focused:
                    scan_detail = []

                print()
                if scan_batch_filter:
                    print(
                        "▶ [4] 全量表明细（仅 batch_id=%s…；与 [2] 同批；最多 30 条）"
                        % (str(scan_batch_filter)[:12],)
                    )
                else:
                    print(
                        "▶ [4] 全量表明细（跨批次按写入时间倒序，同批内总分降序；最多 30 条，便于快速扫榜）"
                    )
                print("  说明：可能出现同一写入时刻下多标的多行，属同一批次快照；与 [2] 不同在于含未通过标的。")
                print("  " + _rule("─", RULE_SNAP_SCAN))
                if scan_has_pool:
                    if scan_has_eval_cols:
                        scan_hdr = (
                            "标的",
                            "简称",
                            "总分",
                            "策略",
                            "趋势",
                            "反转",
                            "突破",
                            "动量",
                            "分位",
                            "入场参考",
                            "止损",
                            "止盈",
                            "止损%",
                            "止盈1%",
                            "止盈2%",
                            "来源",
                            "规则指纹",
                            "是否通过",
                            "写入时间",
                        )
                    else:
                        scan_hdr = (
                            "标的",
                            "简称",
                            "总分",
                            "策略",
                            "趋势",
                            "反转",
                            "突破",
                            "动量",
                            "分位",
                            "入场参考",
                            "止损",
                            "止盈",
                            "止损%",
                            "止盈1%",
                            "止盈2%",
                            "是否通过",
                            "写入时间",
                        )
                    _print_table_row(scan_hdr, W_SCAN)
                    print("  " + _rule("─", RULE_SNAP_SCAN))
                    for row in scan_detail:
                        sev = sfp = None
                        if scan_has_eval_cols and len(row) >= 16:
                            (
                                sym,
                                sym_name,
                                score,
                                src,
                                t,
                                rv,
                                br,
                                m,
                                pct,
                                entry,
                                stop,
                                tpj,
                                sev,
                                sfp,
                                passed,
                                created,
                            ) = row
                        elif scan_has_percentile and len(row) >= 14:
                            (
                                sym,
                                sym_name,
                                score,
                                src,
                                t,
                                rv,
                                br,
                                m,
                                pct,
                                entry,
                                stop,
                                tpj,
                                passed,
                                created,
                            ) = row
                        elif len(row) >= 11:
                            sym, sym_name, score, src, t, rv, br, m, pct, passed, created = row[:11]
                            entry, stop, tpj = None, None, None
                        else:
                            sym, sym_name, score, src, t, rv, br, m, passed, created = row[:10]
                            pct = 0.0
                            entry, stop, tpj = None, None, None
                        strat = _strategy_display(src)
                        disp_nm = _fill_short_name(sym, sym_name, csv_names)
                        sl_pct_s, tp1_pct_s, tp2_pct_s = _risk_pct_cells(entry, stop, tpj)
                        scan_src = (
                            "沿用"
                            if (sev is not None and str(sev).upper().strip() == "CARRYOVER")
                            else "重算"
                        )
                        scan_fp = _truncate_disp(str(sfp or ""), 10) if scan_has_eval_cols else ""
                        scan_cells_core = (
                            sym or "",
                            disp_nm,
                            "%.2f" % (score or 0),
                            strat,
                            "%.2f" % (t or 0),
                            "%.2f" % (rv or 0),
                            "%.2f" % (br or 0),
                            "%.2f" % (m or 0),
                            "%.2f" % (pct or 0),
                            _fmt_price_cell(entry),
                            _fmt_price_cell(stop),
                            _fmt_tp_json_cell(tpj),
                            sl_pct_s,
                            tp1_pct_s,
                            tp2_pct_s,
                        )
                        if scan_has_eval_cols:
                            scan_cells = scan_cells_core + (
                                scan_src,
                                scan_fp,
                                "是" if passed else "否",
                                _format_time_short(created),
                            )
                            scan_aligns = [
                                "l",
                                "l",
                                "r",
                                "l",
                                "r",
                                "r",
                                "r",
                                "r",
                                "r",
                                "r",
                                "r",
                                "r",
                                "r",
                                "r",
                                "l",
                                "l",
                                "l",
                                "l",
                            ]
                        else:
                            scan_cells = scan_cells_core + (
                                "是" if passed else "否",
                                _format_time_short(created),
                            )
                            scan_aligns = [
                                "l",
                                "l",
                                "r",
                                "l",
                                "r",
                                "r",
                                "r",
                                "r",
                                "r",
                                "r",
                                "r",
                                "r",
                                "r",
                                "r",
                                "l",
                                "l",
                            ]
                        _print_table_row(scan_cells, W_SCAN, scan_aligns)
                else:
                    w2 = (11, 8, 8, 12, 10, 10, 26, 8, 8, 8, 8, 16)
                    _print_table_row(
                        (
                            "标的",
                            "简称",
                            "总分",
                            "策略",
                            "入场参考",
                            "止损",
                            "止盈",
                            "止损%",
                            "止盈1%",
                            "止盈2%",
                            "是否通过",
                            "写入时间",
                        ),
                        w2,
                    )
                    print("  " + _rule("─", RULE_SNAP_SCAN))
                    for row in scan_detail:
                        if len(row) >= 9:
                            sym, sym_name, score, src, entry, stop, tpj, passed, created = row[:9]
                        elif len(row) >= 6:
                            sym, sym_name, score, src, passed, created = row
                            entry, stop, tpj = None, None, None
                        else:
                            sym, score, src, passed, created = row
                            sym_name = ""
                            entry, stop, tpj = None, None, None
                        _st2 = _strategy_display(src)
                        if len(_st2) > 12:
                            _st2 = _truncate_disp(_st2, 12)
                        disp_nm = _fill_short_name(sym, sym_name, csv_names)
                        sl_pct_s, tp1_pct_s, tp2_pct_s = _risk_pct_cells(entry, stop, tpj)
                        _print_table_row(
                            (
                                sym or "",
                                disp_nm,
                                "%.2f" % (score or 0),
                                _st2,
                                _fmt_price_cell(entry),
                                _fmt_price_cell(stop),
                                _fmt_tp_json_cell(tpj),
                                sl_pct_s,
                                tp1_pct_s,
                                tp2_pct_s,
                                "是" if passed else "否",
                                _format_time_short(created),
                            ),
                            w2,
                        )
            else:
                print("  （空）")
        except Exception as e:
            if "quant_signal_scan_all" in str(e) or "does not exist" in str(e).lower():
                print()
                print("▶ [3] 全量表 quant_signal_scan_all")
                print("  表未创建：请 make init-l2-quant-signal-table 后重新 run-module-b")
            else:
                raise

        cur.close()
        conn.close()
        print()
        print(_rule("═", 88))
        print(
            "  提示：[2]「批次简码」为批次号前 8 位加 …；完整批次号见 [1][3] 汇总表最后一列。"
            " 按日筛选：make query-module-b-output 3-22（或 QUERY_SCANNER_DATE=2026-03-22）；"
            "当日第几批：QUERY_SCANNER_BATCH_INDEX=0（默认最晚）1=倒数第二；时区：QUERY_SCANNER_TZ（默认 Asia/Shanghai）。"
        )
        print()
    except Exception as e:
        print()
        print("查询失败（PG_L2_DSN 不可达或表不存在）: %s" % e)
        print("请确认 L2 可达，并已 init-l2-quant-signal-table、run-module-b。")
        print()
        sys.exit(0)


if __name__ == "__main__":
    main()
