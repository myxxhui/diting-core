#!/usr/bin/env python3
# 从东方财富拉取行业板块全部成分股（默认六板块：电力、公用事业、能源金属、通信设备、贵金属、航空机场），
# 规范化为 .SH/.SZ，替换 config/diting_symbols.txt 中「东方财富行业板块」段落；并同步写入 config/symbol_names.csv（与名单顺序一致）。
# 数据源：push2delay.eastmoney.com clist API（与 akshare stock_board_industry_cons_em 一致）；已排除沪深 B 股。
# 依赖：requests（make deps-ingest 已含）。用法：make fetch-sector-symbols

import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

import requests

from diting.universe import normalize_symbol

# 东方财富行业板块代码（名称见 data.eastmoney.com/bkzj/BKxxxx.html）
EM_SECTOR_BOARDS = [
    ("BK0428", "电力"),
    ("BK0427", "公用事业"),
    ("BK1015", "能源金属"),
    ("BK0448", "通信设备"),
    ("BK0732", "贵金属"),
    ("BK0420", "航空机场"),
]

# 龙头子集（行尾「# 龙头」）
LEADER_SYMBOLS = frozenset({
    normalize_symbol(s) for s in [
        "600362", "000630", "601600", "000807", "600219", "600497", "000060", "600961", "000960", "600301",
        "603993", "600711", "002340", "600456", "600547", "600489", "000603", "000426", "600459",
        "002460", "002466", "000792", "603799", "688005", "300073", "600111", "600549", "601899",
        "600900", "600011", "600795", "601985", "600905", "000543", "001896", "601991", "000767", "600886",
        "600027", "600023", "003816", "002039", "000993", "600021", "600995", "600674", "600578", "600969",
        "600163", "000600", "600452", "600310", "600780", "600863", "600644", "600719", "600116", "000539",
        "000531", "600131", "600505", "601016", "601619", "603507", "605028", "600982", "600483", "001258",
        "002015", "002665", "300125", "300617", "600167", "000722", "000875", "600868", "600396", "600744",
        "000966", "600292", "600098", "600649", "000037", "600509", "600101",
        "000063", "601138", "688012", "300308", "300502", "002281", "600522", "600487", "002916", "300394",
        "603893", "688498", "688100", "300627", "603236", "300570", "300136", "002463", "300548",
        "600029", "601111", "600115", "600221", "002928", "603885",
    ]
})

EM_CLIST_URL = "https://push2delay.eastmoney.com/api/qt/clist/get"


def _is_cnh_a(sym: str) -> bool:
    """排除沪深 B 股（东财行业成分偶含 B 股）。"""
    code = sym.split(".")[0]
    if sym.endswith(".SH") and code.startswith("900"):
        return False
    if sym.endswith(".SZ") and code.startswith("200"):
        return False
    return True


def fetch_board_constituents(bk: str):
    """
    拉取单板块全部成分股（分页直至累计条数达 total）。
    返回 [(code6, name), ...]（已排除 B 股）。
    """
    raw_pairs = []
    pn = 1
    pz = 200
    total = None
    while True:
        r = requests.get(
            EM_CLIST_URL,
            params={
                "pn": str(pn),
                "pz": str(pz),
                "po": "1",
                "np": "1",
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": "2",
                "invt": "2",
                "fid": "f3",
                "fs": "b:%s f:!50" % bk,
                "fields": "f12,f14",
            },
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=(15, 90),
        )
        r.raise_for_status()
        data = r.json()
        diff = (data.get("data") or {}).get("diff") or []
        if total is None:
            total = int((data.get("data") or {}).get("total") or 0)
        for row in diff:
            code = str(row.get("f12", "")).strip()
            name = str(row.get("f14", "")).strip()
            if code.isdigit() and name:
                raw_pairs.append((code, name))
        if not diff:
            break
        if total and len(raw_pairs) >= total:
            break
        pn += 1

    out = []
    for code, name in raw_pairs:
        sym = normalize_symbol(code)
        if _is_cnh_a(sym):
            out.append((code, name))
    return out


def _find_sector_block_start(lines: list) -> int:
    """返回东方财富板块段落起始行下标（含该行）；若无则返回 len(lines)。"""
    for i, line in enumerate(lines):
        if "东方财富" in line and ("板块成分股" in line or "行业板块" in line):
            return i
    return len(lines)


def _load_csv_names(path: Path) -> dict:
    d = {}
    if not path.exists():
        return d
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(",", 1)
        if len(parts) != 2:
            continue
        sym, name = parts[0].strip(), parts[1].strip()
        if sym and name and sym != "symbol":
            d[sym] = name
    return d


def main():
    repo_root = Path(__file__).resolve().parents[1]
    symbols_file = repo_root / "config" / "diting_symbols.txt"
    names_csv = repo_root / "config" / "symbol_names.csv"
    if not symbols_file.exists():
        print("未找到 %s" % symbols_file, file=sys.stderr)
        return 1

    all_lines = [line.rstrip("\n") for line in symbols_file.read_text(encoding="utf-8").splitlines()]
    head_end = _find_sector_block_start(all_lines)
    head_lines = all_lines[:head_end]
    while len(head_lines) > 0 and not head_lines[-1].strip():
        head_lines.pop()

    head_syms = []
    for line in head_lines:
        stripped = line.strip().split("#")[0].strip()
        if stripped and not stripped.startswith("#"):
            head_syms.append(normalize_symbol(stripped))

    sector_blocks = []
    name_by_sym = _load_csv_names(names_csv)
    ordered_tail = []
    seen_tail = set()

    for bk, label in EM_SECTOR_BOARDS:
        rows = fetch_board_constituents(bk)
        if not rows:
            print("警告: 板块 %s(%s) 未拉取到成分股" % (label, bk), file=sys.stderr)
        block_lines = [
            "# %s 全部（东方财富行业 %s，成分股见数据中心 bkzj/%s）" % (label, bk, bk),
        ]
        for code, name in rows:
            sym = normalize_symbol(code)
            name_by_sym[sym] = name
            if sym not in seen_tail:
                seen_tail.add(sym)
                ordered_tail.append(sym)
            block_lines.append("%s%s" % (sym, "  # 龙头" if sym in LEADER_SYMBOLS else ""))
        sector_blocks.append("\n".join(block_lines))
        print("%s(%s): %s 只（龙头 %s 只）" % (
            label, bk, len(rows), sum(1 for code, _ in rows if normalize_symbol(code) in LEADER_SYMBOLS)))

    if not ordered_tail:
        print("未拉取到任何成分股，请检查网络或稍后重试。", file=sys.stderr)
        return 1

    block_intro = (
        "# 以下为东方财富行业板块成分股（本脚本可刷新）；龙头已用行尾 # 龙头 标出；"
        "数据源：东方财富 push2delay clist；已排除沪深 B 股"
    )
    head_text = "\n".join(head_lines).rstrip() + "\n\n" if head_lines else ""
    new_txt = head_text + block_intro + "\n" + "\n\n".join(sector_blocks) + "\n"
    symbols_file.write_text(new_txt, encoding="utf-8")

    out_order = []
    seen = set()
    for s in head_syms:
        if s not in seen:
            seen.add(s)
            out_order.append(s)
    for s in ordered_tail:
        if s not in seen:
            seen.add(s)
            out_order.append(s)

    csv_lines = [
        "# 标的中文名：与 diting_symbols.txt 顺序一致，供 B 模块与 L2 展示；采集/akshare 失败时兜底",
        "# name_cn 为交易所常用股票简称；六板块成分股由 make fetch-sector-symbols 从东方财富同步，公司更名请核对后更新",
        "symbol,name_cn",
    ]
    for sym in out_order:
        name = name_by_sym.get(sym, sym)
        csv_lines.append("%s,%s" % (sym, name))
    names_csv.write_text("\n".join(csv_lines) + "\n", encoding="utf-8")

    n_head = len(head_syms)
    print("已写入 %s（前段 %s 只 + 六板块并集 %s 只）" % (symbols_file, n_head, len(ordered_tail)))
    print("已写入 %s（共 %s 行数据）" % (names_csv, len(out_order)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
