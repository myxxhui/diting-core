#!/usr/bin/env python3
# 从 akshare 拉取「有色金属」「电力」两板块全部成分股，规范化为 .SH/.SZ，替换 config/diting_symbols.txt 中对应段落；
# 龙头标的用行尾「# 龙头」标出。依赖：需先 make deps-ingest。用法：make fetch-sector-symbols

import sys
from pathlib import Path

# 保证可导入 diting
root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from diting.universe import normalize_symbol

# 龙头子集（用于在全部成分股中做行尾标记 # 龙头）
LEADER_SYMBOLS = frozenset({
    normalize_symbol(s) for s in [
        "600362", "000630", "601600", "000807", "600219", "600497", "000060", "600961", "000960", "600301",
        "603993", "600711", "002340", "600456", "600547", "600489", "000603", "000426", "600459",
        "002460", "002466", "000792", "603799", "688005", "300073", "600111", "600549", "601899",
        "600900", "600011", "600795", "601985", "600905", "000543", "001896", "601991", "000767", "600886",
    ]
})


def fetch_sector_constituents_akshare(sector_name: str):
    """从东方财富行业板块拉取全部成分股，返回 [(code, name), ...]，code 仅数字部分。"""
    try:
        import akshare as ak
        df = ak.stock_board_industry_cons_em(symbol=sector_name)
        if df is None or df.empty:
            return []
        code_col = "代码" if "代码" in df.columns else df.columns[0]
        name_col = "名称" if "名称" in df.columns else (df.columns[1] if len(df.columns) > 1 else None)
        out = []
        for _, row in df.iterrows():
            code = str(row.get(code_col, "")).strip()
            if not code or not code.isdigit():
                continue
            name = str(row.get(name_col, "")).strip() if name_col else ""
            out.append((code, name))
        return out
    except ImportError as e:
        print("拉取板块 %s 失败: 未安装 akshare，请先执行 make deps-ingest。%s" % (sector_name, e), file=sys.stderr)
        return []
    except Exception as e:
        print("拉取板块 %s 失败: %s" % (sector_name, e), file=sys.stderr)
        return []


def _find_sector_block_start(lines: list) -> int:
    """返回「有色金属/电力」段落起始行下标（含该行）；若无则返回 len(lines)。"""
    for i, line in enumerate(lines):
        s = line.strip()
        if "东方财富板块成分股" in s or "有色金属 全部" in s or (i > 0 and "有色金属（" in s and "龙头" in s):
            return i
    return len(lines)


def main():
    repo_root = Path(__file__).resolve().parents[1]
    symbols_file = repo_root / "config" / "diting_symbols.txt"
    if not symbols_file.exists():
        print("未找到 %s" % symbols_file, file=sys.stderr)
        return 1

    with open(symbols_file, encoding="utf-8") as f:
        all_lines = [line.rstrip("\n") for line in f]

    head_end = _find_sector_block_start(all_lines)
    head_lines = all_lines[:head_end]
    # 去掉 head 末尾多余空行，保留一个
    while len(head_lines) > 0 and not head_lines[-1].strip():
        head_lines.pop()
    if head_lines:
        head_lines.append("")

    # 拉取两板块全部成分股
    list_ys = []  # 有色金属：[(code, name), ...] -> 规范化 symbol 列表
    list_dl = []
    for code, name in fetch_sector_constituents_akshare("有色金属"):
        sym = normalize_symbol(code)
        if sym:
            list_ys.append(sym)
    for code, name in fetch_sector_constituents_akshare("电力"):
        sym = normalize_symbol(code)
        if sym:
            list_dl.append(sym)

    if not list_ys and not list_dl:
        print("未拉取到任何成分股（请确认已安装 akshare：make deps-ingest）", file=sys.stderr)
        return 1

    print("有色金属: 共 %s 只（其中龙头 %s 只）" % (len(list_ys), sum(1 for s in list_ys if s in LEADER_SYMBOLS)))
    print("电力: 共 %s 只（其中龙头 %s 只）" % (len(list_dl), sum(1 for s in list_dl if s in LEADER_SYMBOLS)))

    # 构建新段落：两板块全部标的，龙头行尾加「  # 龙头」
    block = []
    block.append("# 以下为东方财富板块成分股（运行 make fetch-sector-symbols 可刷新）；龙头已用行尾 # 龙头 标出")
    block.append("# 有色金属 全部")
    for sym in list_ys:
        block.append("%s  # 龙头" % sym if sym in LEADER_SYMBOLS else sym)
    block.append("")
    block.append("# 电力 全部")
    for sym in list_dl:
        block.append("%s  # 龙头" % sym if sym in LEADER_SYMBOLS else sym)

    with open(symbols_file, "w", encoding="utf-8") as f:
        f.write("\n".join(head_lines + block) + "\n")

    n_head = len([l for l in head_lines if l.strip() and not l.strip().startswith("#")])
    print("已写入 %s（前段 %s 只 + 有色金属 %s 只 + 电力 %s 只）" % (symbols_file, n_head, len(list_ys), len(list_dl)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
