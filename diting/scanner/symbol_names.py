# [Ref: 02_量化扫描引擎_实践] 标的代码 → 中文名，供 L2 写入与终端/查询展示
# 来源：config/symbol_names.csv 或 config/diting_symbols.txt 可选第二列，可选 akshare 补全

import os
from pathlib import Path
from typing import Dict, List, Optional

logger = __import__("logging").getLogger(__name__)


def load_symbol_names(
    root: Optional[Path] = None,
    symbols_file: str = "config/diting_symbols.txt",
    names_csv: str = "config/symbol_names.csv",
) -> Dict[str, str]:
    """
    加载 symbol → 中文名。优先 symbol_names.csv（symbol,name_cn），再解析 diting_symbols.txt 的「symbol,name」或「symbol # name」。
    :return: { "600519.SH": "贵州茅台", ... }
    """
    root = root or Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    out: Dict[str, str] = {}

    # 1) config/symbol_names.csv：symbol,name_cn
    csv_path = root / names_csv
    if csv_path.exists():
        try:
            with open(csv_path, encoding="utf-8") as f:
                for i, line in enumerate(f):
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    parts = line.split(",", 1)
                    if len(parts) >= 2:
                        sym, name = parts[0].strip(), parts[1].strip()
                        if sym and name.lower() != "name_cn":
                            out[sym] = name[:128]
        except Exception as e:
            logger.warning("读取 %s 失败: %s", csv_path, e)

    # 2) config/diting_symbols.txt：支持 "symbol" 或 "symbol,name" 或 "symbol  # name"
    txt_path = root / symbols_file
    if txt_path.exists():
        try:
            with open(txt_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip().split("#")[0].strip()
                    if not line:
                        continue
                    if "," in line:
                        sym, name = line.split(",", 1)
                        sym, name = sym.strip(), name.strip()
                        if sym and name:
                            out[sym] = name[:128]
                    else:
                        sym = line.strip()
                        if sym and sym not in out:
                            out[sym] = ""  # 占位，后续可用 akshare 补全
        except Exception as e:
            logger.warning("解析 %s 失败: %s", txt_path, e)

    return out


def fill_names_from_akshare(symbol_to_name: Dict[str, str], symbols: Optional[List[str]] = None) -> None:
    """
    用 akshare 补全缺失的中文名（就地修改 symbol_to_name）。symbols 为待补全的 symbol 列表，缺省则补全 symbol_to_name 中值为空的 key。
    """
    try:
        import akshare as ak
    except ImportError:
        return
    try:
        df = ak.stock_info_a_code_name()
        if df is None or df.empty:
            return
        code_col = "code" if "code" in df.columns else df.columns[0]
        name_col = "name" if "name" in df.columns else (df.columns[1] if len(df.columns) > 1 else df.columns[0])
        code_to_name = {}
        for _, r in df.iterrows():
            code = str(r[code_col]).strip()
            name = str(r[name_col]).strip() if name_col in r else ""
            if code and name:
                code_to_name[code] = name[:128]
        # symbol 格式 600519.SH -> 600519
        to_fill = list(symbol_to_name.keys()) if symbols is None else symbols
        for sym in to_fill:
            if symbol_to_name.get(sym):
                continue
            code = sym.replace(".SH", "").replace(".SZ", "").strip()
            if code in code_to_name:
                symbol_to_name[sym] = code_to_name[code]
    except Exception as e:
        logger.warning("akshare 补全中文名失败: %s", e)
