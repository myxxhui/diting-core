# [Ref: 02_量化扫描引擎_实践] 标的中文名：优先 L2 数据库，其次静态文件，缺失时从东方财富(akshare)拉取并写入 L2
# 不再依赖静态文件作为唯一来源；数据库为持久化来源，静态文件仅作兜底

import os
from pathlib import Path
from typing import Dict, List, Optional

logger = __import__("logging").getLogger(__name__)


def _load_from_db(dsn: str, symbols: List[str]) -> Dict[str, str]:
    """从 L2 symbol_names 表读取 symbol -> name_cn，返回已有条目。"""
    if not dsn or not symbols:
        return {}
    out: Dict[str, str] = {}
    try:
        import psycopg2
        conn = psycopg2.connect(dsn, connect_timeout=10)
        cur = conn.cursor()
        cur.execute(
            "SELECT symbol, name_cn FROM symbol_names WHERE symbol = ANY(%s) AND name_cn != ''",
            (list(symbols),),
        )
        for row in cur.fetchall():
            out[row[0]] = (row[1] or "")[:128]
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("从 L2 读取 symbol_names 失败: %s", e)
    return out


def _save_to_db(dsn: str, symbol_to_name: Dict[str, str], source: str = "akshare") -> None:
    """将 symbol -> name 写入 L2 symbol_names 表（UPSERT）。"""
    if not dsn or not symbol_to_name:
        return
    try:
        import psycopg2
        conn = psycopg2.connect(dsn, connect_timeout=10)
        cur = conn.cursor()
        for sym, name in symbol_to_name.items():
            if not name:
                continue
            cur.execute(
                """
                INSERT INTO symbol_names (symbol, name_cn, source, updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (symbol) DO UPDATE SET name_cn = EXCLUDED.name_cn, source = EXCLUDED.source, updated_at = NOW()
                """,
                (sym[:32], name[:128], source[:32]),
            )
        conn.commit()
        cur.close()
        conn.close()
        logger.info("已写入 L2 symbol_names %d 条（source=%s）", len(symbol_to_name), source)
    except Exception as e:
        logger.warning("写入 L2 symbol_names 失败: %s", e)


def load_symbol_names(
    root: Optional[Path] = None,
    symbols_file: str = "config/diting_symbols.txt",
    names_csv: str = "config/symbol_names.csv",
) -> Dict[str, str]:
    """
    仅从静态文件加载 symbol -> 中文名（兜底用）。优先 symbol_names.csv，再 diting_symbols.txt。
    :return: { "600519.SH": "贵州茅台", ... }
    """
    root = root or Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    out: Dict[str, str] = {}

    csv_path = root / names_csv
    if csv_path.exists():
        try:
            with open(csv_path, encoding="utf-8") as f:
                for line in f:
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
                            out[sym] = ""
        except Exception as e:
            logger.warning("解析 %s 失败: %s", txt_path, e)

    return out


def _fetch_from_akshare(symbols: List[str]) -> Dict[str, str]:
    """从 akshare（东方财富等）拉取 A 股代码-名称，返回 symbol（带 .SH/.SZ）-> name。"""
    out: Dict[str, str] = {}
    try:
        import akshare as ak
    except ImportError:
        return out
    try:
        df = ak.stock_info_a_code_name()
        if df is None or df.empty:
            return out
        code_col = "code" if "code" in df.columns else df.columns[0]
        name_col = "name" if "name" in df.columns else (df.columns[1] if len(df.columns) > 1 else df.columns[0])
        code_to_name: Dict[str, str] = {}
        for _, r in df.iterrows():
            code = str(r[code_col]).strip()
            name = str(r[name_col]).strip() if name_col in r else ""
            if code and name:
                code_to_name[code] = name[:128]
        want = set(symbols)
        for sym in want:
            code = sym.replace(".SH", "").replace(".SZ", "").strip()
            if code in code_to_name:
                out[sym] = code_to_name[code]
    except Exception as e:
        logger.warning("akshare 拉取标的中文名失败: %s", e)
    return out


def get_symbol_names(
    symbols: List[str],
    dsn: Optional[str] = None,
    root: Optional[Path] = None,
    symbols_file: str = "config/diting_symbols.txt",
    names_csv: str = "config/symbol_names.csv",
    skip_akshare: bool = False,
) -> Dict[str, str]:
    """
    获取标的中文名：优先从 L2 数据库读，缺失时从静态文件补，再缺失时从东方财富(akshare)拉取并写入数据库。
    不依赖静态文件作为唯一来源；数据库为持久化来源。
    :param symbols: 待解析的 symbol 列表（如 300750.SZ）
    :param dsn: L2 连接串（PG_L2_DSN）；为空则跳过 DB 读/写
    :param root: 项目根路径，用于静态文件
    :param skip_akshare: 为 True 时不调 akshare，仅 DB + 静态
    :return: { symbol: name_cn }，缺名的 symbol 可能不在或对应空串
    """
    out: Dict[str, str] = {}
    want = list(symbols) if symbols else []

    if dsn and want:
        from_db = _load_from_db(dsn, want)
        for k, v in from_db.items():
            if v:
                out[k] = v
    missing_after_db = [s for s in want if not out.get(s)]

    if missing_after_db:
        static = load_symbol_names(root=root, symbols_file=symbols_file, names_csv=names_csv)
        from_static = {s: static[s] for s in missing_after_db if static.get(s)}
        for k, v in from_static.items():
            out[k] = v
        if dsn and from_static:
            _save_to_db(dsn, from_static, source="static")
    still_missing = [s for s in want if not out.get(s)]

    if still_missing and not skip_akshare:
        fetched = _fetch_from_akshare(still_missing)
        if fetched and dsn:
            _save_to_db(dsn, fetched, source="akshare")
        for k, v in fetched.items():
            out[k] = v

    return out


def fill_names_from_akshare(symbol_to_name: Dict[str, str], symbols: Optional[List[str]] = None) -> None:
    """
    用 akshare 补全缺失的中文名（就地修改）。未提供 dsn 时的兼容入口；建议改用 get_symbol_names(dsn=...) 以落库。
    """
    to_fill = list(symbol_to_name.keys()) if symbols is None else symbols
    missing = [s for s in to_fill if not symbol_to_name.get(s)]
    if not missing:
        return
    fetched = _fetch_from_akshare(missing)
    for k, v in fetched.items():
        symbol_to_name[k] = v
