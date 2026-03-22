# [Ref: 02_量化扫描引擎_实践] 标的中文名：优先 L2，其次静态文件，缺失时用东方财富个股接口（与 K 线同源 push2）逐只拉「股票简称」写入
# 默认不再使用 akshare 沪深全表 stock_info_a_code_name（大响应易 IncompleteRead）；可用 INGEST_SYMBOL_NAMES_MODE=akshare_bulk 强制启用

import os
import time
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


def symbols_missing_name_cn(dsn: str, symbols: List[str]) -> List[str]:
    """
    返回在 L2 symbol_names 中尚无非空中文名的标的（顺序与 symbols 一致，去重保留首次出现）。
    用于采集 ①.5：已齐全则跳过，仅对缺口拉取并写入。
    """
    if not symbols:
        return []
    seen = set()
    ordered_unique: List[str] = []
    for s in symbols:
        if s not in seen:
            seen.add(s)
            ordered_unique.append(s)
    if not dsn:
        return ordered_unique
    existing = _load_from_db(dsn, ordered_unique)
    return [sym for sym in ordered_unique if not (existing.get(sym) or "").strip()]


def _ingest_symbol_name_timeout_sec() -> float:
    raw = (os.environ.get("INGEST_REQUESTS_TIMEOUT") or "90").strip()
    try:
        return float(max(10, min(300, int(raw))))
    except ValueError:
        return 90.0


def _name_cn_from_eastmoney_individual(symbol_full: str, timeout: Optional[float] = None) -> Optional[str]:
    """东方财富 push2 个股接口，与 K 线同源；返回股票简称。"""
    code = (symbol_full or "").strip().split(".")[0].strip()
    if not code:
        return None
    t = timeout if timeout is not None else _ingest_symbol_name_timeout_sec()
    try:
        import akshare as ak

        df = ak.stock_individual_info_em(symbol=code, timeout=t)
        if df is None or df.empty:
            return None
        for _, row in df.iterrows():
            if str(row.get("item", "")).strip() == "股票简称":
                v = row.get("value")
                if v is not None and str(v).strip():
                    return str(v).strip()[:128]
    except Exception as e:
        logger.debug("东方财富个股信息 %s: %s", symbol_full, e)
    return None


def _fetch_names_eastmoney_individual(symbols: List[str]) -> Dict[str, str]:
    """按标的逐只请求东方财富个股信息，体量小、与采集 OHLCV 同域名，避免上交所全表大响应断连。"""
    out: Dict[str, str] = {}
    if not symbols:
        return out
    try:
        raw_pause = (os.environ.get("INGEST_SYMBOL_NAME_PAUSE_SEC") or "0.2").strip()
        pause = max(0.0, min(2.0, float(raw_pause)))
    except ValueError:
        pause = 0.2
    timeout = _ingest_symbol_name_timeout_sec()
    n = len(symbols)
    logger.info("标的中文名: 东方财富个股接口逐只拉取（共 %s 只，间隔约 %ss）…", n, pause)
    for i, sym in enumerate(symbols):
        name = _name_cn_from_eastmoney_individual(sym, timeout=timeout)
        if name:
            out[sym] = name
        if (i + 1) % 25 == 0 or i + 1 == n:
            logger.info("标的中文名: 进度 %s/%s，已成功 %s 条", i + 1, n, len(out))
        if pause and i + 1 < n:
            time.sleep(pause)
    logger.info("东方财富个股接口: 解析到中文名 %s/%s 条", len(out), n)
    return out


def _save_to_db(dsn: str, symbol_to_name: Dict[str, str], source: str = "eastmoney_em") -> None:
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


def load_symbol_names_csv_only(
    root: Optional[Path] = None,
    names_csv: str = "config/symbol_names.csv",
) -> Dict[str, str]:
    """
    仅从 symbol_names.csv 读取 symbol -> name_cn（不合并 diting_symbols.txt）。
    供 make sync-symbol-names-csv 与 L2 对齐。
    """
    root = root or Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    out: Dict[str, str] = {}
    csv_path = root / names_csv
    if not csv_path.exists():
        logger.warning("未找到 %s", csv_path)
        return out
    try:
        with open(csv_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(",", 1)
                if len(parts) >= 2:
                    sym, name = parts[0].strip(), parts[1].strip()
                    if sym and name.lower() != "name_cn" and name:
                        out[sym] = name[:128]
    except Exception as e:
        logger.warning("读取 %s 失败: %s", csv_path, e)
    return out


def _fetch_from_akshare(symbols: List[str]) -> Dict[str, str]:
    """从 akshare（东方财富等）拉取 A 股代码-名称，返回 symbol（带 .SH/.SZ）-> name。"""
    out: Dict[str, str] = {}
    n_want = len(symbols) if symbols else 0
    try:
        import akshare as ak
    except ImportError:
        logger.warning("akshare 未安装，无法拉取标的中文名")
        return out
    try:
        logger.info(
            "akshare 标的中文名: 请求 stock_info_a_code_name（东方财富），待匹配标的 %s 只…",
            n_want,
        )
        df = ak.stock_info_a_code_name()
        if df is None or df.empty:
            logger.warning("akshare 标的中文名: 接口返回空表")
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
        logger.info(
            "akshare 标的中文名: 全表 %s 行，有效代码 %s 个，匹配当前标的池 %s 条",
            len(df),
            len(code_to_name),
            len(out),
        )
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
    获取标的中文名：优先 L2，缺失时静态文件，再缺失时东方财富个股接口（或与 INGEST_SYMBOL_NAMES_MODE=akshare_bulk 全表）并写入 L2。
    :param skip_akshare: 为 True 时不调远程接口，仅 DB + 静态
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
        mode = (os.environ.get("INGEST_SYMBOL_NAMES_MODE") or "eastmoney").strip().lower()
        if mode == "akshare_bulk":
            fetched = _fetch_from_akshare(still_missing)
            src = "akshare"
        else:
            fetched = _fetch_names_eastmoney_individual(still_missing)
            src = "eastmoney_em"
        if fetched and dsn:
            _save_to_db(dsn, fetched, source=src)
        for k, v in fetched.items():
            out[k] = v

    return out


def fill_names_from_akshare(symbol_to_name: Dict[str, str], symbols: Optional[List[str]] = None) -> None:
    """
    用远程接口补全缺失的中文名（就地修改）；默认走东方财富个股接口。
    """
    to_fill = list(symbol_to_name.keys()) if symbols is None else symbols
    missing = [s for s in to_fill if not symbol_to_name.get(s)]
    if not missing:
        return
    mode = (os.environ.get("INGEST_SYMBOL_NAMES_MODE") or "eastmoney").strip().lower()
    if mode == "akshare_bulk":
        fetched = _fetch_from_akshare(missing)
    else:
        fetched = _fetch_names_eastmoney_individual(missing)
    for k, v in fetched.items():
        symbol_to_name[k] = v
