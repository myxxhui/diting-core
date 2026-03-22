# [Ref: 01_语义分类器] [Ref: 09_核心模块架构规约] Module A 运行入口
# 供 Docker/K8s 部署：从环境变量读取 TIMESCALE_DSN、PG_L2_DSN，执行全量分类（run_full）
# 可选 RUN_LOOP=1 时每 24h 执行一次，供长期运行 Deployment
# 分类完成后将 ClassifierOutput 写入 L2 表 classifier_output_snapshot，供 Module B 按约定读取

import logging
import os
import time
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DomainTag 枚举到展示名（与 L2 primary_tag、过滤展示一致，以中文为主）
_DOMAIN_TAG_NAMES = {0: "未指定", 1: "农业", 2: "科技", 3: "宏观", 4: "未知", 5: ""}


def _load_env() -> None:
    """从 .env 或环境变量加载 DSN（K8s 下由 Secret 注入，无需 .env 文件）。"""
    env_path = os.path.join(os.environ.get("DITING_CORE_ROOT", "."), ".env")
    if os.path.isfile(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    k, v = k.strip(), v.strip().strip('"').strip("'")
                    if k and os.environ.get(k) is None:
                        os.environ[k] = v


def _default_universe_from_diting_symbols():
    """默认按 config/diting_symbols.txt 全部标的（与采集模块一致）。"""
    from pathlib import Path
    root = Path(os.environ.get("DITING_CORE_ROOT", "."))
    if not root.is_absolute():
        root = Path.cwd() / root
    path = root / "config" / "diting_symbols.txt"
    if not path.exists():
        return None
    from diting.universe import normalize_symbol
    symbols = []
    seen = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip().split("#")[0].strip()
            if line:
                sym = normalize_symbol(line)
                if sym and sym not in seen:
                    seen.add(sym)
                    symbols.append(sym)
    return symbols if symbols else None


def run_once() -> int:
    """执行一次分类：默认按 config/diting_symbols.txt 全部标的（与采集模块一致）；也可由 DITING_SYMBOLS/MODULE_AB_SYMBOLS 指定。"""
    _load_env()
    from diting.classifier import SemanticClassifier
    from diting.universe import get_current_a_share_universe, parse_symbol_list_from_env

    # 与采集模块一致：优先 DITING_SYMBOLS / MODULE_AB_SYMBOLS；未设置时默认 config/diting_symbols.txt 全部标的
    symbols_env_set = bool((os.environ.get("DITING_SYMBOLS") or "").strip() or (os.environ.get("MODULE_AB_SYMBOLS") or "").strip())
    universe = parse_symbol_list_from_env("DITING_SYMBOLS") or parse_symbol_list_from_env("MODULE_AB_SYMBOLS")
    if not universe and not symbols_env_set:
        universe = _default_universe_from_diting_symbols()
        if universe:
            logger.info("默认标的：config/diting_symbols.txt 共 %s 只（与采集模块一致）", len(universe))
    if universe:
        if symbols_env_set:
            logger.info("指定股票模式：本批共 %s 只（来源：DITING_SYMBOLS / MODULE_AB_SYMBOLS）", len(universe))
        show = universe[:10]
        if len(universe) > 10:
            logger.info("本批执行标的：%s ... 等共 %s 只", "、".join(show), len(universe))
        else:
            logger.info("本批执行标的：%s", "、".join(universe))
    else:
        if symbols_env_set:
            logger.error(
                "已设置 DITING_SYMBOLS/MODULE_AB_SYMBOLS 但解析结果为空（文件为空、仅注释或路径错误），不使用 Mock。"
                " 请检查标的列表文件（部署前执行 make sync-semantic-classifier-a-symbols 或复制 diting-core/config/diting_symbols.txt）"
            )
            universe = []
        else:
            universe = None
            if os.environ.get("TIMESCALE_DSN"):
                try:
                    universe = get_current_a_share_universe()
                except Exception as e:
                    logger.warning("从库获取全 A 股标的失败: %s", e)
            if not universe:
                universe = ["000998.SZ", "688981.SH", "601899.SH", "999999.SZ"]
                logger.info("未找到 config/diting_symbols.txt 且无法读 L1，使用 fallback 标样: %s", universe)
            if universe:
                logger.info("本批执行标的：共 %s 只", len(universe))

    # 有 L2 时对每只标的从 L2 查行业/营收（一次批量查库）；L2 行业名为空时用 config/industry_fallback.csv 补全，使 Module A 能输出非「未知」
    industry_provider = None
    business_segment_provider = None
    segment_top1_name_provider = None
    segment_disclosure_names_provider = None
    if os.environ.get("PG_L2_DSN") and universe:
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
            l2_data = get_l2_industry_revenue_batch(os.environ["PG_L2_DSN"], universe)
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
            industry_provider = lambda s: merged.get((s or "").strip().upper(), missing)
            n_fallback = sum(
                1
                for s in universe
                if industry_name_needs_fallback(
                    l2_data.get((s or "").strip().upper(), ("", 0, 0, 0))[0]
                )
            )
            logger.info("使用 L2 行业/营收批量数据，覆盖 %s/%s 只标的%s", len(l2_data), len(universe), "（其中行业名为空已用 industry_fallback 补全 %s 只）" % n_fallback if n_fallback else "")
            business_segment_provider = make_business_segment_provider(
                os.environ["PG_L2_DSN"], universe
            )
            _disc = get_top_segment_disclosure_batch(os.environ["PG_L2_DSN"], universe)
            _names_by_sym = get_segment_disclosure_names_batch(os.environ["PG_L2_DSN"], universe)

            def _segment_top1_name(sym: str):
                row = _disc.get((sym or "").strip().upper())
                if not row:
                    return None
                n = (row[0] or "").strip()
                return n or None

            def _segment_disclosure_names(sym: str):
                return _names_by_sym.get((sym or "").strip().upper(), [])

            segment_top1_name_provider = _segment_top1_name
            segment_disclosure_names_provider = _segment_disclosure_names
        except Exception as e:
            logger.warning("L2 行业数据未启用，回退 Mock: %s", e)

    batch_id = str(uuid.uuid4())
    run_kw = dict(
        universe=universe,
        correlation_id=batch_id,
        industry_revenue_provider=industry_provider,
        business_segment_provider=business_segment_provider,
    )
    if segment_top1_name_provider is not None:
        run_kw["segment_top1_name_provider"] = segment_top1_name_provider
    if segment_disclosure_names_provider is not None:
        run_kw["segment_disclosure_names_provider"] = segment_disclosure_names_provider
    results = SemanticClassifier.run_full(**run_kw)
    logger.info("语义分类器本批完成：执行 %s 只，输出 %s 条", len(universe), len(results))

    # [Ref: 01_语义分类器_实践 F9] ClassifierOutput 写入 L2 表 classifier_output_snapshot，供 Module B 按 batch_id 读取
    if results and os.environ.get("PG_L2_DSN"):
        try:
            from diting.classifier.l2_snapshot_writer import write_classifier_output_snapshot
            n = write_classifier_output_snapshot(
                os.environ["PG_L2_DSN"],
                results,
                batch_id=batch_id,
                correlation_id=batch_id,
            )
            if n:
                logger.info("本批分类结果已写入 L2 classifier_output_snapshot，batch_id=%s，行数=%s", batch_id, n)
        except Exception as e:
            logger.warning("写入 L2 classifier_output_snapshot 失败: %s", e)

    # 分类结果摘要：每条约 symbol -> 领域标签（中文名）+ 置信度
    for out in results[:10]:
        tag_strs = []
        for t in out.tags:
            name = getattr(t, "domain_label", None) or _DOMAIN_TAG_NAMES.get(t.domain_tag, str(t.domain_tag))
            if name:
                tag_strs.append(name)
        conf = out.tags[0].confidence if out.tags else 0.0
        logger.info("  分类结果 %s -> %s（置信度 %.2f）", out.symbol, "、".join(tag_strs) or "未知", conf)
    if len(results) > 10:
        logger.info("  ... 以上为前 10 条，共 %s 条结果（完整输出见 stdout，后续 Module B 消费需落库或共享存储）", len(results))
    return 0


def main() -> int:
    run_loop = os.environ.get("RUN_LOOP", "").strip() == "1"
    interval_sec = int(os.environ.get("RUN_LOOP_INTERVAL_SEC", "86400"))

    if run_loop:
        while True:
            run_once()
            logger.info("下次执行间隔 %s 秒", interval_sec)
            time.sleep(interval_sec)
    return run_once()


if __name__ == "__main__":
    raise SystemExit(main())
