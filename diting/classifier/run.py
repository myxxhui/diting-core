# [Ref: 01_语义分类器] [Ref: 09_核心模块架构规约] Module A 运行入口
# 供 Docker/K8s 部署：从环境变量读取 TIMESCALE_DSN、PG_L2_DSN，执行全量分类（run_full）
# 可选 RUN_LOOP=1 时每 24h 执行一次，供长期运行 Deployment
# 执行结果当前仅输出到 stdout（kubectl logs）；若需 Module B 在独立部署时消费，需将结果写入 L2 表或共享存储（待实现）

import logging
import os
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DomainTag 枚举到中文名，便于日志可读；5=DOMAIN_CUSTOM 时以 tag.domain_label 为准
_DOMAIN_TAG_NAMES = {0: "未指定", 1: "农业", 2: "科技", 3: "周期", 4: "未知", 5: ""}


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


def run_once() -> int:
    """执行一次分类：支持 MODULE_AB_SYMBOLS 指定股票，否则 get_current_a_share_universe() 全量。"""
    _load_env()
    from diting.classifier import SemanticClassifier
    from diting.universe import get_current_a_share_universe, parse_symbol_list_from_env

    # 与采集共用一套指定股票：DITING_SYMBOLS；未设置时再读 MODULE_AB_SYMBOLS
    symbols_env_set = bool((os.environ.get("DITING_SYMBOLS") or "").strip() or (os.environ.get("MODULE_AB_SYMBOLS") or "").strip())
    universe = parse_symbol_list_from_env("DITING_SYMBOLS") or parse_symbol_list_from_env("MODULE_AB_SYMBOLS")
    if universe:
        logger.info("指定股票模式：本批共 %s 只（来源：DITING_SYMBOLS / MODULE_AB_SYMBOLS）", len(universe))
        # 输出本批执行标的（前 10 只 + 省略），便于核对
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
                    logger.warning("从库获取全 A 股标的失败，使用空列表或标样: %s", e)
            if not universe:
                universe = ["000998.SZ", "688981.SH", "601899.SH", "999999.SZ"]
                logger.info("使用标样标的列表: %s", universe)
            if universe:
                logger.info("本批执行标的：来自 L1 全量或标样，共 %s 只", len(universe))

    # 有 L2 时对每只标的从 L2 查行业/营收（一次批量查库）；L2 行业名为空时用 config/industry_fallback.csv 补全，使 Module A 能输出非「未知」
    industry_provider = None
    if os.environ.get("PG_L2_DSN") and universe:
        try:
            from diting.classifier.l2_provider import get_l2_industry_revenue_batch
            from diting.ingestion.industry_revenue import _load_industry_fallback
            l2_data = get_l2_industry_revenue_batch(os.environ["PG_L2_DSN"], universe)
            missing = ("未知", 0.0, 0.0, 0.0)
            merged = {}
            for s in universe:
                key = (s or "").strip().upper()
                t = l2_data.get(key, ("", 0.0, 0.0, 0.0))
                if (t[0] or "").strip():
                    merged[key] = t
                else:
                    iname = _load_industry_fallback(s) or "未知"
                    merged[key] = (iname, float(t[1] or 0), float(t[2] or 0), float(t[3] or 0))
            industry_provider = lambda s: merged.get((s or "").strip().upper(), missing)
            n_fallback = sum(1 for s in universe if not (l2_data.get((s or "").strip().upper(), ("", 0, 0, 0))[0] or "").strip())
            logger.info("使用 L2 行业/营收批量数据，覆盖 %s/%s 只标的%s", len(l2_data), len(universe), "（其中行业名为空已用 industry_fallback 补全 %s 只）" % n_fallback if n_fallback else "")
        except Exception as e:
            logger.warning("L2 行业数据未启用，回退 Mock: %s", e)

    results = SemanticClassifier.run_full(
        universe=universe,
        industry_revenue_provider=industry_provider,
    )
    logger.info("语义分类器本批完成：执行 %s 只，输出 %s 条", len(universe), len(results))
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
