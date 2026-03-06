# [Ref: 01_语义分类器] [Ref: 09_核心模块架构规约] Module A 运行入口
# 供 Docker/K8s 部署：从环境变量读取 TIMESCALE_DSN、PG_L2_DSN，执行全量分类（run_full）
# 可选 RUN_LOOP=1 时每 24h 执行一次，供长期运行 Deployment

import logging
import os
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    """执行一次全量分类：get_current_a_share_universe() -> SemanticClassifier.run_full()。"""
    _load_env()
    from diting.classifier import SemanticClassifier

    universe = None
    if os.environ.get("TIMESCALE_DSN"):
        try:
            from diting.universe import get_current_a_share_universe
            universe = get_current_a_share_universe()
        except Exception as e:
            logger.warning("get_current_a_share_universe 失败，使用空列表或 Mock: %s", e)
    if not universe:
        # 无 L1 或未配置：使用标样列表便于验收
        universe = ["000998.SZ", "688981.SH", "601899.SH", "999999.SZ"]
        logger.info("使用标样 universe: %s", universe)

    # 有 L2 时对每只标的从 L2 查行业/营收（一次批量查库）；无 L2 时用 Mock（仅 3 只示例有数据）
    industry_provider = None
    if os.environ.get("PG_L2_DSN") and universe:
        try:
            from diting.classifier.l2_provider import get_l2_industry_revenue_batch
            l2_data = get_l2_industry_revenue_batch(os.environ["PG_L2_DSN"], universe)
            missing = ("未知", 0.0, 0.0, 0.0)
            industry_provider = lambda s: l2_data.get((s or "").strip().upper(), missing)
            logger.info("使用 L2 industry_revenue_summary 批量数据，覆盖 %s/%s 只标的", len(l2_data), len(universe))
        except Exception as e:
            logger.warning("L2 provider 未启用，回退 Mock: %s", e)

    results = SemanticClassifier.run_full(
        universe=universe,
        industry_revenue_provider=industry_provider,
    )
    logger.info("Module A 分类完成: len(universe)=%s, 输出数=%s", len(universe), len(results))
    for out in results[:5]:
        tags = [t.domain_tag for t in out.tags]
        logger.info("  %s -> %s", out.symbol, tags)
    if len(results) > 5:
        logger.info("  ... 共 %s 条", len(results))
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
