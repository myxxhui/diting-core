#!/usr/bin/env python3
# [Ref: 06_B轨_信号层_1对1对1, 05_C模块_输出检测与问题根治最佳实践]
# 全系统全链路：A → B → 信号层 refresh → C 串联执行，一次命令完成全链路运行/测试。
#
# 用法：make run-full-pipeline 或 PYTHONPATH=. python3 scripts/run_full_pipeline.py
# 环境变量：
#   RUN_MODULE_A=1|0     是否执行 A（默认 1）
#   RUN_REFRESH_AFTER_B=1|0  是否在 B 后执行信号层 refresh（默认 1，推荐）
#   RUN_MODULE_C=1|0     是否执行 C（默认 1）
#   DITING_TRACK=a|b     轨（默认 a）；a 时信号层写 a_track_signal_cache（标的新闻+行业新闻双路打标）+观测表；b 时细分→segment_signal_cache
#   MOE_STUB_SEGMENT_SIGNALS=0|1  C 是否用 stub 占位（默认 0；有 refresh 时应用真实数据）
#   PIPELINE_VERBOSE=1  关闭管道精简模式（各子模块完整终端输出；默认不设置则精简）
#   PIPELINE_QUIET=0    显式关闭精简（与 VERBOSE 二选一；未设时由流水线默认注入 QUIET=1）
#   子进程会收到 PIPELINE_STEP / PIPELINE_TOTAL / PIPELINE_TITLE_CN 便于终端区分模块。
#   L2 汇总排查：make query-full-pipeline-result
#
# 工作目录: diting-core

import logging
import os
import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
ROOT = str(root)
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

# 加载 .env
_env = root / ".env"
if _env.exists():
    with open(_env, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and os.environ.get(k) is None:
                    os.environ[k] = v

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

_W = 80


def _banner_step_begin(step: int, total: int, title: str) -> None:
    print()
    print("=" * _W)
    print("  PIPELINE  [%s/%s]  %s" % (step, total, title))
    print("=" * _W)


def _banner_step_end(step: int, total: int, short: str, rc: int) -> None:
    status = "OK" if rc == 0 else "FAIL exit=%s" % rc
    print()
    print("-" * _W)
    print("  PIPELINE  [%s/%s]  %s  —  %s" % (step, total, short, status))
    print("-" * _W)


def _run_step(
    step: int,
    total: int,
    short: str,
    title_cn: str,
    label: str,
    cmd: list,
    cwd: str,
    env: dict,
    *,
    quiet_orchestrator: bool,
) -> int:
    """执行一步；向子进程注入 PIPELINE_STEP/TOTAL/TITLE_CN/LABEL。"""
    step_env = {
        **env,
        "PIPELINE_STEP": str(step),
        "PIPELINE_TOTAL": str(total),
        "PIPELINE_TITLE_CN": title_cn,
        "PIPELINE_LABEL": label,
    }
    if quiet_orchestrator:
        _banner_step_begin(step, total, title_cn)
    else:
        logger.info("run_full_pipeline: 执行 [%s/%s] %s ...", step, total, short)
    rc = subprocess.call(cmd, cwd=cwd, env=step_env)
    if quiet_orchestrator:
        _banner_step_end(step, total, short, rc)
    else:
        if rc != 0:
            logger.error("run_full_pipeline: [%s/%s] %s 退出码 %d", step, total, short, rc)
        else:
            logger.info("run_full_pipeline: [%s/%s] %s 完成", step, total, short)
    return rc


def main() -> int:
    py = os.environ.get("PYTHON_SCANNER") or os.environ.get("PYTHON_INGEST") or sys.executable
    scripts = Path(ROOT) / "scripts"
    env = {**os.environ, "PYTHONPATH": ROOT}
    pv = (os.environ.get("PIPELINE_VERBOSE") or "").strip().lower() in ("1", "true", "yes")
    if not pv:
        env.setdefault("PIPELINE_QUIET", "1")
        env.setdefault("MOE_C_PRINT_ALL", "0")
        env.setdefault("MOE_C_PRINT_MAX", "12")
        env.setdefault("PIPELINE_CALIBRATION_LIST_MAX", "32")
        logging.getLogger().setLevel(logging.WARNING)

    run_a = (os.environ.get("RUN_MODULE_A") or "1").strip().lower() in ("1", "true", "yes")
    run_refresh = (os.environ.get("RUN_REFRESH_AFTER_B") or "1").strip().lower() in ("1", "true", "yes")
    run_c = (os.environ.get("RUN_MODULE_C") or "1").strip().lower() in ("1", "true", "yes")

    track = (os.environ.get("DITING_TRACK") or "a").strip().lower()
    total_steps = (1 if run_a else 0) + 1 + (1 if run_refresh else 0) + (1 if run_c else 0)
    if pv:
        logger.info(
            "run_full_pipeline: 轨=%s, A=%s, refresh=%s, C=%s, 总步数=%s",
            track,
            run_a,
            run_refresh,
            run_c,
            total_steps,
        )
    step = 0
    quiet_orch = not pv

    if run_a:
        step += 1
        rc = _run_step(
            step,
            total_steps,
            "Module A",
            "Module A · 语义分类 → classifier_output_snapshot",
            "MODULE_A",
            [py, str(scripts / "run_module_a_local.py")],
            ROOT,
            env,
            quiet_orchestrator=quiet_orch,
        )
        if rc != 0:
            return rc

    step += 1
    rc = _run_step(
        step,
        total_steps,
        "Module B",
        "Module B · 量化扫描 → quant_signal_scan_all / snapshot",
        "MODULE_B",
        [py, str(scripts / "run_module_b_local.py")],
        ROOT,
        env,
        quiet_orchestrator=quiet_orch,
    )
    if rc != 0:
        return rc

    if run_refresh:
        step += 1
        refresh_short = "信号层-A轨" if track == "a" else "信号层refresh"
        refresh_title = (
            "信号层 · A 轨（a_track_signal_cache 双路打标 + 观测表）"
            if track == "a"
            else "信号层 · 细分 refresh → segment_signal_cache"
        )
        rc = _run_step(
            step,
            total_steps,
            refresh_short,
            refresh_title,
            "SEGMENT_REFRESH",
            [py, str(scripts / "run_refresh_segment_signals.py")],
            ROOT,
            env,
            quiet_orchestrator=quiet_orch,
        )
        if rc != 0:
            if pv:
                logger.warning("信号层 refresh 失败，C 可能无细分信号；继续执行 C")
            else:
                print("  [注意] 信号层 refresh 非 0，C 可能无细分信号；继续跑 C 便于排查")
            # 不 return

    if run_c:
        step += 1
        rc = _run_step(
            step,
            total_steps,
            "Module C",
            "Module C · MoE → moe_expert_opinion_snapshot",
            "MODULE_C",
            [py, str(scripts / "run_module_c_local.py")],
            ROOT,
            env,
            quiet_orchestrator=quiet_orch,
        )
        if rc != 0:
            return rc

    if pv:
        logger.info("run_full_pipeline: 全链路完成")
    print()
    print("=" * _W)
    print("  PIPELINE  全链路结束  （向上按「PIPELINE [n/m]」与 | 归属框 分段阅读）")
    print("=" * _W)
    print("  各步「准出（设计对照）」: 见各节内 ┌─ … ┐ 框。")
    print("  观测要点: A 行数 / B scan 与 snapshot / 信号层有无细分 / C snapshot=MoE 条数")
    print("  L2 汇总: make query-full-pipeline-result")
    print("  完整终端: export PIPELINE_VERBOSE=1 后重跑 make run-full-pipeline")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
