# [Ref: 06_B轨_信号层_1对1对1] 全链路终端：模块归属框（便于区分 A/B/信号层/C）
import os


def pipeline_frame_quiet() -> None:
    """
    在 make run-full-pipeline 子进程中、且 PIPELINE_QUIET=1 时打印一节归属框。
    依赖环境变量：PIPELINE_STEP、PIPELINE_TOTAL、PIPELINE_TITLE_CN（由 run_full_pipeline 注入）。
    """
    pq = (os.environ.get("PIPELINE_QUIET") or "").strip().lower() in ("1", "true", "yes")
    if not pq:
        return
    ps = (os.environ.get("PIPELINE_STEP") or "").strip()
    pt = (os.environ.get("PIPELINE_TOTAL") or "").strip()
    title = (os.environ.get("PIPELINE_TITLE_CN") or "").strip()
    if not (ps and pt and title):
        return
    bar = "+" + ("-" * 76) + "+"
    print()
    print(bar)
    print("|  [%s/%s]  %s" % (ps, pt, title))
    print(bar)
