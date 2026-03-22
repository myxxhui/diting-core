# [Ref: 02_B模块策略_策略实现规约 §3.12]

import os
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]


def test_golden_scanner_batch_cli_passes():
    """子进程设置 PYTHONHASHSEED=0 并清除 DSN，与 make golden-scanner-batch 一致。"""
    env = os.environ.copy()
    env["PYTHONHASHSEED"] = "0"
    env.pop("PG_L2_DSN", None)
    env.pop("TIMESCALE_DSN", None)
    r = subprocess.run(
        [sys.executable, str(_ROOT / "scripts" / "golden_scanner_batch.py")],
        cwd=str(_ROOT),
        env=env,
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, (r.stderr or "") + (r.stdout or "")
