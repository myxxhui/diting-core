# [Ref: 03_原子目标与规约/_共享规约/11_数据采集与输入层规约]
# [Ref: dna_stage2_02.yaml required_config]
# 从 .env 或环境变量加载 TIMESCALE_DSN、PG_L2_DSN、REDIS_URL

import os
from pathlib import Path
from typing import Optional

_env_loaded: bool = False


def _load_dotenv_once() -> None:
    global _env_loaded
    if _env_loaded or os.environ.get("TIMESCALE_DSN"):
        _env_loaded = True
        return
    # 从当前工作目录或 diting/ingestion 向上找 .env
    for d in [Path.cwd(), Path(__file__).resolve().parents[2]]:
        env_file = d / ".env"
        if env_file.exists():
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip().strip('"').strip("'")
                        if k and os.environ.get(k) is None:
                            os.environ[k] = v
            break
    _env_loaded = True


def get_timescale_dsn() -> str:
    _load_dotenv_once()
    v = os.environ.get("TIMESCALE_DSN", "").strip()
    if not v:
        raise ValueError("TIMESCALE_DSN not set (copy .env.template to .env and fill)")
    return v


def get_pg_l2_dsn() -> str:
    v = os.environ.get("PG_L2_DSN", "").strip()
    if not v:
        raise ValueError("PG_L2_DSN not set (copy .env.template to .env and fill)")
    return v


def get_redis_url() -> Optional[str]:
    v = os.environ.get("REDIS_URL", "").strip()
    return v or None
