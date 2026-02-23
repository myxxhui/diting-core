# [Ref: 03_原子目标与规约/_共享规约/11_数据采集与输入层规约]
# [Ref: dna_stage2_02.yaml required_config]
# 从 .env 或环境变量加载 TIMESCALE_DSN、PG_L2_DSN、REDIS_URL

import os
from typing import Optional


def get_timescale_dsn() -> str:
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
