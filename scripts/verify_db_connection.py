#!/usr/bin/env python3
# [Ref: 04_阶段规划与实践/Stage2_数据采集与存储/02_采集逻辑与Dockerfile_实践.md]
# [Ref: 11_数据采集与输入层规约] Redis 默认部署且必验，含缓存/队列效果与简单性能测试
# 工作目录: diting-core。校验 L1/L2 表存在、Redis 连通与简单 SET/GET 性能；退出码 0 表示 V-DB 通过。

import os
import sys
import time

# 加载 .env（与 Makefile 一致）
from pathlib import Path

root = Path(__file__).resolve().parents[1]
env_file = root / ".env"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

try:
    import psycopg2
except ImportError:
    print("psycopg2 not installed (pip install -r requirements-ingest.txt)", file=sys.stderr)
    sys.exit(1)

TIMESCALE_DSN = os.environ.get("TIMESCALE_DSN")
PG_L2_DSN = os.environ.get("PG_L2_DSN")
REDIS_URL = os.environ.get("REDIS_URL", "").strip()


def check_table(conn, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s",
            (table_name,),
        )
        return cur.fetchone() is not None


def main() -> int:
    if not TIMESCALE_DSN:
        print("TIMESCALE_DSN not set (copy .env.template to .env and fill)", file=sys.stderr)
        return 1
    try:
        conn = psycopg2.connect(TIMESCALE_DSN)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        if not check_table(conn, "ohlcv"):
            print("L1: table ohlcv not found", file=sys.stderr)
            conn.close()
            return 1
        print("L1 verify OK")
        conn.close()
    except Exception as e:
        print(f"L1 verify failed: {e}", file=sys.stderr)
        return 1

    if not PG_L2_DSN:
        print("PG_L2_DSN not set (required; copy .env.template to .env and fill)", file=sys.stderr)
        return 1
    try:
        conn = psycopg2.connect(PG_L2_DSN)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        if not check_table(conn, "data_versions"):
            print("L2: table data_versions not found", file=sys.stderr)
            conn.close()
            return 1
        print("L2 verify OK")
        conn.close()
    except Exception as e:
        print(f"L2 verify failed: {e}", file=sys.stderr)
        return 1

    # Redis：必验；连通性 + 缓存/队列效果与简单性能测试（11_ 规约）
    if not REDIS_URL:
        print("REDIS_URL not set (required; copy .env.template to .env and fill)", file=sys.stderr)
        return 1
    try:
        import redis
    except ImportError:
        print("redis not installed (pip install redis)", file=sys.stderr)
        return 1
    try:
        r = redis.from_url(REDIS_URL)
        r.ping()
        print("Redis PING OK")
        # 缓存/队列效果与简单性能测试：N 次 SET+GET，报告平均延迟
        n_rounds = 100
        key = "verify_diting_prod:ping"
        t0 = time.perf_counter()
        for i in range(n_rounds):
            r.set(key, f"v{i}", ex=60)
            r.get(key)
        elapsed = time.perf_counter() - t0
        avg_ms = (elapsed / n_rounds) * 1000
        r.delete(key)
        print(f"Redis cache/queue perf: {n_rounds} SET+GET in {elapsed:.3f}s, avg {avg_ms:.2f} ms/op")
        if avg_ms > 50:
            print(f"WARN: Redis avg latency {avg_ms:.2f} ms > 50 ms", file=sys.stderr)
        print("Redis verify OK")
    except Exception as e:
        print(f"Redis verify failed: {e}", file=sys.stderr)
        return 1

    print("verify-db-connection OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
