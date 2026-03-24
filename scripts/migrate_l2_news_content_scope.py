#!/usr/bin/env python3
# [Ref: 07_行业新闻与标的新闻分离存储_设计] news_content 增加 scope/scope_id，唯一约束改为 (scope, scope_id, title_hash, published_at)
# 用法: PYTHONPATH=. python3 scripts/migrate_l2_news_content_scope.py
# 幂等：可重复执行

import os
import sys
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

_env = Path(ROOT) / ".env"
if _env.exists():
    with open(_env, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and os.environ.get(k) is None:
                    os.environ[k] = v


def main() -> int:
    dsn = os.environ.get("PG_L2_DSN", "").strip()
    if not dsn:
        print("未配置 PG_L2_DSN。", file=sys.stderr)
        return 1
    try:
        import psycopg2
    except ImportError:
        print("未安装 psycopg2。", file=sys.stderr)
        return 1
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            """
            ALTER TABLE news_content ADD COLUMN IF NOT EXISTS scope VARCHAR(32) NOT NULL DEFAULT 'symbol';
            """
        )
        cur.execute(
            """
            ALTER TABLE news_content ADD COLUMN IF NOT EXISTS scope_id VARCHAR(128) NOT NULL DEFAULT '';
            """
        )
        # 回填：个股行 scope_id = symbol；全市场
        cur.execute(
            """
            UPDATE news_content SET scope = 'symbol', scope_id = symbol
            WHERE (scope IS NULL OR scope = 'symbol') AND (scope_id = '' OR scope_id IS NULL)
              AND symbol IS NOT NULL AND symbol != '_MARKET_';
            """
        )
        cur.execute(
            """
            UPDATE news_content SET scope = 'market', scope_id = '_MARKET_'
            WHERE symbol = '_MARKET_';
            """
        )
        cur.execute(
            """
            ALTER TABLE news_content ALTER COLUMN symbol DROP NOT NULL;
            """
        )
        cur.execute("DROP INDEX IF EXISTS uq_news_content_dedup;")
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_news_content_scope_dedup
            ON news_content (scope, scope_id, title_hash, published_at);
            """
        )
        cur.close()
        conn.close()
        print("migrate_l2_news_content_scope OK（scope/scope_id + 唯一索引 uq_news_content_scope_dedup）。")
    except Exception as e:
        print("迁移失败: %s" % e, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
