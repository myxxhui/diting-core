-- [Ref: 03_原子目标与规约/_共享规约/11_数据采集与输入层规约] 全 A 股标的池表
-- 与 L1 同库；表名与字段与 11_ 约定一致：symbol, market, updated_at；可选 count, source
-- 由 Stage2 采集 Job 或 run_ingest_universe 写入；get_current_a_share_universe() 读取

CREATE TABLE IF NOT EXISTS a_share_universe (
    symbol TEXT NOT NULL PRIMARY KEY,
    market TEXT NOT NULL DEFAULT 'A',
    updated_at TIMESTAMPTZ NOT NULL,
    count INTEGER,
    source TEXT
);
