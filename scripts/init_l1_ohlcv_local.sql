-- 本地验证用：仅建表与索引，不含 create_hypertable（TimescaleDB 生产用）
CREATE TABLE IF NOT EXISTS ohlcv (
    symbol   VARCHAR(32) NOT NULL,
    period   VARCHAR(16) NOT NULL,
    datetime TIMESTAMPTZ NOT NULL,
    open     DOUBLE PRECISION NOT NULL,
    high     DOUBLE PRECISION NOT NULL,
    low      DOUBLE PRECISION NOT NULL,
    close    DOUBLE PRECISION NOT NULL,
    volume   BIGINT,
    PRIMARY KEY (symbol, period, datetime)
);
CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_period ON ohlcv (symbol, period, datetime DESC);
