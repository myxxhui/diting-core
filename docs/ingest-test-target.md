# ingest-test 目标数据约定

> [Ref: 04_阶段规划与实践/Stage2_数据采集与存储/02_采集逻辑与Dockerfile_实践.md#l4-stage2-02-target-data]  
> 本文档约定 `make ingest-test` 会采集的**目标数据**，验收时据此对照验证查询结果。

## L1 OHLCV

| 项目 | 约定 |
|------|------|
| **股票范围** | 至少 2 只 A 股：`000001.SZ`（平安银行）、`600000.SH`（浦发银行） |
| **周期** | `period = daily`（日线） |
| **时间范围** | 最近约 5 个自然日内的交易日数据（具体条数依 AkShare 返回） |

**验证查询**（执行 `make ingest-test` 后）：

- 有哪些股票与周期：  
  `psql $TIMESCALE_DSN -c "SELECT DISTINCT symbol, period FROM ohlcv ORDER BY symbol, period;"`
- 日期范围与总行数：  
  `psql $TIMESCALE_DSN -c "SELECT min(datetime) AS from_ts, max(datetime) AS to_ts, count(*) AS rows FROM ohlcv;"`
- 每只股票每周期条数：  
  `psql $TIMESCALE_DSN -c "SELECT symbol, period, count(*) AS cnt FROM ohlcv GROUP BY symbol, period ORDER BY symbol, period;"`

**通过标准**：结果中至少包含上述 2 只股票、`period=daily`，且在约定时间范围内有数据。

## L2 / 行业与新闻

| 项目 | 约定 |
|------|------|
| **data_type** | 至少包含：`industry_revenue`（行业/营收）、`news`（新闻） |
| **industry_revenue** | 至少 1 条：来自 AkShare 财务摘要（默认 symbol 000001） |
| **news** | 至少 1 条：国内（AkShare 最新资讯）或国际（OpenBB 宏观/报价）任一路径写入 |

**验证查询**：

- 各 data_type 条数：  
  `psql $PG_L2_DSN -c "SELECT data_type, count(*) AS cnt FROM data_versions GROUP BY data_type ORDER BY data_type;"`
- 样例条目：  
  `psql $PG_L2_DSN -c "SELECT data_type, version_id, timestamp FROM data_versions ORDER BY timestamp DESC LIMIT 5;"`

**通过标准**：能列出 `industry_revenue` 与 `news` 的 data_type，且条数与上述约定一致（至少各 1 条路径可写）。
