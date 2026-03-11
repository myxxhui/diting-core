# 东方财富接口连接被拒：原因与应对

采集使用 AkShare 访问东方财富（eastmoney）时出现 `RemoteDisconnected`、`Connection reset` 等，**目标是用生产数据**，不依赖静态回退。本文说明可能原因与应对方式。

## 被拒是因为「标的太多」还是「单次历史数据量太大」？

| 数据类型 | 接口 | 每次请求 | 被拒更可能原因 |
|----------|------|----------|----------------|
| **行业/个股信息** | `stock_individual_info_em(symbol)` | 一只标的、**当前**信息（无历史） | **① 请求次数多**：全市场 5000+ 只，每只 1 次请求；**② IP/地区**：境外或香港节点常**第一次请求即断**，与数量无关 |
| **K 线 OHLCV** | `stock_zh_a_hist(symbol, start_date, end_date)` | 一只标的、**一段日期**日线（生产约 5 年） | **① 请求次数多**：全市场每只 1 次；**② 单次数据量**：5 年约 1200 条，单次响应较大，可能触发限流或超时；**③ IP/地区**：同上 |

结论：

- **行业接口**：单次请求很轻，被拒多半是 **请求次数多（标的多）** 或 **IP/地区限制**（境外/香港易首包即断）。
- **K 线接口**：既有 **标的多、请求次数多**，也有 **单次历史数据量较大**，两者都可能触发限流或断连。

## 应对措施（优先顺序）

1. **IP/地区**（境外、香港、云机房）
   - 在**境内**机器或 K8s 节点跑采集；或
   - 配置 **HTTP 代理**（境内出口）：在 `.env` 中设置 `INGEST_HTTP_PROXY`、`INGEST_HTTPS_PROXY`（如 `http://proxy.example.com:8080`），再执行 `make ingest-production`。akshare 底层 requests 会读取 `HTTP_PROXY`/`HTTPS_PROXY`。

2. **请求次数多 / 限流**
   - 减小每批数量、加大间隔，降低 QPS：
     - `INGEST_OHLCV_BATCH_SIZE=50`（或更小，如 15）
     - `INGEST_OHLCV_BATCH_PAUSE_SEC=120`
     - `INGEST_OHLCV_DELAY_BETWEEN_SYMBOLS=5`
     - 行业/新闻按标延迟：`INGEST_EXTRA_DELAY_SEC=3`
   - 先对**指定标的**跑通再扩全量：设置 `DITING_SYMBOLS=config/diting_symbols.txt`（或逗号分隔列表），确认能稳定拉取后再去掉或扩大列表。

3. **单次历史数据量（仅 OHLCV）**
   - 缩短单次请求时间范围：例如 `INGEST_PRODUCTION_OHLCV_DAYS_BACK=365` 先拉 1 年，再分批延长；
   - 或改用 **Baostock**：`INGEST_OHLCV_SOURCE=baostock`，避免东方财富 K 线接口限流（行业仍走 akshare 或 JQData）。

4. **换数据源**
   - 行业/财务：`INGEST_SOURCE=jqdata`，用聚宽拉行业与财务，不经过东方财富；
   - K 线：`INGEST_OHLCV_SOURCE=baostock` 或 `jqdata`。

## 境外（如菲律宾）本地：27 只 + 最近 3 个月、尽量不失败

`make ingest-production` 会读 **diting-core/.env**。按下面配置后，再在 diting-core 目录执行 `make ingest-production`。

1. **指定 27 只、不拉全 A 股列表**（避免首请求就触发东方财富限流/地区限制）  
   - `DITING_SYMBOLS=config/diting_symbols.txt`  
   - `INGEST_PRODUCTION_UNIVERSE=false`

2. **只采最近 3 个月**（用统一日期，与 INGEST_SOURCE 无关）  
   - `INGEST_JQDATA_DATE_START=2026-01-01`  
   - `INGEST_JQDATA_DATE_END=2026-03-07`（格式必须 YYYY-MM-DD，月/日两位）

3. **数据源用 akshare**  
   - `INGEST_SOURCE=akshare` 或 `INGEST_SOURCE=AkShare`

4. **每批 1 只、加长间隔**（减轻限流与断连）  
   - `INGEST_OHLCV_BATCH_SIZE=1`  
   - `INGEST_OHLCV_BATCH_PAUSE_SEC=30`  
   - `INGEST_OHLCV_DELAY_BETWEEN_SYMBOLS=8`  
   - `INGEST_EXTRA_DELAY_SEC=5`

5. **按阶段分开执行**（先全部 K 线 → 停顿 → 全部行业 → 停顿 → 全部新闻，便于阶段间长停顿、观察限流是否缓解）  
   - `INGEST_SEPARATE_PHASES=1`  
   - `INGEST_PHASE_PAUSE_SEC=120`（阶段间暂停秒数，可改为 180 或 300 试）  
   - 再执行 `make ingest-production`。日志会先跑完 Phase 1（K 线），暂停后 Phase 2（行业），再暂停后 Phase 3（新闻）。**可能有助于**东方财富按“近期请求”限流时，通过长停顿让行业阶段少被拒；若为纯地区封锁则仍需代理或境内节点。

6. **有境内 HTTP 代理时**（东方财富境外 IP 易被拒时优先）  
   - `INGEST_HTTP_PROXY=http://代理地址:端口`  
   - `INGEST_HTTPS_PROXY=http://代理地址:端口`  
   再执行 `make ingest-production`，akshare 底层会走代理。

7. **执行**  
   - `cd diting-core && make ingest-production`  
   - 若仍出现 RemoteDisconnected：先试加长上述延迟或 INGEST_PHASE_PAUSE_SEC；若无代理可考虑在境内机器或 K8s 节点跑采集，或改用 `INGEST_OHLCV_SOURCE=baostock` / `INGEST_SOURCE=jqdata`（见上文「换数据源」）。

## 当前逻辑约定

- 东方财富/JQData 均无数据时，**不使用静态回退**，只记日志并返回失败，便于发现网络或数据源问题。
- 静态回退（`config/industry_fallback.csv`）仅用于运维**显式**执行 `scripts/backfill_industry_from_fallback.py` 等单独回填场景，不作为采集失败时的自动兜底。
