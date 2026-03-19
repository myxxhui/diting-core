# make ingest-production 卡住原因分析与解决方案

## 一、卡住位置（从日志推断）

日志最后一行：
```
INFO diting.ingestion.l1_writer write_ohlcv_batch: inserted/updated 154 rows
INFO diting.ingestion.ohlcv K 线 L1 写入完成 154 行
```
之后没有出现 `industry_revenue` / `financial` / `news` 的任何日志。

每只标的的**执行顺序**是：
1. **OHLCV**（K 线）→ 2. **行业/财务摘要**（industry_revenue）→ 3. **财务报表**（financial）→ 4. **新闻**（news）

因此可以确定：卡在 **002015.SZ 的「行业/财务」或「财务」或「新闻」** 的**第一次网络请求**上，且该请求没有超时，一直等服务器响应。

---

## 二、根本原因

| 模块 | 调用的 akshare 接口 | 是否对 requests 做超时/补丁 |
|------|----------------------|------------------------------|
| **ohlcv** | `stock_zh_a_hist` | ✅ 有：在 `ohlcv.py` 里对 `requests.get` 做了 45s 超时补丁（仅在该函数执行期间生效） |
| **industry_revenue** | `stock_individual_info_em`、`stock_financial_abstract`、`stock_financial_analysis_indicator` | ❌ **无**：直接调 akshare，未给 requests 加超时 |
| **financial** | `stock_financial_abstract`、`stock_finance_sina` 等 | ❌ **无**：同上 |
| **news** | `stock_news_em`、`js_news` | ⚠️ 有 30s 超时，但仅对 URL 含 `eastmoney.com` 的 `requests.get`；若 akshare 用 Session 或其它 URL 则可能不生效 |

结论：

- **只有 K 线**在拉取时对 `requests.get` 做了临时 45 秒超时。
- **行业、财务、新闻** 使用的 akshare 接口底层也是 `requests`，但**没有**在这些路径上统一加超时。
- 东方财富/新浪接口在限流、网络抖动或服务器无响应时，可能长时间不返回也不断连，导致 **TCP 一直等待**，表现为进程「卡着不动」。

---

## 三、推荐方案：进程级统一超时（推荐）

**思路**：在 **`run_ingest_production.py` 的 `main()` 入口** 对 `requests` 做一次**进程级**补丁，让**本次进程内所有** `requests.get` / `Session.request` 都带上默认超时（例如 45 秒）。这样：

- **行业、财务、新闻、K 线** 里所有走 `requests` 的 akshare 调用都会在 45 秒内要么成功要么抛超时，不会无限卡住。
- 超时后上层已有重试（industry_revenue / financial / news 等都有 retry），会自动重试，无需改各模块内部逻辑。
- 只改一处（生产采集入口），不动 ohlcv/industry_revenue/financial/news 内部实现，风险小、易回滚。

**实现要点**：

1. 在 `main()` 开头（在加载 .env、解析参数之后，真正开始采集之前）：
   - `import requests`
   - 保存 `requests.get`、`requests.Session.request` 的原始引用。
   - 用包装函数对**所有**请求统一 `kwargs.setdefault("timeout", 45)`（或可配置，如从环境变量 `INGEST_REQUESTS_TIMEOUT` 读，默认 45）。
   - 替换：`requests.get = ...`、`requests.Session.request = ...`。
2. 超时时间建议 45 秒（与 ohlcv 一致）；若需可配置，可加 `.env` 项如 `INGEST_REQUESTS_TIMEOUT=45`。
3. 进程结束时无需恢复补丁（进程退出即失效）；若希望严谨，可在 `main()` 末尾用 try/finally 恢复。

**优点**：

- 一次修改，覆盖所有采集阶段的 HTTP 请求，避免再次在「行业/财务/新闻」某一步卡死。
- 不侵入 akshare 和各 ingestion 模块，便于维护。
- 与现有重试逻辑兼容：超时抛异常 → 现有 retry 捕获后重试。

**可选增强**：

- 在 industry_revenue / financial 的 akshare 调用前打一条日志（如「正在拉取行业/财务 002015.SZ…」），便于下次若还有问题快速定位到具体接口。

---

## 四、其它可选方案（不推荐作为主方案）

| 方案 | 说明 | 缺点 |
|------|------|------|
| 在各模块内分别给 akshare/requests 加超时 | 在 industry_revenue.py、financial.py、news.py 里各自 patch 或传 timeout | 改动分散，易遗漏；akshare 内部若用 Session 或多层封装，不一定生效 |
| 用 signal 或 threading 做单步超时 | 每步（如每只标的的 industry）包一层「N 秒后强制抛异常」 | 与多线程/异步兼容性差，且可能误杀正常慢请求 |
| 仅拉大 ohlcv 超时、不管其它 | 只保证 K 线不卡 | 当前卡点已在 K 线之后，无法解决行业/财务/新闻卡死 |

---

## 五、建议结论

- **原因**：卡在 002015.SZ 的 **行业或财务或新闻** 的第一次 akshare 请求，且这些路径**没有对 requests 设置超时**，导致一直等待服务器。
- **最佳修复**：在 **`run_ingest_production.py` 的 `main()` 开头** 做 **进程级 `requests` 超时补丁**（统一 45s，可选从环境变量读取），确保整次 `make ingest-production` 中所有 HTTP 请求都有超时，避免再次卡死。

你同意后，我将在 `run_ingest_production.py` 中按上述方案实现并加上可选日志。
