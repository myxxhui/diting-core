# 采集数据类型、写入表与 A/B 模块使用对照

本文档说明 **make ingest-production**（全量生产采集）会采集哪些数据、写入哪些表，以及 **Module A**、**Module B** 如何使用这些数据。

---

## 一、采集数据类型与写入表（L1 / L2）

| 采集类型 | 数据来源 | 写入库 | 写入表/存储 | 采集步骤 |
|----------|----------|--------|-------------|----------|
| **标的池（Universe）** | AkShare/JQData 全 A 股列表 | L1 TimescaleDB | **a_share_universe**（symbol, market, updated_at, count, source） | step1 |
| **K 线（OHLCV）** | AkShare/JQData/Baostock 日线 | L1 TimescaleDB | **ohlcv**（symbol, period, datetime, open, high, low, close, volume） | step2 |
| **行业/财务** | AkShare/JQData 财务摘要、申万行业等 | L2 PostgreSQL | **industry_revenue_summary**（symbol, industry_name, revenue_ratio, rnd_ratio, commodity_ratio, updated_at）<br>**data_versions**（data_type=industry_revenue 的版本记录） | step3 |
| **新闻** | AkShare/OpenBB/JQData 全市场 + 个股新闻 | L2 PostgreSQL | **data_versions**（data_type=news 的版本记录）<br>L2 文件/JSON（如 l2/news/ 下按 version_id 存储） | step4 |

- **L1**：TimescaleDB（`TIMESCALE_DSN`），存标的池与 K 线。
- **L2**：PostgreSQL（`PG_L2_DSN`），存行业/财务汇总表、新闻版本及文件路径；**data_versions** 表记录各 data_type 的 version_id、timestamp、file_path 等，用于版本追溯与 Module A 按版本取数。

---

## 二、Module A（语义分类器）如何使用

| 数据类型 | 使用方式 | 代码位置 |
|----------|----------|----------|
| **标的池** | `get_current_a_share_universe()` 从 L1 **a_share_universe** 读全 A 股列表，作为待分类标的池 | `diting.universe`、`diting.classifier.run`、`diting.classifier.semantic` |
| **行业/财务** | 从 L2 **industry_revenue_summary** 按 symbol 批量查（industry_name, revenue_ratio, rnd_ratio, commodity_ratio），作为语义分类特征输入 | `diting.classifier.l2_provider.get_l2_industry_revenue_batch`、`classifier/run.py` |
| **新闻** | 规约上为 Module A/C 输入；当前实现中语义分类器主要用行业/财务，新闻多用于知识库/后续扩展 | 06_ 实践文档 |

结论：**Module A 依赖** L1 **a_share_universe**（标的池）+ L2 **industry_revenue_summary**（行业/营收等）；采集 step1 + step3 写满后即可支持 Module A 全量分类。**指定股票**：默认使用 **`DITING_SYMBOLS`**（与采集、Module B 共用一套名单）；未设置时可用 `MODULE_AB_SYMBOLS` 仅控制 A/B。

---

## 三、Module B（量化扫描引擎）如何使用

| 数据类型 | 使用方式 | 代码位置 |
|----------|----------|----------|
| **标的池** | `get_current_a_share_universe()` 从 L1 **a_share_universe** 读全 A 股列表，与 Module A 同源、同批 | `diting.universe`、`diting.scanner.quant.QuantScanner.run_full` |
| **K 线（OHLCV）** | 规约/设计上 Module B 使用 **L1 ohlcv** 做技术面扫描（技术得分、板块强度等）；当前 `QuantScanner.scan_market` 为占位，未查 ohlcv，后续接入 TA-Lib/VectorBT 等将读 L1 ohlcv | `diting.scanner.quant`、09_/11_ 规约 |

结论：**Module B 依赖** L1 **a_share_universe**（标的池）+ L1 **ohlcv**（K 线，规约输入；实现待填）；采集 step1 + step2 写满后即可在实现层接入 Module B 所需数据。

---

## 四、简要汇总

| 表名 | 库 | 采集步骤 | Module A | Module B |
|------|-----|----------|----------|----------|
| **a_share_universe** | L1 | step1 | ✅ 标的池 | ✅ 标的池 |
| **ohlcv** | L1 | step2 | — | ✅ 规约输入（K 线，实现待填） |
| **industry_revenue_summary** | L2 | step3 | ✅ 行业/财务特征 | — |
| **data_versions** | L2 | step3/step4 | 版本追溯/按版本取数 | — |
| 新闻（L2 文件/JSON） | L2 | step4 | 可选/扩展 | — |

**make ingest-production** 四步全开时，会写满上表所需数据，**不会缺失导致 A/B 模块无法正常工作**；仅当某步被关闭（如 `INGEST_PRODUCTION_OHLCV=false`）时，对应表可能缺数据，需按需开启。

**执行顺序**：step1 标的池仅执行一次；之后**按批**执行，每批内依次完成 step2（K 线）→ step3（行业/财务）→ step4（该批个股新闻）；step4 的全市场+宏观新闻在首批前执行一次。断点续跑以「已完成标的数」为单位，每批完成本批的 step2+step3+step4 后再更新进度。

**指定股票（默认一套，采集与 A/B 共用）**：设置环境变量 **`DITING_SYMBOLS`**（逗号分隔或一行一个代码的文件路径），则采集仅对这些标的执行 step2～step4 并写入 L1/L2，Module A/B 也仅对这些标的运行；不设置则全量。可选覆盖：仅采集用 `INGEST_PRODUCTION_SYMBOLS`，仅 A/B 用 `MODULE_AB_SYMBOLS`。

**支持指定股票的模块与入口**：

| 模块/入口 | 说明 | 读取变量 |
|-----------|------|----------|
| 全量采集 | `run_ingest_production.py` / `make ingest-production` | DITING_SYMBOLS → INGEST_PRODUCTION_SYMBOLS |
| 增量采集 | `run_ingest_production_incremental.py` | DITING_SYMBOLS → INGEST_PRODUCTION_SYMBOLS |
| Module A 入口 | `diting.classifier.run` / `python -m diting.classifier.run` | DITING_SYMBOLS → MODULE_AB_SYMBOLS |
| Module B 入口 | `QuantScanner.run_full()` 未传 universe 时 | DITING_SYMBOLS → MODULE_AB_SYMBOLS |
| Module A 内部 | `SemanticClassifier.run_full()` 未传 universe 时 | DITING_SYMBOLS → MODULE_AB_SYMBOLS |
| 每日编排 | `run_daily_scan.py`（A+B 同批） | DITING_SYMBOLS → MODULE_AB_SYMBOLS |

**C/E/F/G 等**：Module C（MoE 议会）、Module E（风控）及规约中的 F/G 在代码仓中目前为**占位或尚未实现**「按标的列表运行」的入口，因此**不需要**支持指定股票。日后若实现类似 A/B 的 `run_full(universe)` 或按标的调用的入口，再统一读取 `DITING_SYMBOLS` 即可。
