# diting-core

核心逻辑仓（血肉与大脑）。[Ref: 02_三位一体仓库规约]

## 目录结构

与 `global_const.trinity_repos.repo_i.directories` 一致：

- `diting/abstraction/` - 接口抽象层 [Ref: 05_接口抽象层规约]
- `diting/drivers/` - 驱动层
- `diting/moe/` - MoE 议会 [Ref: 09_核心模块架构规约 Module C]
- `diting/risk/` - 风控 [Module E]
- `diting/strategy/` - 策略层
- `tests/` - 单测
- `design/` - 设计产物 [Ref: 04_全链路通信协议矩阵]

## 本地单测

```bash
make test
```

预期：退出码 0，单测全绿。

## Stage2 本地实践（无 K3s 时）

实践文档：diting-doc `04_阶段规划与实践/Stage2_数据采集与存储/`。**部署与编排归属 diting-infra**（见 02_三位一体仓库规约）：Docker Compose、本地建表脚本与 `local-deps-up/down/init` 在 **diting-infra** 执行；本仓仅负责连接与验证。

1. **在 diting-infra 启动本地 L1/L2**：
   ```bash
   cd <diting-infra> && make local-deps-up && make local-deps-init
   ```

2. **在本仓（diting-core）配置并验证**（需 Python 依赖 `requirements-ingest.txt`）：
   - 复制 `.env.template` 为 `.env`，填写 `TIMESCALE_DSN=postgresql://postgres:postgres@localhost:15432/postgres`、`PG_L2_DSN=postgresql://postgres:postgres@localhost:15433/diting_l2`
   ```bash
   make verify-db-connection
   make ingest-test
   ```

3. **按文档做 V-DATA**：执行 `docs/ingest-test-target.md` 中 5 条 psql 验证查询，将结果填入文档「目标数据约定与真实结果」表。

4. **回收本地资源**（在 diting-infra）：
   ```bash
   cd <diting-infra> && make local-deps-down
   ```

有 Stage2-01 集群（K3s）时，在本仓配置 `.env` 指向集群 NodePort 后执行 `make verify-db-connection`、`make ingest-test` 即可。
