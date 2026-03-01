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

## 环境变量（拉代码即可配置）

所有关键配置以 **`.env.template`** 为模板，复制为 `.env` 后本地填写，勿提交 `.env`。

| 变量 | 必填 | 说明 |
|------|------|------|
| `TIMESCALE_DSN` | ✅ | L1 时序库连接串；本地 Compose 示例：`postgresql://postgres:postgres@localhost:15432/postgres` |
| `PG_L2_DSN` | ✅ | L2 知识库连接串；本地示例：`postgresql://postgres:postgres@localhost:15433/diting_l2` |
| `REDIS_URL` | ✅ | Redis 连接串；本地示例：`redis://localhost:15479/0` |
| `API_KEY` / `DB_URL` | 可选 | 预留扩展 |
| `DITING_INGEST_MOCK` | 可选 | 设为 `1` 时 ingest 使用 mock 数据（无外网/CI 可用） |
| `KUBECONFIG` / `PUBLIC_IP` | 可选 | 连接已部署的 K3s 时填写 |

```bash
cp .env.template .env
# 编辑 .env 填写上述必填三项后执行：
make verify-db-connection
make ingest-test
```

## Stage2 本地实践（无 K3s 时）

**部署与编排归属 diting-infra**（见 02_三位一体仓库规约）。完整步骤（本地 L1/L2 启动、本仓配置与验证、回收）见 **diting-infra** 仓库的 `docs/Stage2-本地实践.md`。

本仓仅负责连接与验证：复制 `.env.template` 为 `.env` 并填写 DSN 后，执行 `make verify-db-connection`、`make ingest-test`。V-DATA 的 5 条 psql 约定见本仓 `docs/ingest-test-target.md`。
