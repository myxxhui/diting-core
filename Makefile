# diting-core Makefile
# [Ref: 03_原子目标与规约/_共享规约/02_三位一体仓库规约]

.PHONY: test build test-docker verify verify-db-connection verify-data-test verify-data-production ingest-test ingest-deploy ingest-test-real ingest-production ingest-production-incremental ingest-production-fast ingest-production-incremental-fast deps-ingest build-images deps-classifier diting prod

# 采集相关 target 使用的 Python：akshare 要求 >= 3.8，优先 python3.8（若已安装）
PYTHON_INGEST := $(shell command -v python3.8 2>/dev/null || command -v python3.9 2>/dev/null || command -v python3 2>/dev/null)

# 语义分类器单测依赖（无 pip 时用 python3 -m pip）；安装后执行 make test-classifier-cov
deps-classifier:
	@python3 -m pip install pytest pytest-cov PyYAML

# Stage2-06 采集依赖（真实行情）：akshare、psycopg2、redis 等；06_ 步骤 3、7、8 执行前须先 make deps-ingest。akshare 要求 Python >= 3.8
# 先装 akshare --no-deps 再装 core 依赖，避免 akshare 1.18+ 的 curl_cffi>=0.13 与部分环境不兼容
deps-ingest:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	py="$(PYTHON_INGEST)"; [ -z "$$py" ] && { echo "错误: 未找到 python3.8/python3.9/python3"; exit 1; }; \
	$$py -c "import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)" || { echo "错误: akshare 要求 Python >= 3.8，当前: $$($$py --version 2>&1)。请安装 Python 3.8+（如 dnf install python38）或使用 pyenv/conda。"; exit 1; }; \
	echo "使用 $$py，安装 akshare（无依赖）与采集运行时依赖..."; \
	cd "$$root" && $$py -m pip install --no-deps "akshare==1.17.1" && $$py -m pip install -r requirements-ingest-core.txt && echo "deps-ingest OK（可执行 make ingest-test-real / make ingest-production）"

test:
	@cd "$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))" && (python3 -m pytest tests/ -v --tb=short 2>/dev/null || python3 tests/unit/test_abstraction.py)
# Stage3-01 语义分类器：单测与覆盖率 [Ref: 01_语义分类器_实践]
test-classifier:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; cd "$$root" && PYTHONPATH="$$root" python3 -m pytest tests/unit/test_classifier.py -v --tb=short
test-classifier-cov:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; cd "$$root" && PYTHONPATH="$$root" python3 -m pytest tests/unit/test_classifier.py -v --tb=short --cov=diting.classifier --cov-report=term-missing

build:
	@echo "Build target (placeholder)"
	@exit 0

test-docker:
	@echo "Docker test (placeholder)"
	@exit 0

# make verify [project] [env]：make verify diting prod = 使用 .env 校验 L1/L2 连接与表（生产数据环境验证）
verify:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	cd "$$root" && PYTHONPATH="$$root" python3 scripts/verify_db_connection.py

# 占位目标：make verify diting prod 时不被当作文件
diting:
	@true
prod:
	@true

# Stage2-01 下游验证：与 make verify diting prod 同逻辑；保留兼容
verify-db-connection:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	cd "$$root" && PYTHONPATH="$$root" python3 scripts/verify_db_connection.py

# Stage2-06 生产级数据验收：L1 单标日线≥5 年、标的与 universe 一致；见 06_生产级数据要求_实践
verify-data-production:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	cd "$$root" && PYTHONPATH="$$root" python3 scripts/verify_data_production.py

# Stage2-06 测试集验证：仅验 ingest-test 写入的约 15 标数据存在与口径（用于步骤 4、数据继承验证），不要求 5 年深度
verify-data-test:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	cd "$$root" && VERIFY_DATA_SCOPE=test PYTHONPATH="$$root" python3 scripts/verify_data_production.py

# 生产级数据数量与质量报告（对照 06_/11_ 与 AB 模块预期，仅输出报告）
report-production-data:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	cd "$$root" && PYTHONPATH="$$root" python3 scripts/check_production_data_report.py

# Stage2-02 采集逻辑验证：执行 ingest_ohlcv、ingest_industry_revenue、ingest_news；退出码 0 表示通过。需先 make verify-db-connection。
ingest-test:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	cd "$$root" && PYTHONPATH="$$root" python3 scripts/run_ingest_test.py

# Stage2-06 少量真实行情（约 15 标/约 30 日）：禁止 mock，强制从 AkShare 拉取；步骤 3、7 使用。须先 make deps-ingest
ingest-test-real:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	cd "$$root" && INGEST_FORBID_MOCK=1 PYTHONPATH="$$root" $(PYTHON_INGEST) scripts/run_ingest_test.py

# 部署时采集：检查 L1 是否有数据/是否过期，自动选择全量或增量（见 scripts/run_ingest_deploy.py）
ingest-deploy:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	cd "$$root" && PYTHONPATH="$$root" $(PYTHON_INGEST) scripts/run_ingest_deploy.py

# Stage2-06 全量生产级数据采集：先刷新全A股 universe，再按 universe 拉取单标≥5 年日线。默认串行+标间延迟（CONCURRENT=1、DELAY=3s）减轻东方财富断连；仍断连可设 INGEST_OHLCV_SOURCE=baostock 或加大 INGEST_OHLCV_DELAY_BETWEEN_SYMBOLS。
# 不在此处 export 默认值，由脚本加载 .env；未设置的变量才用脚本内默认参数。
ingest-production:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	cd "$$root" && PYTHONPATH="$$root" $(PYTHON_INGEST) scripts/run_ingest_production.py

# 生产级日终增量：全 A 股标的、仅补最近 N 天（默认 7，可设 INGEST_PRODUCTION_INCREMENTAL_DAYS）；建议每个交易日结束后执行。
# 不在此处 export 默认值，由脚本加载 .env；未设置的变量才用脚本内默认参数。
ingest-production-incremental:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	cd "$$root" && PYTHONPATH="$$root" $(PYTHON_INGEST) scripts/run_ingest_production_incremental.py

# 本地加速：全量/日终增量 使用 CONCURRENT=5、RATE=2.0，OHLCV 约 45 min；若出现 RemoteDisconnected 请改用 make ingest-production / make ingest-production-incremental。
ingest-production-fast:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	cd "$$root" && export INGEST_OHLCV_CONCURRENT=5 INGEST_OHLCV_RATE_PER_SEC=2.0 && \
	PYTHONPATH="$$root" $(PYTHON_INGEST) scripts/run_ingest_production.py
ingest-production-incremental-fast:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	cd "$$root" && export INGEST_OHLCV_CONCURRENT=5 INGEST_OHLCV_RATE_PER_SEC=2.0 && \
	PYTHONPATH="$$root" $(PYTHON_INGEST) scripts/run_ingest_production_incremental.py

# Stage2-02 一键构建本阶段所涉全部镜像 [Ref: 04_阶段规划与实践/Stage2_数据采集与存储/02_采集逻辑与Dockerfile_实践.md V-BUILD-ALL]
# 当前仅采集镜像；推送到 ACR 供 K3s（多为 linux/amd64）拉取时须指定平台，避免在 Mac ARM 上构建出 arm64 镜像导致节点 exec format error
DOCKER_PLATFORM ?= linux/amd64
build-images:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	cd "$$root" && docker build --platform $(DOCKER_PLATFORM) -f Dockerfile.ingest -t diting-ingest:test .
	@echo "build-images: diting-ingest:test OK ($(DOCKER_PLATFORM))"

# Stage2-06 本地构建并推送到项目 ACR；Chart 默认使用 latest [Ref: 06_生产级数据要求_实践.md]
# 需先配置环境变量 DITING_ACR_PASSWORD，或已 docker login 对应 registry。
# ACR 地址：crpi-7vifw4ok9jkcxr60.cn-hongkong.personal.cr.aliyuncs.com/titan-core/ ；用户名：sean_hui
ACR_REGISTRY ?= crpi-7vifw4ok9jkcxr60.cn-hongkong.personal.cr.aliyuncs.com
ACR_REPO ?= titan-core/diting-ingest
ACR_USERNAME ?= sean_hui
ACR_IMAGE := $(ACR_REGISTRY)/$(ACR_REPO):latest
push-images: build-images
	@docker tag diting-ingest:test $(ACR_IMAGE); \
	if [ -n "$$DITING_ACR_PASSWORD" ]; then \
	  echo "$$DITING_ACR_PASSWORD" | docker login $(ACR_REGISTRY) -u $(ACR_USERNAME) --password-stdin; \
	fi; \
	docker push $(ACR_IMAGE) && echo "push-images: $(ACR_IMAGE) OK"

# ---------- Stage2 本地实践：L1/L2 编排与建表归属 diting-infra（02_三位一体仓库规约）----------
# 请在 diting-infra 执行 make local-deps-up、make local-deps-init 后，在本仓配置 .env 指向 localhost:15432/15433，再执行 verify-db-connection、ingest-test。回收时在 diting-infra 执行 make local-deps-down。
