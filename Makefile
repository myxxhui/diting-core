# diting-core Makefile
# [Ref: 03_原子目标与规约/_共享规约/02_三位一体仓库规约]

.PHONY: test build test-docker verify verify-db-connection verify-data-test verify-data-production ingest-test ingest-test-real ingest-production deps-ingest build-images deps-classifier diting prod

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

# Stage2-06 全量生产级数据采集：先刷新全A股 universe，再按 universe 拉取单标≥5 年日线；步骤 8 必须用本 target，禁止用 ingest-test 代替。
ingest-production:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	cd "$$root" && PYTHONPATH="$$root" $(PYTHON_INGEST) scripts/run_ingest_production.py

# Stage2-02 一键构建本阶段所涉全部镜像 [Ref: 04_阶段规划与实践/Stage2_数据采集与存储/02_采集逻辑与Dockerfile_实践.md V-BUILD-ALL]
# 当前仅采集镜像；后续若有新增镜像在此追加即可。
build-images:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	cd "$$root" && docker build -f Dockerfile.ingest -t diting-ingest:test .
	@echo "build-images: diting-ingest:test OK"

# ---------- Stage2 本地实践：L1/L2 编排与建表归属 diting-infra（02_三位一体仓库规约）----------
# 请在 diting-infra 执行 make local-deps-up、make local-deps-init 后，在本仓配置 .env 指向 localhost:15432/15433，再执行 verify-db-connection、ingest-test。回收时在 diting-infra 执行 make local-deps-down。
