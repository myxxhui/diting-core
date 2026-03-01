# diting-core Makefile
# [Ref: 03_原子目标与规约/_共享规约/02_三位一体仓库规约]

.PHONY: test build test-docker verify verify-db-connection ingest-test build-images deps-classifier diting prod

# 语义分类器单测依赖（无 pip 时用 python3 -m pip）；安装后执行 make test-classifier-cov
deps-classifier:
	@python3 -m pip install pytest pytest-cov PyYAML

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

# Stage2-02 一键构建本阶段所涉全部镜像 [Ref: 04_阶段规划与实践/Stage2_数据采集与存储/02_采集逻辑与Dockerfile_实践.md V-BUILD-ALL]
# 当前仅采集镜像；后续若有新增镜像在此追加即可。
build-images:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	cd "$$root" && docker build -f Dockerfile.ingest -t diting-ingest:test .
	@echo "build-images: diting-ingest:test OK"

# ---------- Stage2 本地实践：L1/L2 编排与建表归属 diting-infra（02_三位一体仓库规约）----------
# 请在 diting-infra 执行 make local-deps-up、make local-deps-init 后，在本仓配置 .env 指向 localhost:15432/15433，再执行 verify-db-connection、ingest-test。回收时在 diting-infra 执行 make local-deps-down。
