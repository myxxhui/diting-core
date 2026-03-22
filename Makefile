# diting-core Makefile
# [Ref: 03_原子目标与规约/_共享规约/02_三位一体仓库规约]

.PHONY: test build test-docker verify verify-db-connection verify-data-test verify-data-production query-ingest-overview check-ohlcv-consistency ingest-test ingest-deploy ingest-test-real ingest-production ingest-production-background ingest-production-incremental ingest-production-fast ingest-production-incremental-fast fetch-sector-symbols deps-ingest ingest-business-profile build-images build-module-a deps-classifier deps-scanner deps-scanner-alphalens diting prod run-module-a query-module-a-output init-l2-classifier-table init-l2-industry-revenue-table init-l2-business-profile-tables init-l2-b-module-tables run-module-b query-module-b-output verify-module-b init-l2-quant-signal-table prune-l2-quant-scan-all factor-quality-smoke init-l2-symbol-names-table sync-symbol-names-csv test-scanner golden-scanner-batch run-module-c query-module-c-output verify-module-c init-l2-moe-opinion-table test-moe deps-moe

# 采集相关 target 使用的 Python：akshare 推荐 >= 3.9。Make 解析时用非交互 shell，需先加载 pyenv 才能找到 pyenv 安装的 3.9
PYTHON_INGEST := $(shell export PYENV_ROOT="$$HOME/.pyenv" && [ -d "$$PYENV_ROOT/bin" ] && export PATH="$$PYENV_ROOT/bin:$$PATH" && eval "$$(pyenv init - 2>/dev/null)" 2>/dev/null; command -v python3.11 2>/dev/null || command -v python3.10 2>/dev/null || command -v python3.9 2>/dev/null || command -v python3.8 2>/dev/null || command -v python3 2>/dev/null)
# Stage3-02 扫描引擎：TA-Lib 需 Python 3.7+ 且系统已装 ta-lib C 库；优先 python3.8 以使用已安装的 TA-Lib
PYTHON_SCANNER := $(shell command -v python3.8 2>/dev/null || command -v python3.9 2>/dev/null || command -v python3 2>/dev/null)

# 语义分类器单测依赖（无 pip 时用 python3 -m pip）；安装后执行 make test-classifier-cov
deps-classifier:
	@python3 -m pip install pytest pytest-cov PyYAML

# Stage3-02 扫描引擎依赖：numpy/PyYAML/psycopg2/TA-Lib；TA-Lib 需先安装系统层 ta-lib C 库（见 02_量化扫描引擎_实践）；含 pytest 供 make test-scanner
deps-scanner:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	py="$(PYTHON_SCANNER)"; [ -z "$$py" ] && { echo "错误: 未找到 python3.8/python3.9/python3"; exit 1; }; \
	echo "使用 $$py 安装扫描引擎依赖..."; \
	cd "$$root" && $$py -m pip install -r requirements-scanner.txt && $$py -m pip install pytest -q && echo "deps-scanner OK（可执行 make run-module-b / make verify-module-b / make test-scanner）。若 L2 报缺表：make init-l2-b-module-tables"

# Alphalens 因子烟测可选依赖（见 requirements-scanner-alphalens.txt 内 Peewee 说明）；不影响主扫描链
deps-scanner-alphalens:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	py="$(PYTHON_SCANNER)"; [ -z "$$py" ] && py=python3; \
	cd "$$root" && $$py -m pip install -r requirements-scanner-alphalens.txt && echo "deps-scanner-alphalens OK（可 make factor-quality-smoke）"

# Stage2-06 采集依赖（真实行情）：akshare、psycopg2、redis 等；06_ 步骤 3、7、8 执行前须先 make deps-ingest。akshare 要求 >= 3.8（3.9+ 无告警，若系统有 3.9+ 会优先用）
# 先装 akshare --no-deps 再装 core 依赖，避免 akshare 1.18+ 的 curl_cffi>=0.13 与部分环境不兼容
deps-ingest:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	py="$(PYTHON_INGEST)"; [ -z "$$py" ] && { echo "错误: 未找到 python3.8/python3.9/python3"; exit 1; }; \
	$$py -c "import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)" || { echo "错误: akshare 要求 Python >= 3.8，当前: $$($$py --version 2>&1)。请安装 Python 3.8+ 或使用 pyenv/conda。"; exit 1; }; \
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

# 一键查询采集已落库数据：标的列表、中文名、K线条数/日期范围、新闻/行业/财务概况（见 06_生产级数据要求_实践）
query-ingest-overview:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	cd "$$root" && PYTHONPATH="$$root" python3 scripts/query_ingest_data_overview.py

# DSN 与表一致性诊断：与 ingest 同源 config 取 DSN，列出 L1 ohlcv 全表概况与 symbol 样本、L2 industry_revenue_summary 样本（见 06_生产级数据要求_实践）
check-ohlcv-consistency:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	cd "$$root" && PYTHONPATH="$$root" python3 scripts/check_ohlcv_table_consistency.py

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
# 执行前仅同步「连接相关」变量（TIMESCALE_DSN、PG_L2_DSN、REDIS_URL、KUBECONFIG、PUBLIC_IP）从 prod.conn 到 .env，不覆盖其它配置。
# 可通过 PROD_CONN_FILE 指定 prod.conn 路径，未设则使用 ../diting-infra/prod.conn（相对本仓根目录）。
# 不在此处 export 默认值，由脚本加载 .env；未设置的变量才用脚本内默认参数。
CONN_KEYS_PATTERN = ^(TIMESCALE_DSN|PG_L2_DSN|REDIS_URL|KUBECONFIG|PUBLIC_IP)=
ingest-production:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	conn_file="$(PROD_CONN_FILE)"; [ -z "$$conn_file" ] && conn_file="$$root/../diting-infra/prod.conn"; \
	if [ -f "$$conn_file" ]; then \
		if [ ! -f "$$root/.env" ]; then \
			cp "$$conn_file" "$$root/.env"; \
			echo "已从 prod.conn 创建 .env（首次）"; \
		else \
			_p=$$(grep -E '^(TIMESCALE_DSN|PG_L2_DSN|REDIS_URL)=' "$$conn_file" 2>/dev/null | sort); \
			_e=$$(grep -E '^(TIMESCALE_DSN|PG_L2_DSN|REDIS_URL)=' "$$root/.env" 2>/dev/null | sort); \
			if [ "$$_p" != "$$_e" ]; then \
				_t="$$root/.env.ingest.tmp"; \
				grep -E '$(CONN_KEYS_PATTERN)' "$$conn_file" 2>/dev/null > "$$_t.conn"; \
				grep -v -E '$(CONN_KEYS_PATTERN)' "$$root/.env" 2>/dev/null > "$$_t.other"; \
				cat "$$_t.conn" "$$_t.other" > "$$root/.env"; \
				rm -f "$$_t.conn" "$$_t.other"; \
				echo "已用 prod.conn 仅同步连接相关变量到 .env（其它配置未改动）"; \
			fi; \
		fi; \
	fi; \
	cd "$$root" && PYTHONPATH="$$root" $(PYTHON_INGEST) scripts/run_ingest_production.py

# 同上，但后台运行：nohup 将输出写入 INGEST_PROD_LOG（默认 ingest-production.log），关终端不杀进程；查看进度: tail -f $(INGEST_PROD_LOG)
INGEST_PROD_LOG ?= ingest-production.log
ingest-production-background:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	log="$(INGEST_PROD_LOG)"; \
	case "$$log" in /*) ;; *) log="$$root/$$log";; esac; \
	echo "后台启动 make ingest-production，日志: $$log"; \
	cd "$$root" && nohup $(MAKE) ingest-production >> "$$log" 2>&1 & \
	_pid=$$!; echo "PID=$$_pid"; echo "查看进度: tail -f $$log"

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

# 从 akshare 拉取「有色金属」「电力」板块成分股并追加到 config/diting_symbols.txt（需先 make deps-ingest）
fetch-sector-symbols:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	cd "$$root" && PYTHONPATH="$$root" $(PYTHON_INGEST) scripts/fetch_sector_symbols_and_append.py

# Stage2-02 一键构建本阶段所涉全部镜像（仅构建；推送用 make push-images）[Ref: 02_采集逻辑与Dockerfile_实践, 01_语义分类器_实践]
DOCKER_PLATFORM ?= linux/amd64
ACR_REGISTRY ?= crpi-7vifw4ok9jkcxr60.cn-hongkong.personal.cr.aliyuncs.com
ACR_REPO ?= titan-core/diting-ingest
ACR_REPO_MODULE_A ?= titan-core/diting-module-a
ACR_USERNAME ?= sean_hui
# ACR 登录密码：在下一行直接赋值（或运行时 export）。勿将含真实密码的 Makefile 提交到仓库。
DITING_ACR_PASSWORD ?= Hui123123
# 若在此仓写死密码，可改为例如：DITING_ACR_PASSWORD ?= 你的ACR密码Hui123123
ACR_IMAGE := $(ACR_REGISTRY)/$(ACR_REPO):latest
ACR_IMAGE_MODULE_A := $(ACR_REGISTRY)/$(ACR_REPO_MODULE_A):latest

build-images: build-module-a build-module-b
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	cd "$$root" && docker build --platform $(DOCKER_PLATFORM) -f Dockerfile.ingest -t diting-ingest:test .
	@echo "build-images: diting-ingest:test + diting-module-a:test + diting-module-b:latest OK ($(DOCKER_PLATFORM))"

# Stage3-01 Module A 语义分类器镜像 [Ref: 01_语义分类器_实践, global_const.deployable_units.module_a]
build-module-a:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	cd "$$root" && docker build --platform $(DOCKER_PLATFORM) -f docker/module_a/Dockerfile -t diting-module-a:test .
	@echo "build-module-a: diting-module-a:test OK ($(DOCKER_PLATFORM))"

# Stage3-02 Module B 量化扫描引擎镜像 [Ref: 02_量化扫描引擎_实践, global_const.deployable_units.module_b]；镜像内含 TA-Lib 系统库与 Python 绑定
build-module-b:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	cd "$$root" && docker build --platform $(DOCKER_PLATFORM) -f docker/module_b/Dockerfile -t diting-module-b:latest .
	@echo "build-module-b: diting-module-b:latest OK ($(DOCKER_PLATFORM))"

# Stage3-01 一键本地运行 A 模块：加载 .env，执行分类，输出执行标的、执行结果、写入位置 [Ref: 01_语义分类器_实践]
run-module-a:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	_saved_ds="$$DITING_SYMBOLS"; _saved_mab="$$MODULE_AB_SYMBOLS"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	if [ -n "$$_saved_ds" ]; then export DITING_SYMBOLS="$$_saved_ds"; fi; \
	if [ -n "$$_saved_mab" ]; then export MODULE_AB_SYMBOLS="$$_saved_mab"; fi; \
	cd "$$root" && PYTHONPATH="$$root" python3 scripts/run_module_a_local.py

# Stage3-01 一键查询 A 模块写入的数据：L2 表 classifier_output_snapshot（需 PG_L2_DSN 可达）[Ref: 01_语义分类器_实践]
query-module-a-output:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	cd "$$root" && PYTHONPATH="$$root" python3 scripts/query_classifier_output.py

# Stage3-01 在 L2 库中创建 classifier_output_snapshot 表（若不存在）；L2 可达但报「表可能未创建」时先执行此目标 [Ref: 01_语义分类器_实践]
init-l2-classifier-table:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	cd "$$root" && PYTHONPATH="$$root" python3 scripts/init_l2_classifier_table.py

# Stage3-02 在 L2 库中创建 industry_revenue_summary（若不存在）；板块强度/行业映射与 Module A 同源 [Ref: 11_数据采集与输入层规约]
init-l2-industry-revenue-table:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	py="$(PYTHON_SCANNER)"; [ -z "$$py" ] && py=python3; \
	cd "$$root" && PYTHONPATH="$$root" $$py scripts/init_l2_industry_revenue_summary.py

# L2：segment_registry + symbol_business_profile（主营构成，Module A segment_shares）[Ref: 12_右脑数据支撑与Segment规约]
init-l2-business-profile-tables:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	cd "$$root" && PYTHONPATH="$$root" python3 scripts/init_l2_business_profile_tables.py

# 主营构成批量入库（AkShare stock_zygc_em → L2）；申万「电力」等须依赖本数据才可不标未知。须 make deps-ingest；可选 INGEST_BUSINESS_BATCH_PAUSE_SEC
# 注意：先保存命令行传入的 DITING_SYMBOLS，再 source .env，避免 .env 内 DITING_SYMBOLS 覆盖本次显式指定
ingest-business-profile:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	_saved_ds="$$DITING_SYMBOLS"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	if [ -n "$$_saved_ds" ]; then export DITING_SYMBOLS="$$_saved_ds"; fi; \
	cd "$$root" && PYTHONPATH="$$root" $(PYTHON_INGEST) scripts/run_ingest_business_profile_batch.py

# B 模块常用 L2 表：分类器快照、量化快照、行业汇总、主营构成（按需执行，首次连库或报 relation does not exist 时）
init-l2-b-module-tables: init-l2-classifier-table init-l2-quant-signal-table init-l2-industry-revenue-table init-l2-business-profile-tables
	@echo "init-l2-b-module-tables OK（classifier_output_snapshot / quant_signal_* / industry_revenue_summary / segment_registry+symbol_business_profile）"

# Stage3-02 一键本地运行 B 模块：基于 A 同源标的执行扫描，结果写入 L2 quant_signal_snapshot 供 Module C 使用 [Ref: 02_量化扫描引擎_实践]（需先 make deps-scanner；L2 缺表时 make init-l2-b-module-tables）
# macOS：Homebrew 安装的 ta-lib 为 libta-lib.dylib，Python 包需 libta_lib；若存在 .ta_lib_link 则加入 DYLD_LIBRARY_PATH
run-module-b:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	[ -d "$$root/.ta_lib_link" ] && export DYLD_LIBRARY_PATH="$$root/.ta_lib_link:/opt/homebrew/opt/ta-lib/lib:$${DYLD_LIBRARY_PATH:-}"; true; \
	py="$(PYTHON_SCANNER)"; [ -z "$$py" ] && py=python3; \
	cd "$$root" && PYTHONPATH="$$root" $$py scripts/run_module_b_local.py

# Stage3-02 一键查询 B 模块写入的数据：L2 表 quant_signal_snapshot（需 PG_L2_DSN 可达）[Ref: 02_量化扫描引擎_实践]
# 可选位置参数：make query-module-b-output 3-22（日历日，与 QUERY_SCANNER_DATE 等价）；QUERY_SCANNER_BATCH_INDEX=1 取当日倒数第二批
query-module-b-output:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	py="$(PYTHON_SCANNER)"; [ -z "$$py" ] && py=python3; \
	cd "$$root" && PYTHONPATH="$$root" $$py scripts/query_scanner_output.py $(filter-out $@,$(MAKECMDGOALS))

# 吞掉「make query-module-b-output 3-22」中额外目标（如 3-22），避免 No rule to make target
%:
	@:

# Stage3-02 B 模块功能验证：基于 A 同源标的跑扫描，校验输出格式与 L2 写入、是否符合预期 [Ref: 02_量化扫描引擎_实践]
# B 模块功能验证：默认用 Mock OHLCV 与跳过 akshare 补全，保证无库/无外网时可跑通；需真实 L1 时取消下一行注释并设 TIMESCALE_DSN
verify-module-b:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	export VERIFY_USE_MOCK_OHLCV=1 VERIFY_MODULE_B_SKIP_AKSHARE=1; \
	py="$(PYTHON_SCANNER)"; [ -z "$$py" ] && py=python3; \
	cd "$$root" && PYTHONPATH="$$root" $$py scripts/run_scanner_functional_verify.py

# Stage3-02 在 L2 库中创建 quant_signal_snapshot 表（若不存在）；L2 可达但报「表可能未创建」时先执行此目标 [Ref: 02_量化扫描引擎_实践]
init-l2-quant-signal-table:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	py="$(PYTHON_SCANNER)"; [ -z "$$py" ] && py=python3; \
	cd "$$root" && PYTHONPATH="$$root" $$py scripts/init_l2_quant_signal_table.py

# 按保留天数裁剪 L2 quant_signal_scan_all（需 PG_L2_DSN）；示例: make prune-l2-quant-scan-all EXTRA='--dry-run'
prune-l2-quant-scan-all:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	py="$(PYTHON_SCANNER)"; [ -z "$$py" ] && py=python3; \
	cd "$$root" && PYTHONPATH="$$root" $$py scripts/prune_quant_signal_scan_all.py $(EXTRA)

# Alphalens 因子分层管线烟测（无 alphalens 则 exit 0）；可选依赖见 requirements-scanner-alphalens.txt / make deps-scanner-alphalens
factor-quality-smoke:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	py="$(PYTHON_SCANNER)"; [ -z "$$py" ] && py=python3; \
	cd "$$root" && PYTHONPATH="$$root" $$py scripts/factor_quality_smoke.py

# Stage3-02 在 L2 库中创建 symbol_names 表（标的中文名持久化）；优先从该表读，缺失时 akshare 拉取并落库
init-l2-symbol-names-table:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	py="$(PYTHON_SCANNER)"; [ -z "$$py" ] && py=python3; \
	cd "$$root" && PYTHONPATH="$$root" $$py scripts/init_l2_symbol_names_table.py

# 将 config/symbol_names.csv 同步到 L2 symbol_names（关闭 INGEST_SYMBOL_NAMES 时手工维护 CSV 后用）[Ref: 02_量化扫描引擎_实践]
sync-symbol-names-csv:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	py="$(PYTHON_SCANNER)"; [ -z "$$py" ] && py=python3; \
	cd "$$root" && PYTHONPATH="$$root" $$py scripts/sync_symbol_names_csv_to_l2.py

# Stage3-04 Module C：pytest（MoE 议会）[Ref: 04_A轨_MoE议会_实践]
test-moe:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	cd "$$root" && PYTHONPATH="$$root" python3 -m pytest tests/unit/test_moe.py tests/unit/test_b_track.py -v --tb=short

# Module C 与 B 共用 TA-Lib 扫描链；一键跑 C 前请先 make deps-scanner
deps-moe: deps-scanner
	@echo "deps-moe OK（同 deps-scanner，可 make run-module-c）"

# Stage3-04 在 L2 创建 moe_expert_opinion_snapshot（Module C 输出表）
init-l2-moe-opinion-table:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	cd "$$root" && PYTHONPATH="$$root" python3 scripts/init_l2_moe_expert_opinion_table.py

# Stage3-04 一键运行 C（唯一入口）。已配 PG_L2_DSN 且未设 MOE_PIPELINE 时默认从 L2 读 A+B；本机当场重算 B 请 export MOE_PIPELINE=full（须 TIMESCALE_DSN、deps-scanner）
run-module-c:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	[ -d "$$root/.ta_lib_link" ] && export DYLD_LIBRARY_PATH="$$root/.ta_lib_link:/opt/homebrew/opt/ta-lib/lib:$${DYLD_LIBRARY_PATH:-}"; true; \
	py="$(PYTHON_SCANNER)"; [ -z "$$py" ] && py=python3; \
	cd "$$root" && MOE_STUB_SEGMENT_SIGNALS=1 PYTHONPATH="$$root" $$py scripts/run_module_c_local.py

# Stage3-04 一键查询 C 写入：L2 moe_expert_opinion_snapshot
query-module-c-output:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	[ -f "$$root/.env" ] && . "$$root/.env"; true; \
	cd "$$root" && PYTHONPATH="$$root" python3 scripts/query_module_c_output.py

# Stage3-04 C 模块烟测：MoE 单测 + Golden batch（不跑全量 A/B）
verify-module-c:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	py="$(PYTHON_SCANNER)"; [ -z "$$py" ] && py=python3; \
	cd "$$root" && PYTHONPATH="$$root" $$py -m pytest tests/unit/test_moe.py tests/unit/test_b_track.py -q --tb=line && \
	PYTHONHASHSEED=0 env -u PG_L2_DSN -u TIMESCALE_DSN PYTHONPATH="$$root" $$py scripts/golden_scanner_batch.py

# Stage3-02 扫描引擎单测 [Ref: 02_量化扫描引擎_实践]；PYTHONHASHSEED=0 保证 mock OHLCV 与 Golden batch 可复现
test-scanner:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	py="$(PYTHON_SCANNER)"; [ -z "$$py" ] && py=python3; \
	cd "$$root" && PYTHONHASHSEED=0 PYTHONPATH="$$root" $$py -m pytest tests/unit/test_scanner.py tests/unit/test_sector_strength.py tests/unit/test_golden_scanner_batch.py -v --tb=short

# Golden batch：固定标的 + 分数区间（须无 PG_L2_DSN/TIMESCALE_DSN，与 test_golden_scanner_batch 一致）
golden-scanner-batch:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	py="$(PYTHON_SCANNER)"; [ -z "$$py" ] && py=python3; \
	cd "$$root" && PYTHONHASHSEED=0 env -u PG_L2_DSN -u TIMESCALE_DSN PYTHONPATH="$$root" $$py scripts/golden_scanner_batch.py

# 构建并推送到 ACR（密码取 Makefile 中 DITING_ACR_PASSWORD 或环境变量）
push-images: build-images
	@if [ -z "$(DITING_ACR_PASSWORD)" ]; then echo "错误: 请在 Makefile 中为 DITING_ACR_PASSWORD 赋值或 export DITING_ACR_PASSWORD"; exit 1; fi; \
	echo "$(DITING_ACR_PASSWORD)" | docker login $(ACR_REGISTRY) -u $(ACR_USERNAME) --password-stdin || { echo "登录失败，请检查 DITING_ACR_PASSWORD 与账号是否有 $(ACR_REGISTRY) 的推送权限"; exit 1; }; \
	docker tag diting-ingest:test $(ACR_IMAGE) && docker push $(ACR_IMAGE) && echo "push $(ACR_IMAGE) OK" || { echo "推送 ingest 失败；若报 requested access denied，请确认账号对 titan-core 命名空间有推送权限"; exit 1; }; \
	docker tag diting-module-a:test $(ACR_IMAGE_MODULE_A) && docker push $(ACR_IMAGE_MODULE_A) && echo "push $(ACR_IMAGE_MODULE_A) OK" || { echo "推送 module-a 失败；若报 requested access denied，请确认账号对 titan-core 命名空间有推送权限"; exit 1; }

# 仅推送 Module A 镜像（假定已 make build-module-a）
push-module-a:
	@if [ -z "$(DITING_ACR_PASSWORD)" ]; then echo "错误: 请在 Makefile 中为 DITING_ACR_PASSWORD 赋值或 export"; exit 1; fi; \
	echo "$(DITING_ACR_PASSWORD)" | docker login $(ACR_REGISTRY) -u $(ACR_USERNAME) --password-stdin || exit 1; \
	docker tag diting-module-a:test $(ACR_IMAGE_MODULE_A) && docker push $(ACR_IMAGE_MODULE_A) && echo "push-module-a: $(ACR_IMAGE_MODULE_A) OK"

# ---------- Stage2 本地实践：L1/L2 编排与建表归属 diting-infra（02_三位一体仓库规约）----------
# 请在 diting-infra 执行 make local-deps-up、make local-deps-init 后，在本仓配置 .env 指向 localhost:15432/15433，再执行 verify-db-connection、ingest-test。回收时在 diting-infra 执行 make local-deps-down。
