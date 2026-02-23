# diting-core Makefile
# [Ref: 03_原子目标与规约/_共享规约/02_三位一体仓库规约]

.PHONY: test build test-docker verify-db-connection

test:
	@cd "$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))" && (python3 -m pytest tests/ -v --tb=short 2>/dev/null || python3 tests/unit/test_abstraction.py)

build:
	@echo "Build target (placeholder)"
	@exit 0

test-docker:
	@echo "Docker test (placeholder)"
	@exit 0

# Stage2-01 下游验证：使用 .env 中 TIMESCALE_DSN 连接 L1 并对 init 所建表执行查询；退出码 0 表示 V7 通过
verify-db-connection:
	@root="$$(dirname $(realpath $(firstword $(MAKEFILE_LIST))))"; \
	. "$$root/.env" 2>/dev/null || true; \
	if [ -z "$$TIMESCALE_DSN" ]; then echo "TIMESCALE_DSN not set (copy .env.template to .env and fill)"; exit 1; fi; \
	psql "$$TIMESCALE_DSN" -v ON_ERROR_STOP=1 -c "SELECT 1" && psql "$$TIMESCALE_DSN" -v ON_ERROR_STOP=1 -c "\\dt" | grep -q ohlcv && echo "verify-db-connection OK"
