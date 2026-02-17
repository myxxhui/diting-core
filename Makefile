# diting-core Makefile
# [Ref: 03_Stage1_仓库与骨架/01_三位一体仓库设计] Stage1-01 占位
# [Ref: 03_Stage1_仓库与骨架/02_核心接口与Proto设计] Stage1-02 单测通过

.PHONY: test build test-docker

test:
	python3 -m pytest tests/ -v --tb=short

build:
	@echo "TODO: build target"
	@exit 0

test-docker:
	@echo "TODO: test-docker target"
	@exit 0
