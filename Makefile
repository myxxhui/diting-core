# diting-core Makefile [Ref: dna_dev_workflow workflow_stages[s0].verification_commands]
# 与 global_const.trinity_repos.repo_i.make_targets 一致

.PHONY: test build test-docker

test:
	PYTHONPATH=. python3 -m pytest tests/ -v --tb=short

build:
	echo "Placeholder: build target for Stage2"

test-docker:
	echo "Placeholder: test-docker for Stage2"
