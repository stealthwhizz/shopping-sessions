# Local loop for Shopping Sessions. CTX picks the implementation: `.` = Rust (root Dockerfile),
# `reference` = DuckDB reference. Examples:
#   make fixtures                          # Rust image on the 36 fixtures
#   make run-dev check-dev CTX=reference IMAGE=shopping-ref
IMAGE    ?= shopping-sessions:latest
CTX      ?= .
CPUS     ?= 8
MEM      ?= 12g
PLATFORM ?= linux/amd64
DEV      ?= data/dev
ANSWER   ?= data/dev-answer
FIXTURES ?= fixtures
# Test scripts need polars; the workspace venv has it (system Python here does not).
PY       ?= $(if $(wildcard ../event/shopping/.venv/bin/python),../event/shopping/.venv/bin/python,python3)
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures dev run-dev check-dev

build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) $(CTX)

fixtures: build
	$(PY) scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

check-dev:
	$(PY) scripts/compare.py out $(ANSWER)
