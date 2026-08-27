# FinFlow — developer entrypoints.
# Every target is safe to run repeatedly.

.DEFAULT_GOAL := help
.PHONY: help install lint format typecheck test test-fast cov check clean up down demo backfill daily docs

PYTHON_VERSION := 3.12

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install:  ## Create the virtualenv and install all dependencies
	uv python install $(PYTHON_VERSION)
	uv sync --all-extras
	uv run pre-commit install

lint:  ## Check formatting and lint rules
	uv run ruff check src tests
	uv run ruff format --check src tests

format:  ## Apply formatting and auto-fixable lint rules
	uv run ruff check --fix src tests
	uv run ruff format src tests

typecheck:  ## Run mypy in strict mode
	uv run mypy

test:  ## Run the full test suite with coverage
	uv run pytest --cov --cov-report=term-missing

test-fast:  ## Run only unit tests, no coverage
	uv run pytest -m "not integration" -q --no-cov

cov:  ## Write an HTML coverage report to htmlcov/
	uv run pytest --cov --cov-report=html
	@echo "Report: htmlcov/index.html"

check: lint typecheck test  ## Everything CI runs, locally

clean:  ## Remove caches and build artifacts
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage dist build
	find . -type d -name __pycache__ -prune -exec rm -rf {} +

# ---- Placeholders, implemented in later milestones -----------------------

up:  ## Start local services (M6)
	@echo "Not implemented until M6."

down:  ## Stop local services (M6)
	@echo "Not implemented until M6."

demo:  ## Seed data and run the pipeline offline (M10)
	@echo "Not implemented until M10."

backfill:  ## Backfill the full instrument universe (M2)
	@echo "Not implemented until M2."

daily:  ## Run the daily pipeline: ingest -> load -> dbt -> evaluate -> deliver (M4)
	@echo "Not implemented until M4."

docs:  ## Generate dbt docs (M3)
	@echo "Not implemented until M3."
