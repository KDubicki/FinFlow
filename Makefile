# FinFlow — developer entrypoints.
# Every target is safe to run repeatedly.

.DEFAULT_GOAL := help
.PHONY: help install lint format typecheck imports registry dialect audit test test-fast cov check clean test-live up down demo backfill backfill-offline build dbt-deps daily docs

PYTHON_VERSION := 3.12

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install:  ## Create the virtualenv and install all dependencies
	uv python install $(PYTHON_VERSION)
	uv sync --all-extras
	uv run pre-commit install
	cd dbt && DBT_PROFILES_DIR=. uv run dbt deps

lint:  ## Check formatting and lint rules
	uv run ruff check src tests
	uv run ruff format --check src tests

format:  ## Apply formatting and auto-fixable lint rules
	uv run ruff check --fix src tests
	uv run ruff format src tests

typecheck:  ## Run mypy in strict mode
	uv run mypy

imports:  ## Check the dependency rule
	uv run lint-imports

registry:  ## Validate every instruments/*.yml
	uv run python scripts/validate_registry.py

dialect:  ## Check mart SQL stays portable
	uv run python scripts/check_dialect_neutrality.py

audit:  ## Check dependencies against known advisories
	uv run pip-audit --strict

test:  ## Run the unit suite with coverage (no network)
	uv run pytest -m "not integration" --cov --cov-report=term-missing

test-live:  ## Run live vendor tests (network; never on a PR)
	uv run pytest -m integration -v --no-cov

test-fast:  ## Run only unit tests, no coverage
	uv run pytest -m "not integration" -q --no-cov

cov:  ## Write an HTML coverage report to htmlcov/
	uv run pytest --cov --cov-report=html
	@echo "Report: htmlcov/index.html"

check: lint typecheck imports registry dialect test  ## Everything CI runs, locally

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

backfill:  ## Backfill the full instrument universe
	uv run finflow-backfill $(ARGS)

backfill-offline:  ## Backfill from the synthetic source, no network
	uv run finflow-backfill --offline $(ARGS)

daily:  ## Run the daily pipeline: ingest -> load -> dbt -> evaluate -> deliver (M4)
	@echo "Not implemented until M4."

dbt-deps:  ## Install dbt packages from the committed lockfile
	cd dbt && DBT_PROFILES_DIR=. uv run dbt deps

build:  ## Load the raw zone into the warehouse and run dbt
	uv run finflow-build $(ARGS)

docs:  ## Generate dbt docs
	cd dbt && DBT_PROFILES_DIR=. uv run dbt docs generate
	@echo "Browse with: cd dbt && uv run dbt docs serve"
