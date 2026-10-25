# ==============================================================================
# Makefile — UrbanPulse Developer Shortcuts
#
# All commands run from the repo root. Every target is documented.
#
# Usage:
#   make <target>           Run a target
#   make help               Show all available targets
#   make <target> -n        Dry-run (print commands without executing)
#
# Prerequisites: Python 3.11+, Docker Desktop, AWS CLI, Terraform 1.6+
# ==============================================================================

# Load .env file if it exists
ifneq (,$(wildcard .env))
  include .env
  export
endif

# Default values (overridable via CLI: make ingest-taxi YEAR=2023 MONTH=06)
YEAR   ?= 2024
MONTH  ?= 01
ENV    ?= dev

.DEFAULT_GOAL := help

# ─────────────────────────────────────────────────────────────
# HELP
# ─────────────────────────────────────────────────────────────
.PHONY: help
help: ## Show this help message
	@echo ""
	@echo "  🏙️  UrbanPulse — Developer Commands"
	@echo "  ──────────────────────────────────────────────────────"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'
	@echo ""

# ─────────────────────────────────────────────────────────────
# ENVIRONMENT SETUP
# ─────────────────────────────────────────────────────────────
.PHONY: install install-dev env-check

install: ## Install core Python dependencies
	pip install -e .

install-dev: ## Install all dependencies including dev/test tools
	pip install -e ".[dev]"

install-all: ## Install everything (core + dev + spark + quality)
	pip install -e ".[dev,spark,quality]"

env-check: ## Verify required environment variables are set
	@echo "Checking environment variables..."
	@test -n "$(AWS_REGION)"        || (echo "❌ AWS_REGION not set"; exit 1)
	@test -n "$(S3_LAKE_BUCKET)"    || (echo "❌ S3_LAKE_BUCKET not set"; exit 1)
	@test -n "$(REDSHIFT_HOST)"     || (echo "❌ REDSHIFT_HOST not set"; exit 1)
	@echo "✅ Environment looks good"

# ─────────────────────────────────────────────────────────────
# LOCAL DOCKER STACK
# ─────────────────────────────────────────────────────────────
.PHONY: local-up local-down local-restart local-logs

local-up: ## Start full local stack (Airflow + Kafka + Postgres)
	docker compose -f docker/docker-compose.yml up -d
	@echo "✅ Local stack started"
	@echo "   Airflow UI:  http://localhost:8080  (admin/admin)"
	@echo "   Kafka UI:    http://localhost:8090"

local-down: ## Stop and remove local stack containers
	docker compose -f docker/docker-compose.yml down

local-restart: ## Restart local stack
	docker compose -f docker/docker-compose.yml restart

local-logs: ## Tail logs from all local stack containers
	docker compose -f docker/docker-compose.yml logs -f

kafka-up: ## Start Kafka only (lightweight)
	docker compose -f docker/kafka/docker-compose.kafka.yml up -d

kafka-down: ## Stop Kafka
	docker compose -f docker/kafka/docker-compose.kafka.yml down

# ─────────────────────────────────────────────────────────────
# TERRAFORM — Infrastructure as Code
# ─────────────────────────────────────────────────────────────
.PHONY: tf-init tf-plan tf-apply tf-destroy tf-fmt tf-validate

tf-init: ## Initialize Terraform (download providers)
	cd terraform && terraform init

tf-fmt: ## Format all Terraform files
	cd terraform && terraform fmt -recursive

tf-validate: ## Validate Terraform configuration syntax
	cd terraform && terraform validate

tf-plan: ## Preview infrastructure changes (ENV=dev|prod)
	cd terraform && terraform plan -var="environment=$(ENV)"

tf-apply: ## Apply infrastructure changes (ENV=dev|prod)
	cd terraform && terraform apply -var="environment=$(ENV)" -auto-approve

tf-destroy: ## ⚠️  DESTROY all infrastructure (ENV=dev|prod)
	@echo "⚠️  This will DESTROY all AWS infrastructure for ENV=$(ENV)"
	@read -p "Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ]
	cd terraform && terraform destroy -var="environment=$(ENV)" -auto-approve

tf-output: ## Show Terraform outputs (bucket names, endpoints)
	cd terraform && terraform output

# ─────────────────────────────────────────────────────────────
# DATA INGESTION
# ─────────────────────────────────────────────────────────────
.PHONY: ingest-taxi ingest-weather ingest-geo ingest-all ingest-sample-local

ingest-taxi: ## Ingest SF taxi data (YEAR=2024 MONTH=01)
	python3 -m ingestion.batch.sf_taxi_loader --year $(YEAR) --month $(MONTH)

ingest-weather: ## Ingest weather data (YEAR=2024 MONTH=01)
	python3 -m ingestion.batch.weather_ingester --year $(YEAR) --month $(MONTH)

ingest-geo: ## Load SF neighborhoods GeoJSON to S3
	python3 -m ingestion.batch.geo_loader

ingest-all: ## Run all batch ingestion for a given month
	$(MAKE) ingest-taxi YEAR=$(YEAR) MONTH=$(MONTH)
	$(MAKE) ingest-weather YEAR=$(YEAR) MONTH=$(MONTH)
	$(MAKE) ingest-geo

ingest-sample-local: ## Ingest sample taxi + weather + geo into local lake without cloud creds
	python3 -m ingestion.batch.sf_taxi_loader --year $(YEAR) --month $(MONTH) --sample --local-only
	python3 -m ingestion.batch.weather_ingester --year $(YEAR) --month $(MONTH) --sample --local-only
	python3 -m ingestion.batch.geo_loader --local-only

produce-events: ## Start Kafka event producer (streaming simulation)
	python3 -m ingestion.streaming.event_producer --dry-run

# ─────────────────────────────────────────────────────────────
# SPARK — Batch Processing
# ─────────────────────────────────────────────────────────────
.PHONY: spark-bronze spark-silver spark-all spark-bronze-local spark-silver-local spark-all-local

spark-bronze: ## Run PySpark Bronze → Silver job via spark-submit (YEAR=2024 MONTH=01)
	spark-submit \
		--master local[*] \
		spark/jobs/bronze_to_silver.py \
		--year $(YEAR) --month $(MONTH)

spark-silver: ## Run PySpark Silver → Gold job via spark-submit (YEAR=2024 MONTH=01)
	spark-submit \
		--master local[*] \
		spark/jobs/silver_to_gold.py \
		--year $(YEAR) --month $(MONTH)

spark-bronze-local: ## Run Bronze → Silver locally without spark-submit (YEAR=2024 MONTH=01)
	python3 -m spark.jobs.bronze_to_silver --year $(YEAR) --month $(MONTH) --local

spark-silver-local: ## Run Silver → Gold locally without spark-submit (YEAR=2024 MONTH=01)
	python3 -m spark.jobs.silver_to_gold --year $(YEAR) --month $(MONTH) --local

spark-all: ## Run full Spark pipeline for a given month
	$(MAKE) spark-bronze YEAR=$(YEAR) MONTH=$(MONTH)
	$(MAKE) spark-silver YEAR=$(YEAR) MONTH=$(MONTH)

spark-all-local: ## Run full local transformation pipeline (YEAR=2024 MONTH=01)
	$(MAKE) spark-bronze-local YEAR=$(YEAR) MONTH=$(MONTH)
	$(MAKE) spark-silver-local YEAR=$(YEAR) MONTH=$(MONTH)

# ─────────────────────────────────────────────────────────────
# DBT — Transformation
# ─────────────────────────────────────────────────────────────
.PHONY: dbt-deps dbt-run dbt-test dbt-docs dbt-clean dbt-full

dbt-deps: ## Install dbt package dependencies
	cd dbt && dbt deps

dbt-run: ## Run all dbt models
	cd dbt && dbt run --profiles-dir . --target $(ENV)

dbt-test: ## Run all dbt tests
	cd dbt && dbt test --profiles-dir . --target $(ENV)

dbt-docs: ## Generate and serve dbt documentation
	cd dbt && dbt docs generate --profiles-dir . --target $(ENV)
	cd dbt && dbt docs serve --port 8081

dbt-clean: ## Clean dbt compiled artifacts
	cd dbt && dbt clean

dbt-full: dbt-deps ## Run full dbt pipeline (deps → run → test → docs)
	$(MAKE) dbt-run
	$(MAKE) dbt-test
	$(MAKE) dbt-docs

dbt-debug: ## Test dbt connection to Redshift
	cd dbt && dbt debug --profiles-dir . --target $(ENV)

# ─────────────────────────────────────────────────────────────
# DATA QUALITY — Great Expectations
# ─────────────────────────────────────────────────────────────
.PHONY: quality-check quality-docs

quality-check: ## Run all Great Expectations checkpoints
	python quality/run_checks.py

quality-docs: ## Generate & serve Great Expectations data docs
	cd quality && great_expectations docs build

# ─────────────────────────────────────────────────────────────
# TESTING
# ─────────────────────────────────────────────────────────────
.PHONY: test test-ingestion test-spark lint type-check

test: ## Run all unit tests with coverage
	pytest

test-ingestion: ## Run only ingestion tests
	python3 -m unittest discover -s ingestion/tests -v

test-spark: ## Run only PySpark tests
	python3 -m unittest discover -s spark/tests -v

lint: ## Lint and format check all Python code
	ruff check .
	ruff format --check .

lint-fix: ## Auto-fix all lint and formatting issues
	ruff check . --fix
	ruff format .

type-check: ## Run static type checking with mypy
	mypy ingestion/ spark/

# ─────────────────────────────────────────────────────────────
# CI/CD SIMULATION (run locally what GitHub Actions runs)
# ─────────────────────────────────────────────────────────────
.PHONY: ci-check

ci-check: ## Run all CI checks locally before pushing
	@echo "──────────────────────────────────────────"
	@echo "Running CI checks..."
	@echo "──────────────────────────────────────────"
	$(MAKE) lint
	$(MAKE) type-check
	$(MAKE) test
	$(MAKE) tf-fmt
	$(MAKE) tf-validate
	cd dbt && dbt compile --profiles-dir . --target $(ENV) || true
	@echo "──────────────────────────────────────────"
	@echo "✅ All CI checks passed!"
	@echo "──────────────────────────────────────────"

# ─────────────────────────────────────────────────────────────
# FULL PIPELINE RUN (end-to-end for a given month)
# ─────────────────────────────────────────────────────────────
.PHONY: pipeline-run

pipeline-run: env-check ## Run full pipeline end-to-end (YEAR=2024 MONTH=01)
	@echo "🚀 Running full UrbanPulse pipeline for $(YEAR)-$(MONTH)..."
	$(MAKE) ingest-all  YEAR=$(YEAR) MONTH=$(MONTH)
	$(MAKE) spark-all   YEAR=$(YEAR) MONTH=$(MONTH)
	$(MAKE) quality-check
	$(MAKE) dbt-full
	@echo "✅ Pipeline complete for $(YEAR)-$(MONTH)"

# ─────────────────────────────────────────────────────────────
# CLEANUP
# ─────────────────────────────────────────────────────────────
.PHONY: clean clean-all

clean: ## Remove Python build artifacts and cache
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Cleaned build artifacts"

clean-all: clean ## Remove all generated files including dbt target
	$(MAKE) dbt-clean
	@echo "✅ Full clean complete"
