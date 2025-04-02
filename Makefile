# Makefile for databricks_toolkit

run:
	@echo "Running job: $(JOB)"
	. $(VENV_ACTIVATE) && python local_runner.py --job=$(JOB)

VENV_ACTIVATE=.venv/bin/activate

.PHONY: help test run-ingest run-pipeline lint format deploy-ingest run-ingest-remote sync

help:
	@echo "Available targets:"
	@echo "  make test         Run all tests"
	@echo "  make run-ingest   Run the ingest_customer job locally"
	@echo "  make sync         Pull latest changes from GitHub into Databricks Repos"

test:
	@echo "Running tests..."
	PYTHONPATH=. . $(VENV_ACTIVATE) && pytest tests

run-ingest:
	@echo "Running ingest_customer locally..."
	. $(VENV_ACTIVATE) && python local_runner.py

run-pipeline:
	@echo "Running job pipeline: $(PIPELINE)"
	. $(VENV_ACTIVATE) && python local_runner.py --pipeline=$(PIPELINE)

lint:
	. $(VENV_ACTIVATE) && flake8

format:
	. $(VENV_ACTIVATE) && black .

deploy-ingest:
	@echo "🚀 Deploying ingest_customer job to Databricks..."
	databricks jobs create \
		--profile databricks \
		--json-file jobs/ingest_customer_job.json

run-ingest-remote:
	@echo "⚡ Running ingest_customer job remotely..."
	databricks jobs run-now \
		--profile databricks \
		--job-id $(JOB_ID)

sync:
	@echo "🔄 Pulling latest changes from GitHub into Databricks Repos..."
	databricks repos update \
		--profile databricks \
		--path "/Repos/timpomaville663@gmail.com/databricks_toolkit" \
		--branch main

deploy-transform:
	@echo "🚀 Deploying transform_orders job to Databricks..."
	databricks jobs create \
		--profile databricks \
		--json-file jobs/transform_orders_job.json

run-transform-remote:
	@echo "⚡ Running transform_orders job remotely..."
	databricks jobs run-now \
		--profile databricks \
		--job-id $(JOB_ID)

deploy-transform:
	@echo "🚀 Deploying transform_orders job to Databricks..."
	databricks jobs create \
		--profile databricks \
		--json-file jobs/transform_orders_job.json

run-transform-remote:
	@echo "⚡ Running transform_orders job remotely..."
	databricks jobs run-now \
		--profile databricks \
		--job-id $(JOB_ID)

clean-remote:
	@echo "🧹 Deleting Databricks job ID: $(JOB_ID)..."
	databricks jobs delete \
		--profile databricks \
		--job-id $(JOB_ID)

deploy-pipeline:
	@echo "🚀 Deploying default_pipeline multi-task job to Databricks..."
	databricks jobs create \
		--profile databricks \
		--json-file jobs/default_pipeline_job.json

run-pipeline-remote:
	@echo "⚡ Running remote default_pipeline job..."
	databricks jobs run-now \
		--profile databricks \
		--job-id $(JOB_ID)