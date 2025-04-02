# Makefile for databricks_toolkit

run:
	@echo "Running job: $(JOB)"
	source $(VENV_ACTIVATE) && python local_runner.py --job=$(JOB)

VENV_ACTIVATE=.venv/bin/activate

.PHONY: help test run-ingest

help:
	@echo "Available targets:"
	@echo "  make test         Run all tests"
	@echo "  make run-ingest   Run the ingest_customer job locally"

test:
	@echo "Running tests..."
	PYTHONPATH=. source $(VENV_ACTIVATE) && pytest tests

run-ingest:
	@echo "Running ingest_customer locally..."
	source $(VENV_ACTIVATE) && python local_runner.py

run-pipeline:
	@echo "Running job pipeline: $(PIPELINE)"
	source $(VENV_ACTIVATE) && python local_runner.py --pipeline=$(PIPELINE)

lint:
	source $(VENV_ACTIVATE) && flake8

format:
	source $(VENV_ACTIVATE) && black .

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

.PHONY: sync

sync:
	@echo "🔄 Pulling latest changes from GitHub into Databricks Repos..."
	databricks repos update \
		--profile databricks \
		--path "/Repos/timpomaville663@gmail.com/databricks_toolkit" \
		--branch main