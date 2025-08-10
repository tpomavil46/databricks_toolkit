# Makefile for databricks_toolkit

VENV_ACTIVATE=.venv/bin/activate

.PHONY: help test lint format sync \
        run run-ingest run-pipeline \
        deploy-ingest run-ingest-remote \
        deploy-transform run-transform-remote \
        deploy-pipeline run-pipeline-remote \
        generate-job clean-remote \
        billing-costs billing-report billing-check

help:
	@echo "Available targets:"
	@echo "  make test                    Run unit tests"
	@echo "  make lint                    Run flake8 linter"
	@echo "  make format                  Auto-format code with black"
	@echo "  make sync                    Sync GitHub -> Databricks Repos"
	@echo "  make run JOB=<job>           Run a local job"
	@echo "  make run-pipeline PIPELINE=default_pipeline"
	@echo "  make deploy-ingest           Deploy ingest_customer job"
	@echo "  make run-ingest-remote JOB_ID=..."
	@echo "  make deploy-transform"
	@echo "  make run-transform-remote JOB_ID=..."
	@echo "  make deploy-pipeline"
	@echo "  make run-pipeline-remote JOB_ID=..."
	@echo "  make clean-remote JOB_ID=..."
	@echo "  make generate-job JOB=<job>  Generate JSON spec"
	@echo ""
	@echo "Billing Monitoring:"
	@echo "  make billing-costs YEAR=2025 MONTH=8    Get monthly costs"
	@echo "  make billing-report YEAR=2025 MONTH=8   Generate cost report"
	@echo "  make billing-check THRESHOLD=100        Check cost threshold"

test:
	@echo "Running tests..."
	PYTHONPATH=. . $(VENV_ACTIVATE) && pytest tests

lint:
	. $(VENV_ACTIVATE) && flake8

format:
	. $(VENV_ACTIVATE) && black .

sync:
	@echo "🔄 Pulling latest changes from GitHub into Databricks Repos..."
	databricks repos update \
		--profile databricks \
		--path "/Repos/timpomaville663@gmail.com/databricks_toolkit" \
		--branch main

run:
	@echo "Running job: $(JOB)"
	. $(VENV_ACTIVATE) && python local_runner.py --job=$(JOB)

run-ingest:
	@echo "Running ingest_customer locally..."
	. $(VENV_ACTIVATE) && python local_runner.py

run-pipeline:
	@echo "Running job pipeline: $(PIPELINE)"
	. $(VENV_ACTIVATE) && python local_runner.py --pipeline=$(PIPELINE)

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

clean-remote:
	@echo "🧹 Deleting Databricks job ID: $(JOB_ID)..."
	databricks jobs delete \
		--profile databricks \
		--job-id $(JOB_ID)

generate-job:
	@echo "🛠 Generating job spec for: $(JOB)"
	python scripts/generate_job_spec.py --job $(JOB)

deploy-kpi:
	@echo "🚀 Deploying kpi_pipeline job to Databricks..."
	databricks jobs create \
		--profile databricks \
		--json-file jobs/kpi_pipeline_job.json

run-kpi-remote:
	@echo "⚡ Running kpi_pipeline job remotely..."
	databricks jobs run-now \
		--profile databricks \
		--job-id $(JOB_ID)

deploy-generate-kpis:
	@echo "🚀 Deploying generate_kpis job to Databricks..."
	databricks jobs create \
		--profile databricks \
		--json-file jobs/generate_kpis_job.json

run-generate-kpis-remote:
	@echo "⚡ Running generate_kpis job remotely..."
	databricks jobs run-now \
		--profile databricks \
		--job-id $(JOB_ID)

# Billing Monitoring Commands

billing-costs:
	@echo "💰 Getting monthly costs for $(YEAR)-$(MONTH)..."
	. $(VENV_ACTIVATE) && python -c "\
from utils.billing_monitor import BillingMonitor, BillingCLI; \
monitor = BillingMonitor(); \
cli = BillingCLI(monitor); \
cli.run_monthly_report($(YEAR), $(MONTH))"

billing-report:
	@echo "📊 Generating comprehensive cost report for $(YEAR)-$(MONTH)..."
	. $(VENV_ACTIVATE) && python -c "\
from utils.billing_monitor import BillingMonitor; \
import json; \
monitor = BillingMonitor(); \
report = monitor.generate_cost_report($(YEAR), $(MONTH)); \
print(json.dumps(report, indent=2, default=str))"

billing-check:
	@echo "🔍 Checking cost threshold ($$(THRESHOLD))..."
	. $(VENV_ACTIVATE) && python -c "\
from utils.billing_monitor import BillingMonitor, BillingCLI; \
monitor = BillingMonitor(); \
cli = BillingCLI(monitor); \
cli.run_cost_check($(THRESHOLD))"

