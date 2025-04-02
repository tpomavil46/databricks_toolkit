# 🛠️ Databricks Toolkit – Command Reference

## ✅ Local Dev & Testing

| Command | Description |
|--------|-------------|
| `make test` | Run unit tests in `tests/` using `pytest`. |
| `make lint` | Run `flake8` for code linting. |
| `make format` | Run `black` to auto-format your Python files. |
| `make run JOB=ingest_customer` | Run a job locally using Spark on your machine. Executes `local_runner.py` with the specified job. |

---

## 🚀 Packaging & Deployment

| Command | Description |
|--------|-------------|
| `zip -r databricks_toolkit.zip . -x ...` | Manual command to create a clean zip package of your project, excluding `.venv`, `.git`, and OS files. |
| `databricks fs cp databricks_toolkit.zip dbfs:/Users/<email>/... --overwrite` | Uploads the zip to DBFS (no longer recommended). |
| `make deploy-ingest` | Deploy the `jobs/ingest_customer_job.json` spec to Databricks and create a job. Returns `job_id`. |
| `make deploy-transform` | Deploy the `jobs/transform_orders_job.json` spec to Databricks and create a job. Returns `job_id`. |
| `make deploy-pipeline` | Deploys the `default_pipeline` multi-task job with chaining in Databricks. |
| `make run-pipeline-remote JOB_ID=<job_id>` | Kicks off the multi-task pipeline job remotely in Databricks. |

---

## ⚡ Remote Job Execution

| Command | Description |
|--------|-------------|
| `make run-ingest-remote JOB_ID=<job_id>` | Starts a run of your deployed Databricks job by `job_id`. |
| `databricks runs get --run-id <run_id>` | Get the current status of a specific job run. |
| `databricks runs list --job-id <job_id>` | List recent runs for a specific job. |
| `databricks jobs list` | List all jobs in your workspace (by name and ID). |
| `databricks jobs delete --job-id <job_id>` | Clean up an old or broken job if needed. |
| `make run-transform-remote JOB_ID=<job_id>` | Starts a run of your deployed `transform_orders` job. |
| `make clean-remote JOB_ID=<job_id>` | Delete a remote Databricks job cleanly from the CLI. |

---

## ⚙️ Job JSON Files

These define how your job runs.

| Filename | Purpose |
|----------|---------|
| `jobs/ingest_customer_job.json` | Defines a Databricks job that runs `ingest_customer.py` using either an existing or new cluster. |
| `"existing_cluster_id"` | Used to run your code on your dev cluster (`dev-firestorm`). |
| `"python_file"` | Points to the `.py` file inside your Databricks workspace (e.g. `/Workspace/Users/<you>/...`). |
| `jobs/transform_orders_job.json` | Defines a Databricks job that runs `transform_orders.py` with parameters for path and table. |

---

## 🧠 Cluster & Path Essentials

| Thing | How to Find It |
|-------|----------------|
| Your cluster ID | Go to Compute → click your cluster → look in the URL: `clusters/<cluster-id>` |
| Your DBFS home | `dbfs:/Users/timpomaville663@gmail.com/` |
| Your workspace code path | `/Workspace/Users/timpomaville663@gmail.com/databricks_toolkit/` |

---

## 🧪 Manual Debugging Tips

| Command | Purpose |
|---------|---------|
| `cat jobs/ingest_customer_job.json | jq .` | Validates your job spec JSON. |
| `databricks clusters list` | Shows available cluster types and IDs. |
| `databricks fs ls dbfs:/...` | Lists files in DBFS to verify uploads. |

---

## 🔥 Project Structure

- `Makefile` – task runner for everything (`test`, `run`, `deploy`, etc.)
- `local_runner.py` – handles local job execution
- `jobs/*.py` – job logic (e.g., `ingest_customer`)
- `utils/` – shared helpers (`io`, `config`)
- `tests/` – test coverage

---

## 🔁 Repo Syncing

| Command | Description |
|---------|-------------|
| `make sync` | Pull the latest changes into your Databricks Repos directory. Use this after pushing to GitHub. |

---

## ✅ Pipelines (Local & Remote)

| Command | Description |
|---------|-------------|
| `make run-pipeline PIPELINE=default_pipeline` | Run a full pipeline locally (e.g., `ingest_customer` → `transform_orders`). |
| `make deploy-pipeline` | Deploy the full multi-task pipeline job to Databricks (e.g., from `jobs/default_pipeline_job.json`). |
| `make run-pipeline-remote JOB_ID=<job_id>` | Execute a remote pipeline run using the specified job ID. |

---

## 🧼 Cleanup

| Command | Description |
|---------|-------------|
| `make clean-remote JOB_ID=<job_id>` | Deletes a job from Databricks to clean up stale or unused jobs. |

## 🏁 What's Next

- Add `make deploy-transform`, `make run-transform-remote`
- Add `make clean-remote` to delete stale jobs
- Add GitHub Action (`ci.yml`) to run `make test && make lint`
- Parameterize paths for flexibility in dev/prod

---

✨ Built for high-velocity Databricks development, without the notebook hell.