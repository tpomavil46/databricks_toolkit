# 📦 Pipelines

This folder contains multi-step workflows that orchestrate multiple jobs.

---

## 🧬 Structure

Each pipeline should define a `run(spark, **kwargs)` method.

### Example: `default_pipeline.py`

```python
def run(spark, **kwargs):
    from jobs.ingest_customer import run as ingest
    from jobs.transform_orders import run as transform

    ingest(spark, **kwargs)
    transform(spark, **kwargs)
```

---

## 🔁 Local Execution

Run a full pipeline locally:

```bash
make run-pipeline PIPELINE=default_pipeline
```

---

## ☁️ Remote Execution

Trigger a pipeline job on Databricks:

```bash
make run-pipeline-remote JOB_ID=<job_id>
```

You can retrieve `JOB_ID` after deploying your job spec with:

```bash
make deploy-pipeline
```

| `make deploy-kpi` | Deploy the `kpi_pipeline` multi-task job to Databricks |
| `make run-kpi-remote JOB_ID=<job_id>` | Trigger a remote run of your KPI pipeline |

---

## 💡 Guidelines

- Import and run jobs in the order they should execute
- Use `**kwargs` to pass shared parameters like `--input_path`, `--output_table`, etc.
- Avoid complex logic here — pipelines should orchestrate, not transform
- Create new pipelines as separate `.py` files, e.g. `kpi_pipeline.py`, `delta_lake_pipeline.py`, etc.

---

## 📁 Files

| Pipeline File            | Description                                |
|--------------------------|--------------------------------------------|
| `default_pipeline.py`    | Runs `ingest_customer` then `transform_orders` |
| `kpi_pipeline.py` | Simulates a multi-step KPI generation flow |
| _more coming soon..._    | Add your own!                              |

---

## 🧠 Tip

Pipelines are a great way to test multiple job steps together before moving them into full production workflows using Databricks Workflows or MLflow Pipelines.
