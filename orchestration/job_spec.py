"""
orchestration/job_spec.py — Databricks Jobs API Spec Builders.

Module   : orchestration.job_spec
Concept  : Build job JSON programmatically instead of maintaining raw JSON files
When     : CI/CD pipelines that deploy jobs via the Databricks REST API or Terraform.

Job API hierarchy
-----------------
Job
  settings
    name, tags, schedule, email_notifications, webhook_notifications
    job_clusters[]         — cluster specs shared across tasks
    tasks[]                — ordered task definitions
      task_key             — unique name within the job
      depends_on[]         — upstream task keys (DAG edges)
      existing_cluster_id / job_cluster_key / new_cluster
      notebook_task / python_wheel_task / spark_python_task / dlt_task / sql_task
      retry_on_timeout, max_retries, min_retry_interval_millis
      timeout_seconds

Design
------
Each builder returns a plain dict — callers compose them and pass to
/api/2.1/jobs/create or the DABs jobs resource block.  No SDK dependency.

Public API
----------
build_autoscale_cluster(min_workers, max_workers, node_type, spark_version, photon) -> dict
build_fixed_cluster(num_workers, node_type, spark_version, photon) -> dict
build_notebook_task(notebook_path, base_params, source) -> dict
build_python_task(python_file, parameters) -> dict
build_dlt_task(pipeline_id) -> dict
build_task(task_key, task_config, cluster_key, depends_on, ...) -> dict
build_schedule(quartz_cron, timezone, pause_status) -> dict
build_email_notifications(on_failure, on_success, no_alert_for_skipped) -> dict
build_job_spec(name, tasks, job_clusters, schedule, ...) -> dict
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def build_autoscale_cluster(
    min_workers: int = 2,
    max_workers: int = 8,
    node_type: str = "i3.xlarge",
    spark_version: str = "15.4.x-scala2.12",
    photon: bool = True,
    spark_conf: Optional[Dict[str, str]] = None,
) -> dict:
    """
    Build a job cluster spec with autoscaling.

    Parameters
    ----------
    min_workers : int
    max_workers : int
    node_type : str
        AWS instance type or Azure VM size.
    spark_version : str
        Databricks Runtime version string.
    photon : bool
        Enable Photon vectorized engine (recommended for SQL-heavy workloads).
    spark_conf : dict | None
        Additional Spark config key-value pairs.

    Returns
    -------
    dict
        new_cluster spec dict for use in a task or job_cluster definition.
    """
    spec: dict = {
        "spark_version": spark_version,
        "node_type_id": node_type,
        "autoscale": {"min_workers": min_workers, "max_workers": max_workers},
        "spark_conf": spark_conf or {},
    }
    if photon:
        spec["runtime_engine"] = "PHOTON"
    return spec


def build_fixed_cluster(
    num_workers: int = 4,
    node_type: str = "i3.xlarge",
    spark_version: str = "15.4.x-scala2.12",
    photon: bool = True,
    spark_conf: Optional[Dict[str, str]] = None,
) -> dict:
    """
    Build a fixed-size job cluster spec.

    Use when workload is predictable and autoscaling overhead is unacceptable
    (e.g., latency-sensitive streaming jobs).

    Parameters
    ----------
    num_workers : int
    node_type : str
    spark_version : str
    photon : bool
    spark_conf : dict | None

    Returns
    -------
    dict
    """
    spec: dict = {
        "spark_version": spark_version,
        "node_type_id": node_type,
        "num_workers": num_workers,
        "spark_conf": spark_conf or {},
    }
    if photon:
        spec["runtime_engine"] = "PHOTON"
    return spec


def build_notebook_task(
    notebook_path: str,
    base_params: Optional[Dict[str, str]] = None,
    source: str = "WORKSPACE",
) -> dict:
    """
    Build a notebook_task config dict.

    Parameters
    ----------
    notebook_path : str
        Absolute workspace path or Repos path to the notebook.
    base_params : dict | None
        Key-value widget parameters passed to the notebook.
    source : str
        'WORKSPACE' or 'GIT' (when using Git folders / Repos).

    Returns
    -------
    dict
    """
    task: dict = {
        "notebook_path": notebook_path,
        "source": source,
    }
    if base_params:
        task["base_parameters"] = base_params
    return {"notebook_task": task}


def build_python_task(
    python_file: str,
    parameters: Optional[List[str]] = None,
    source: str = "WORKSPACE",
) -> dict:
    """
    Build a spark_python_task config dict.

    Parameters
    ----------
    python_file : str
        Path to the .py file.
    parameters : list[str] | None
        Command-line arguments passed to the script.
    source : str

    Returns
    -------
    dict
    """
    task: dict = {
        "python_file": python_file,
        "source": source,
    }
    if parameters:
        task["parameters"] = parameters
    return {"spark_python_task": task}


def build_dlt_task(pipeline_id: str) -> dict:
    """
    Build a pipeline_task config dict for triggering a DLT pipeline.

    Parameters
    ----------
    pipeline_id : str
        UUID of the DLT pipeline.

    Returns
    -------
    dict
    """
    return {"pipeline_task": {"pipeline_id": pipeline_id}}


def build_sql_task(
    query_id: Optional[str] = None,
    dashboard_id: Optional[str] = None,
    warehouse_id: str = "",
) -> dict:
    """
    Build a sql_task config dict for running a Databricks SQL query or dashboard.

    Parameters
    ----------
    query_id : str | None
        DBSQL saved query UUID.
    dashboard_id : str | None
        DBSQL dashboard UUID.
    warehouse_id : str
        SQL warehouse UUID to execute on.

    Returns
    -------
    dict

    Raises
    ------
    ValueError
        If neither query_id nor dashboard_id is provided.
    """
    if not query_id and not dashboard_id:
        raise ValueError("Either query_id or dashboard_id must be provided")
    task: dict = {"warehouse_id": warehouse_id}
    if query_id:
        task["query"] = {"query_id": query_id}
    if dashboard_id:
        task["dashboard"] = {"dashboard_id": dashboard_id}
    return {"sql_task": task}


def build_task(
    task_key: str,
    task_config: dict,
    cluster_key: Optional[str] = None,
    depends_on: Optional[List[str]] = None,
    max_retries: int = 1,
    min_retry_interval_millis: int = 60_000,
    timeout_seconds: int = 3600,
    run_if: str = "ALL_SUCCESS",
) -> dict:
    """
    Build a complete task definition for a job.

    Parameters
    ----------
    task_key : str
        Unique identifier for this task within the job.
    task_config : dict
        One of: build_notebook_task(), build_python_task(), build_dlt_task(), etc.
    cluster_key : str | None
        Key into job_clusters[] for the shared cluster.  None = new cluster per task
        (expensive).
    depends_on : list[str] | None
        task_keys this task depends on (DAG edges).
    max_retries : int
        Number of automatic retries on failure.  0 = no retry.
    min_retry_interval_millis : int
        Wait between retries (milliseconds).
    timeout_seconds : int
        Task timeout.  0 = no timeout.
    run_if : str
        'ALL_SUCCESS', 'AT_LEAST_ONE_SUCCESS', 'NONE_FAILED', 'ALL_DONE'.

    Returns
    -------
    dict
    """
    task: dict = {
        "task_key": task_key,
        "run_if": run_if,
        "max_retries": max_retries,
        "min_retry_interval_millis": min_retry_interval_millis,
        "timeout_seconds": timeout_seconds,
        **task_config,
    }
    if cluster_key:
        task["job_cluster_key"] = cluster_key
    if depends_on:
        task["depends_on"] = [{"task_key": k} for k in depends_on]
    return task


def build_schedule(
    quartz_cron: str,
    timezone: str = "UTC",
    pause_status: str = "UNPAUSED",
) -> dict:
    """
    Build a job schedule spec.

    Parameters
    ----------
    quartz_cron : str
        Quartz cron expression.  Example: '0 0 6 * * ?'  (daily at 06:00 UTC).
    timezone : str
        Java timezone ID.  Example: 'America/New_York'.
    pause_status : str
        'PAUSED' or 'UNPAUSED'.

    Returns
    -------
    dict
    """
    return {
        "quartz_cron_expression": quartz_cron,
        "timezone_id": timezone,
        "pause_status": pause_status,
    }


def build_email_notifications(
    on_failure: Optional[List[str]] = None,
    on_success: Optional[List[str]] = None,
    no_alert_for_skipped: bool = True,
) -> dict:
    """
    Build an email_notifications config for a job.

    Parameters
    ----------
    on_failure : list[str] | None
        Email addresses to notify on task or job failure.
    on_success : list[str] | None
        Email addresses to notify on job success.
    no_alert_for_skipped : bool
        Suppress notifications for skipped runs (e.g., due to concurrency policy).

    Returns
    -------
    dict
    """
    config: dict = {"no_alert_for_skipped_runs": no_alert_for_skipped}
    if on_failure:
        config["on_failure"] = on_failure
    if on_success:
        config["on_success"] = on_success
    return config


def build_job_cluster(key: str, cluster_spec: dict) -> dict:
    """
    Wrap a cluster spec as a named job_cluster entry.

    Parameters
    ----------
    key : str
        Unique key for this cluster within the job.
    cluster_spec : dict
        From build_autoscale_cluster() or build_fixed_cluster().

    Returns
    -------
    dict
    """
    return {"job_cluster_key": key, "new_cluster": cluster_spec}


def build_job_spec(
    name: str,
    tasks: List[dict],
    job_clusters: Optional[List[dict]] = None,
    schedule: Optional[dict] = None,
    email_notifications: Optional[dict] = None,
    tags: Optional[Dict[str, str]] = None,
    max_concurrent_runs: int = 1,
) -> dict:
    """
    Build a complete Databricks job spec for the Jobs API.

    Pass the returned dict to POST /api/2.1/jobs/create or embed in a DABs
    resources/jobs/*.yml file.

    Parameters
    ----------
    name : str
        Human-readable job name (must be unique within a workspace).
    tasks : list[dict]
        From build_task().
    job_clusters : list[dict] | None
        From build_job_cluster().  Shared clusters reused across tasks.
    schedule : dict | None
        From build_schedule().
    email_notifications : dict | None
        From build_email_notifications().
    tags : dict | None
        Key-value tags for cost attribution.
    max_concurrent_runs : int
        Maximum simultaneous runs.  1 = sequential (safe default).

    Returns
    -------
    dict

    Raises
    ------
    ValueError
        If name is empty or tasks is empty.
    """
    if not name:
        raise ValueError("Job name must not be empty")
    if not tasks:
        raise ValueError("tasks must not be empty — a job requires at least one task")

    spec: dict = {
        "name": name,
        "tasks": tasks,
        "max_concurrent_runs": max_concurrent_runs,
    }
    if job_clusters:
        spec["job_clusters"] = job_clusters
    if schedule:
        spec["schedule"] = schedule
    if email_notifications:
        spec["email_notifications"] = email_notifications
    if tags:
        spec["tags"] = tags
    return spec
