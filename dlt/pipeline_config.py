"""
dlt/pipeline_config.py — DLT Pipeline Settings Builders.

Module   : dlt.pipeline_config
Concept  : Build DLT pipeline configuration dicts for the Pipelines API
When     : Creating or updating a DLT pipeline via REST API, Terraform, or DABs.

Pipeline settings that matter
------------------------------
continuous vs triggered
    continuous=True  → pipeline runs 24/7, processes new data as it arrives.
    continuous=False → pipeline runs as a job (one update, then stops).
    Use triggered for batch workloads; continuous for low-latency streaming.

development vs production
    development=True  → resets the pipeline on each run (rebuilds all tables).
                        Errors surface immediately; no retries.
    development=False → incremental updates; retries on transient failures.
    Always use development=False in prod.

photon=True / serverless=True
    Mutually exclusive; serverless uses a managed cluster with Photon included.
    Serverless is the recommended default (no cluster management).

channel
    'CURRENT'  → stable DBR channel (recommended for prod).
    'PREVIEW'  → latest features; less stable.

catalog / target
    catalog  → Unity Catalog catalog name (metastore-aware pipelines).
    target   → Hive metastore schema (legacy; prefer catalog).
    Set one, not both.

Public API
----------
build_pipeline_settings(name, libraries, ...) -> dict
build_pipeline_cluster(label, num_workers, node_type, spark_conf) -> dict
build_notification_config(email_addresses, alerts) -> dict
"""

from __future__ import annotations

from typing import Dict, List, Optional


VALID_CHANNELS = {"CURRENT", "PREVIEW"}
VALID_ALERT_TYPES = {
    "on-update-success",
    "on-update-failure",
    "on-update-fatal-failure",
    "on-flow-failure",
}


def build_pipeline_settings(
    name: str,
    libraries: List[Dict],
    catalog: Optional[str] = None,
    target: Optional[str] = None,
    continuous: bool = False,
    development: bool = False,
    photon: bool = True,
    serverless: bool = False,
    channel: str = "CURRENT",
    configuration: Optional[Dict[str, str]] = None,
    clusters: Optional[List[dict]] = None,
    notifications: Optional[List[dict]] = None,
    edition: str = "ADVANCED",
) -> dict:
    """
    Build a DLT pipeline settings dict.

    Parameters
    ----------
    name : str
        Pipeline display name.
    libraries : list[dict]
        Source notebook or file references.
        Example: [{"notebook": {"path": "/Repos/me/project/pipeline_notebook"}}]
    catalog : str | None
        Unity Catalog catalog name.  Enables metastore-aware pipelines.
    target : str | None
        Hive metastore schema (legacy).  Use catalog instead for new pipelines.
    continuous : bool
        True = continuous processing; False = triggered (batch-style).
    development : bool
        True = development mode (full recompute on each run).
    photon : bool
        Enable Photon.  Ignored when serverless=True.
    serverless : bool
        Use serverless compute (recommended).  Overrides cluster config.
    channel : str
        'CURRENT' or 'PREVIEW'.
    configuration : dict | None
        Spark configuration key-value pairs passed to the pipeline.
    clusters : list[dict] | None
        From build_pipeline_cluster().  Ignored when serverless=True.
    notifications : list[dict] | None
        From build_notification_config().
    edition : str
        'CORE', 'PRO', or 'ADVANCED'.

    Returns
    -------
    dict

    Raises
    ------
    ValueError
        If name is empty, channel is invalid, or both catalog and target are set.
    """
    if not name:
        raise ValueError("Pipeline name must not be empty")
    if channel not in VALID_CHANNELS:
        raise ValueError(f"channel must be one of {VALID_CHANNELS}, got '{channel}'")
    if catalog and target:
        raise ValueError("Set either catalog (Unity Catalog) or target (Hive), not both")

    spec: dict = {
        "name": name,
        "libraries": libraries,
        "continuous": continuous,
        "development": development,
        "channel": channel,
        "edition": edition,
    }
    if serverless:
        spec["serverless"] = True
    else:
        if photon:
            spec["photon"] = True
        if clusters:
            spec["clusters"] = clusters

    if catalog:
        spec["catalog"] = catalog
    if target:
        spec["target"] = target
    if configuration:
        spec["configuration"] = configuration
    if notifications:
        spec["notifications"] = notifications

    return spec


def build_pipeline_cluster(
    label: str = "default",
    num_workers: Optional[int] = None,
    min_workers: Optional[int] = None,
    max_workers: Optional[int] = None,
    node_type: str = "i3.xlarge",
    spark_conf: Optional[Dict[str, str]] = None,
    custom_tags: Optional[Dict[str, str]] = None,
) -> dict:
    """
    Build a DLT pipeline cluster config.

    Parameters
    ----------
    label : str
        'default' or 'maintenance'.
    num_workers : int | None
        Fixed worker count.  Set either num_workers or min/max_workers.
    min_workers : int | None
    max_workers : int | None
    node_type : str
    spark_conf : dict | None
    custom_tags : dict | None

    Returns
    -------
    dict

    Raises
    ------
    ValueError
        If neither fixed nor autoscale workers are specified.
    """
    if num_workers is None and (min_workers is None or max_workers is None):
        raise ValueError(
            "Specify either num_workers (fixed) or both min_workers and max_workers (autoscale)"
        )
    cluster: dict = {"label": label, "node_type_id": node_type}
    if num_workers is not None:
        cluster["num_workers"] = num_workers
    else:
        cluster["autoscale"] = {
            "min_workers": min_workers,
            "max_workers": max_workers,
            "mode": "ENHANCED",
        }
    if spark_conf:
        cluster["spark_conf"] = spark_conf
    if custom_tags:
        cluster["custom_tags"] = custom_tags
    return cluster


def build_notification_config(
    email_addresses: List[str],
    alerts: Optional[List[str]] = None,
) -> dict:
    """
    Build a DLT pipeline notification config.

    Parameters
    ----------
    email_addresses : list[str]
    alerts : list[str] | None
        Alert types to trigger notification.  Defaults to all failure types.
        Valid values: 'on-update-success', 'on-update-failure',
        'on-update-fatal-failure', 'on-flow-failure'.

    Returns
    -------
    dict

    Raises
    ------
    ValueError
        If email_addresses is empty or an alert type is invalid.
    """
    if not email_addresses:
        raise ValueError("email_addresses must not be empty")
    effective_alerts = alerts or ["on-update-failure", "on-update-fatal-failure"]
    for a in effective_alerts:
        if a not in VALID_ALERT_TYPES:
            raise ValueError(f"Invalid alert type '{a}'. Valid: {VALID_ALERT_TYPES}")
    return {"email_recipients": email_addresses, "alerts": effective_alerts}


def build_notebook_library(path: str) -> dict:
    """Return a notebook library reference for use in pipeline libraries list."""
    return {"notebook": {"path": path}}


def build_file_library(path: str) -> dict:
    """Return a file library reference (Python file) for use in pipeline libraries list."""
    return {"file": {"path": path}}
