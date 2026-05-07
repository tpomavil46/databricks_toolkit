"""
orchestration/dabs_yaml.py — Databricks Asset Bundles (DABs) YAML Builders.

Module   : orchestration.dabs_yaml
Concept  : Generate DABs bundle.yml and resource YAML programmatically
When     : CI/CD pipelines that deploy Databricks resources via `databricks bundle deploy`.

DABs project structure
----------------------
    my_bundle/
      databricks.yml           # root bundle config (targets, workspace, variables)
      resources/
        jobs/
          ingest_job.yml       # one file per job resource
        pipelines/
          silver_pipeline.yml  # one file per DLT pipeline
      src/                     # notebooks / Python files
      tests/

databricks.yml (bundle root)
-----------------------------
bundle:
  name: my_bundle

targets:
  dev:
    mode: development
    workspace:
      host: https://adb-xxx.azuredatabricks.net
  prod:
    mode: production
    workspace:
      host: https://adb-yyy.azuredatabricks.net

include:
  - resources/**/*.yml

Key DABs concepts
-----------------
- Variables: ${var.my_var} — defined in databricks.yml, overridden per target.
- Resources: jobs, pipelines, clusters, experiments, model_serving_endpoints.
- Permissions: set at resource level; DABs syncs them on deploy.
- mode: development — prefixes all resource names with [dev <user>] to avoid
  collisions with production resources in the same workspace.

Public API
----------
build_bundle_root_yaml(bundle_name, targets, include_patterns) -> str
build_job_resource_yaml(job_name, job_spec, permissions) -> str
build_pipeline_resource_yaml(pipeline_name, pipeline_config, permissions) -> str
build_variable_block(variables) -> str
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional


def build_bundle_root_yaml(
    bundle_name: str,
    targets: Dict[str, Dict[str, Any]],
    include_patterns: Optional[List[str]] = None,
    variables: Optional[Dict[str, Dict[str, str]]] = None,
) -> str:
    """
    Generate a databricks.yml root bundle configuration.

    Parameters
    ----------
    bundle_name : str
    targets : dict
        Mapping of target name → target config dict.
        Each target dict may have: mode, workspace.host, default (bool).
        Example: {"dev": {"mode": "development", "workspace": {"host": "https://..."}}}
    include_patterns : list[str] | None
        Glob patterns for resource YAML files.
        Default: ["resources/**/*.yml"]
    variables : dict | None
        Variable name → {"default": value, "description": text} mapping.

    Returns
    -------
    str
        YAML string for databricks.yml.
    """
    includes = include_patterns or ["resources/**/*.yml"]
    include_lines = "\n".join(f"  - {p}" for p in includes)

    target_blocks = []
    for name, cfg in targets.items():
        lines = [f"  {name}:"]
        if "mode" in cfg:
            lines.append(f"    mode: {cfg['mode']}")
        if cfg.get("default"):
            lines.append("    default: true")
        if "workspace" in cfg:
            ws = cfg["workspace"]
            lines.append("    workspace:")
            if "host" in ws:
                lines.append(f"      host: {ws['host']}")
            if "root_path" in ws:
                lines.append(f"      root_path: {ws['root_path']}")
        target_blocks.append("\n".join(lines))

    targets_yaml = "\n".join(target_blocks)

    var_block = ""
    if variables:
        var_lines = ["variables:"]
        for var_name, meta in variables.items():
            var_lines.append(f"  {var_name}:")
            if "default" in meta:
                var_lines.append(f"    default: {meta['default']}")
            if "description" in meta:
                var_lines.append(f"    description: {meta['description']}")
        var_block = "\n" + "\n".join(var_lines)

    return f"""bundle:
  name: {bundle_name}

include:
{include_lines}

targets:
{targets_yaml}{var_block}
"""


def build_job_resource_yaml(
    job_name: str,
    job_spec: dict,
    permissions: Optional[List[Dict[str, str]]] = None,
) -> str:
    """
    Generate a resources/jobs/<name>.yml file for a DABs job resource.

    Parameters
    ----------
    job_name : str
        Resource key (Python-identifier style, e.g., 'ingest_bronze_job').
    job_spec : dict
        From orchestration.job_spec.build_job_spec().
    permissions : list[dict] | None
        List of permission entries.
        Example: [{"level": "CAN_MANAGE_RUN", "group_name": "data-engineers"}]

    Returns
    -------
    str
        YAML string for the job resource file.
    """
    spec_json = json.dumps(job_spec, indent=2)
    spec_lines = "\n".join(f"    {line}" for line in spec_json.splitlines())

    perm_block = ""
    if permissions:
        perm_lines = ["    permissions:"]
        for p in permissions:
            perm_lines.append("      - level: " + p.get("level", "CAN_VIEW"))
            if "group_name" in p:
                perm_lines.append("        group_name: " + p["group_name"])
            elif "user_name" in p:
                perm_lines.append("        user_name: " + p["user_name"])
            elif "service_principal_name" in p:
                perm_lines.append("        service_principal_name: " + p["service_principal_name"])
        perm_block = "\n" + "\n".join(perm_lines)

    return f"""resources:
  jobs:
    {job_name}:
{spec_lines}{perm_block}
"""


def build_pipeline_resource_yaml(
    pipeline_name: str,
    pipeline_config: dict,
    permissions: Optional[List[Dict[str, str]]] = None,
) -> str:
    """
    Generate a resources/pipelines/<name>.yml file for a DABs DLT pipeline resource.

    Parameters
    ----------
    pipeline_name : str
        Resource key.
    pipeline_config : dict
        From dlt.pipeline_config.build_pipeline_settings().
    permissions : list[dict] | None

    Returns
    -------
    str
    """
    cfg_json = json.dumps(pipeline_config, indent=2)
    cfg_lines = "\n".join(f"    {line}" for line in cfg_json.splitlines())

    perm_block = ""
    if permissions:
        perm_lines = ["    permissions:"]
        for p in permissions:
            perm_lines.append("      - level: " + p.get("level", "CAN_VIEW"))
            if "group_name" in p:
                perm_lines.append("        group_name: " + p["group_name"])
        perm_block = "\n" + "\n".join(perm_lines)

    return f"""resources:
  pipelines:
    {pipeline_name}:
{cfg_lines}{perm_block}
"""


def build_variable_ref(var_name: str) -> str:
    """
    Return a DABs variable reference string.

    Parameters
    ----------
    var_name : str

    Returns
    -------
    str
        "${var.<name>}" for embedding in YAML values.
    """
    return "${" + f"var.{var_name}" + "}"


def build_lookup_ref(resource_type: str, resource_name: str) -> str:
    """
    Return a DABs resource lookup reference.

    Used to reference another resource's ID without hardcoding it.
    Example: reference a cluster policy ID: ${resources.clusters.my_cluster.id}

    Parameters
    ----------
    resource_type : str
        DABs resource type: 'jobs', 'pipelines', 'clusters', etc.
    resource_name : str

    Returns
    -------
    str
    """
    return "${" + f"resources.{resource_type}.{resource_name}.id" + "}"
