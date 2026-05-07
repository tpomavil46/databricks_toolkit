"""Tests for orchestration.job_spec and orchestration.dabs_yaml — no Spark required."""

from __future__ import annotations

import pytest

from orchestration.job_spec import (
    build_autoscale_cluster,
    build_fixed_cluster,
    build_notebook_task,
    build_python_task,
    build_dlt_task,
    build_sql_task,
    build_task,
    build_schedule,
    build_email_notifications,
    build_job_cluster,
    build_job_spec,
)
from orchestration.dabs_yaml import (
    build_bundle_root_yaml,
    build_job_resource_yaml,
    build_pipeline_resource_yaml,
    build_variable_ref,
    build_lookup_ref,
)


# ---------------------------------------------------------------------------
# Cluster specs
# ---------------------------------------------------------------------------

class TestBuildAutoscaleCluster:
    def test_autoscale_key_present(self):
        spec = build_autoscale_cluster(2, 8)
        assert "autoscale" in spec
        assert spec["autoscale"]["min_workers"] == 2
        assert spec["autoscale"]["max_workers"] == 8

    def test_spark_version_present(self):
        spec = build_autoscale_cluster()
        assert "spark_version" in spec

    def test_photon_enabled_by_default(self):
        spec = build_autoscale_cluster()
        assert spec.get("runtime_engine") == "PHOTON"

    def test_photon_disabled(self):
        spec = build_autoscale_cluster(photon=False)
        assert "runtime_engine" not in spec

    def test_spark_conf_forwarded(self):
        spec = build_autoscale_cluster(spark_conf={"spark.foo": "bar"})
        assert spec["spark_conf"]["spark.foo"] == "bar"

    def test_node_type(self):
        spec = build_autoscale_cluster(node_type="r5.4xlarge")
        assert spec["node_type_id"] == "r5.4xlarge"


class TestBuildFixedCluster:
    def test_num_workers(self):
        spec = build_fixed_cluster(num_workers=4)
        assert spec["num_workers"] == 4

    def test_no_autoscale_key(self):
        spec = build_fixed_cluster()
        assert "autoscale" not in spec

    def test_photon_disabled(self):
        spec = build_fixed_cluster(photon=False)
        assert "runtime_engine" not in spec


# ---------------------------------------------------------------------------
# Task configs
# ---------------------------------------------------------------------------

class TestBuildNotebookTask:
    def test_notebook_path(self):
        cfg = build_notebook_task("/Repos/me/notebook")
        assert cfg["notebook_task"]["notebook_path"] == "/Repos/me/notebook"

    def test_base_params(self):
        cfg = build_notebook_task("/nb", base_params={"env": "prod"})
        assert cfg["notebook_task"]["base_parameters"]["env"] == "prod"

    def test_no_params_no_key(self):
        cfg = build_notebook_task("/nb")
        assert "base_parameters" not in cfg["notebook_task"]

    def test_source_default(self):
        cfg = build_notebook_task("/nb")
        assert cfg["notebook_task"]["source"] == "WORKSPACE"


class TestBuildPythonTask:
    def test_python_file(self):
        cfg = build_python_task("/src/pipeline.py")
        assert cfg["spark_python_task"]["python_file"] == "/src/pipeline.py"

    def test_parameters(self):
        cfg = build_python_task("/src/p.py", parameters=["--env", "prod"])
        assert cfg["spark_python_task"]["parameters"] == ["--env", "prod"]

    def test_no_params_no_key(self):
        cfg = build_python_task("/src/p.py")
        assert "parameters" not in cfg["spark_python_task"]


class TestBuildDltTask:
    def test_pipeline_id(self):
        cfg = build_dlt_task("abc-123")
        assert cfg["pipeline_task"]["pipeline_id"] == "abc-123"


class TestBuildSqlTask:
    def test_query_id(self):
        cfg = build_sql_task(query_id="q123", warehouse_id="wh1")
        assert cfg["sql_task"]["query"]["query_id"] == "q123"

    def test_neither_raises(self):
        with pytest.raises(ValueError):
            build_sql_task(warehouse_id="wh1")

    def test_warehouse_id_in_task(self):
        cfg = build_sql_task(query_id="q1", warehouse_id="wh99")
        assert cfg["sql_task"]["warehouse_id"] == "wh99"


# ---------------------------------------------------------------------------
# build_task
# ---------------------------------------------------------------------------

class TestBuildTask:
    def _notebook_cfg(self):
        return build_notebook_task("/nb")

    def test_task_key(self):
        t = build_task("my_task", self._notebook_cfg())
        assert t["task_key"] == "my_task"

    def test_depends_on(self):
        t = build_task("t2", self._notebook_cfg(), depends_on=["t1"])
        assert t["depends_on"] == [{"task_key": "t1"}]

    def test_no_depends_when_not_set(self):
        t = build_task("t1", self._notebook_cfg())
        assert "depends_on" not in t

    def test_cluster_key(self):
        t = build_task("t1", self._notebook_cfg(), cluster_key="shared")
        assert t["job_cluster_key"] == "shared"

    def test_max_retries(self):
        t = build_task("t1", self._notebook_cfg(), max_retries=3)
        assert t["max_retries"] == 3

    def test_run_if(self):
        t = build_task("t1", self._notebook_cfg(), run_if="ALL_DONE")
        assert t["run_if"] == "ALL_DONE"

    def test_notebook_task_key_present(self):
        t = build_task("t1", self._notebook_cfg())
        assert "notebook_task" in t


# ---------------------------------------------------------------------------
# build_schedule
# ---------------------------------------------------------------------------

class TestBuildSchedule:
    def test_cron_expression(self):
        s = build_schedule("0 0 6 * * ?")
        assert s["quartz_cron_expression"] == "0 0 6 * * ?"

    def test_timezone(self):
        s = build_schedule("0 0 * * * ?", timezone="America/New_York")
        assert s["timezone_id"] == "America/New_York"

    def test_pause_status(self):
        s = build_schedule("0 0 * * * ?", pause_status="PAUSED")
        assert s["pause_status"] == "PAUSED"


# ---------------------------------------------------------------------------
# build_email_notifications
# ---------------------------------------------------------------------------

class TestBuildEmailNotifications:
    def test_on_failure(self):
        n = build_email_notifications(on_failure=["ops@example.com"])
        assert n["on_failure"] == ["ops@example.com"]

    def test_no_alert_for_skipped(self):
        n = build_email_notifications()
        assert n["no_alert_for_skipped_runs"] is True

    def test_no_on_failure_when_not_set(self):
        n = build_email_notifications()
        assert "on_failure" not in n


# ---------------------------------------------------------------------------
# build_job_spec
# ---------------------------------------------------------------------------

class TestBuildJobSpec:
    def _task(self):
        return build_task("t1", build_notebook_task("/nb"))

    def test_name(self):
        spec = build_job_spec("my_job", [self._task()])
        assert spec["name"] == "my_job"

    def test_tasks_present(self):
        spec = build_job_spec("j", [self._task()])
        assert len(spec["tasks"]) == 1

    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="name"):
            build_job_spec("", [self._task()])

    def test_empty_tasks_raises(self):
        with pytest.raises(ValueError, match="tasks"):
            build_job_spec("j", [])

    def test_max_concurrent_runs(self):
        spec = build_job_spec("j", [self._task()], max_concurrent_runs=3)
        assert spec["max_concurrent_runs"] == 3

    def test_schedule_included(self):
        s = build_schedule("0 0 6 * * ?")
        spec = build_job_spec("j", [self._task()], schedule=s)
        assert "schedule" in spec

    def test_job_clusters_included(self):
        jc = build_job_cluster("cluster1", build_autoscale_cluster())
        spec = build_job_spec("j", [self._task()], job_clusters=[jc])
        assert "job_clusters" in spec

    def test_tags_included(self):
        spec = build_job_spec("j", [self._task()], tags={"team": "data"})
        assert spec["tags"]["team"] == "data"


# ---------------------------------------------------------------------------
# DABs YAML
# ---------------------------------------------------------------------------

class TestBuildBundleRootYaml:
    def test_bundle_name_in_yaml(self):
        yaml = build_bundle_root_yaml("my_bundle", {"dev": {"mode": "development"}})
        assert "my_bundle" in yaml

    def test_target_name_in_yaml(self):
        yaml = build_bundle_root_yaml("b", {"dev": {"mode": "development"}})
        assert "dev:" in yaml

    def test_mode_in_yaml(self):
        yaml = build_bundle_root_yaml("b", {"prod": {"mode": "production"}})
        assert "production" in yaml

    def test_include_pattern_in_yaml(self):
        yaml = build_bundle_root_yaml("b", {}, include_patterns=["resources/**/*.yml"])
        assert "resources/**/*.yml" in yaml

    def test_variables_block(self):
        yaml = build_bundle_root_yaml(
            "b", {},
            variables={"catalog": {"default": "dev_catalog", "description": "target catalog"}}
        )
        assert "catalog" in yaml
        assert "dev_catalog" in yaml

    def test_workspace_host(self):
        targets = {"prod": {"mode": "production", "workspace": {"host": "https://adb-123.net"}}}
        yaml = build_bundle_root_yaml("b", targets)
        assert "adb-123.net" in yaml


class TestBuildJobResourceYaml:
    def _spec(self):
        return build_job_spec("my_job", [build_task("t", build_notebook_task("/nb"))])

    def test_resources_jobs_key(self):
        yaml = build_job_resource_yaml("my_job", self._spec())
        assert "resources:" in yaml
        assert "jobs:" in yaml

    def test_job_name_key(self):
        yaml = build_job_resource_yaml("ingest_job", self._spec())
        assert "ingest_job:" in yaml

    def test_permissions_in_yaml(self):
        yaml = build_job_resource_yaml(
            "j", self._spec(),
            permissions=[{"level": "CAN_MANAGE_RUN", "group_name": "data-team"}]
        )
        assert "permissions:" in yaml
        assert "data-team" in yaml


class TestDabsHelpers:
    def test_variable_ref(self):
        ref = build_variable_ref("catalog")
        assert "${var.catalog}" == ref

    def test_lookup_ref(self):
        ref = build_lookup_ref("pipelines", "bronze_pipeline")
        assert "resources.pipelines.bronze_pipeline.id" in ref
