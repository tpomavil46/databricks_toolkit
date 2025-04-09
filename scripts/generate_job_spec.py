import os
import json
import argparse
from typing import Any, Dict, cast
from utils.logger import log_function_call

BASE_JOB_SPEC = {
    "name": None,
    "tasks": [
        {
            "task_key": None,
            "existing_cluster_id": os.environ.get(
                "DATABRICKS_CLUSTER_ID", "0401-190124-nho4m8l8"
            ),
            "spark_python_task": {"python_file": None},
        }
    ],
    "timeout_seconds": 3600,
    "max_retries": 1,
}


@log_function_call
def generate_spec(job_name: str, email: str):
    # Safely cast to a dictionary to satisfy Mypy or type checkers
    spec = cast(Dict[str, Any], BASE_JOB_SPEC.copy())

    # Fill in values dynamically
    spec["name"] = job_name.replace("_", " ").title()
    spec["tasks"][0]["task_key"] = job_name
    spec["tasks"][0]["spark_python_task"][
        "python_file"
    ] = f"/Workspace/Repos/{email}/databricks_toolkit/jobs/{job_name}.py"

    # Write to file
    out_path = f"jobs/{job_name}_job.json"
    with open(out_path, "w") as f:
        json.dump(spec, f, indent=2)

    print(f"📦 Generated {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--job", required=True, help="Name of the job, e.g. ingest_customer"
    )
    parser.add_argument(
        "--email",
        required=False,
        default="timpomaville663@gmail.com",
        help="Your Databricks email for the Repo path",
    )

    args = parser.parse_args()
    generate_spec(args.job, args.email)
