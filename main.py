# main.py

import argparse
import os
from databricks.connect import DatabricksSession
from pipelines.default_pipeline import run  # use your central orchestrator
from dotenv import load_dotenv
from utils.logger import log_function_call

# Load .env for DATABRICKS_PROFILE and DATABRICKS_CLUSTER_ID
load_dotenv()


@log_function_call
def main():
    parser = argparse.ArgumentParser(description="Run the full Medallion pipeline")

    parser.add_argument("--input_table", type=str, required=True, help="Input path or table (CSV or similar)")
    parser.add_argument("--bronze_path", type=str, required=True, help="Target Bronze table name")
    parser.add_argument("--silver_path", type=str, required=True, help="Target Silver table name")
    parser.add_argument("--gold_path", type=str, required=True, help="Target Gold base name")
    parser.add_argument("--format", type=str, default="delta", help="Storage format (default: delta)")
    parser.add_argument("--view_or_table", type=str, default="view", help="Gold output type: 'view' or 'table'")
    parser.add_argument("--vendor_filter", type=int, default=None, help="Optional vendor ID filter for Gold view")

    args = parser.parse_args()

    # Create Spark session via Databricks Connect
    spark = (
        DatabricksSession.builder.profile(os.getenv("DATABRICKS_PROFILE"))
        .clusterId(os.getenv("DATABRICKS_CLUSTER_ID"))
        .getOrCreate()
    )

    run(spark, **vars(args))


if __name__ == "__main__":
    main()