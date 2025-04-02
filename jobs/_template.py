"""
Template job for Databricks or local Spark execution.

Usage:
- Copy this file to `jobs/your_new_job.py`
- Customize `input_key` and `output_table`
"""

import os
import shutil
from utils.io import load_csv_to_df, write_df_as_table  # noqa: F401
from utils.config import get_input_path, get_output_table


def run(spark, **kwargs):
    # 🔧 Customize these two values
    input_key = "your_input_key"
    output_table = get_output_table("your_output_table")

    # 🧼 Cleanup local warehouse if running locally
    if os.getenv("ENV", "local") or not os.getenv("ENV"):
        table_path = f"spark-warehouse/{output_table}"
        if os.path.exists(table_path):
            print(f"💣 Clearing existing table path: {table_path}")
            shutil.rmtree(table_path)

    # 🧪 Load, transform, write
    input_path = get_input_path(input_key)
    df = load_csv_to_df(spark, input_path)

    # Optionally inspect the data
    df.show()

    # 💾 Save as managed table
    df.write.mode("overwrite").saveAsTable(output_table)
