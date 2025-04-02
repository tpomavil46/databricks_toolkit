"""
Transform Orders Job

Supports:
- --input_path override (e.g., local CSV or DBFS path)
- --output_table override (e.g., demo.orders_transformed)
"""

import os
import shutil
from utils.io import load_csv_to_df, write_df_as_table  # noqa: F401
from utils.config import get_input_path, get_output_table


def run(spark, **kwargs):
    # Prefer CLI overrides, fallback to config
    input_path = kwargs.get("input_path") or get_input_path("orders")
    output_table = kwargs.get("output_table") or get_output_table("orders_transformed")

    # Cleanup local warehouse table if running locally
    if os.getenv("ENV", "local") or not os.getenv("ENV"):
        table_path = f"spark-warehouse/{output_table}"
        if os.path.exists(table_path):
            print(f"💣 Clearing existing table path: {table_path}")
            shutil.rmtree(table_path)

    # Example: read + transform
    df = load_csv_to_df(spark, input_path)
    df_transformed = df  # 🧪 Replace this with real transformations later
    df_transformed.show()

    # Save to managed table
    df_transformed.write.mode("overwrite").saveAsTable(output_table)