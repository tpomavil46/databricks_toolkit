from utils.io import load_csv_to_df, write_df_as_table  # noqa: F401
from utils.config import get_output_table, get_input_path
import shutil
import os


def run(spark, **kwargs):
    # Prefer CLI args over config
    input_path = kwargs.get("input_path") or get_input_path("customers")
    output_table = kwargs.get("output_table") or get_output_table("customers_cleaned")

    # Cleanup for local runs
    if os.getenv("ENV", "local") or not os.getenv("ENV"):
        table_path = f"spark-warehouse/{output_table}"
        if os.path.exists(table_path):
            print(f"💣 Clearing existing table path: {table_path}")
            shutil.rmtree(table_path)

    df = load_csv_to_df(spark, input_path)
    df.show()

    df.write.mode("overwrite").saveAsTable(output_table)
