# bronze/ingest_customers.py

import os
import shutil
from utils.io import load_csv_to_df, write_df_as_table
from utils.config import get_output_table, get_input_path
from utils.session import get_spark


def run(**kwargs):
    spark = get_spark()

    # Prefer CLI args over config
    input_path = kwargs.get("input_path") or get_input_path("customers")
    output_table = kwargs.get("output_table") or get_output_table("customers_cleaned")

    # Cleanup for local runs
    if os.getenv("ENV", "local") or not os.getenv("ENV"):
        table_path = f"spark-warehouse/{output_table}"
        if os.path.exists(table_path):
            print(f"💣 Clearing existing table path: {table_path}")
            shutil.rmtree(table_path)

    print(f"📥 Reading CSV from: {input_path}")
    df = load_csv_to_df(spark, input_path)
    df.show()

    print(f"💾 Writing to table: {output_table}")
    write_df_as_table(df, output_table)
    print(f"✅ Ingestion complete for table: {output_table}")