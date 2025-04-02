from utils.io import load_csv_to_df, write_df_as_table  # noqa: F401
from utils.config import get_input_path
import os
import shutil


def run(spark, **kwargs):
    # Dynamically resolve input path
    input_path = get_input_path("orders")

    # Optional: clean local warehouse table if it already exists
    if os.getenv("ENV", "local") or not os.getenv("ENV"):
        table_path = "spark-warehouse/orders_transformed"
        if os.path.exists(table_path):
            print(f"💣 Clearing existing table path: {table_path}")
            shutil.rmtree(table_path)

    # For now, just simulate reading something (you can change this later)
    df = load_csv_to_df(spark, input_path)
    df.show()

    # Save to local or managed table
    df.write.mode("overwrite").saveAsTable("orders_transformed")
