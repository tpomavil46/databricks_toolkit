from utils.io import load_csv_to_df, write_df_as_table
from utils.config import get_input_path
import shutil
import os

# Optional: only do this if running locally
if os.getenv("ENV", "local") or not os.getenv("ENV"):
    table_path = "spark-warehouse/customers_cleaned"
    if os.path.exists(table_path):
        shutil.rmtree(table_path)


def run(spark, **kwargs):
    input_path = get_input_path("customers")
    df = load_csv_to_df(spark, input_path)
    df.show()

    df.write.mode("overwrite").saveAsTable("customers_cleaned")
