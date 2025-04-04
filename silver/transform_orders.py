import os
import shutil
from utils.io import read_table, write_df_as_table
from utils.config import get_output_table, get_input_table
from utils.session import get_spark
import pyspark.sql.functions as F


def run(**kwargs):
    spark = get_spark()

    input_table = kwargs.get("input_table") or get_input_table("orders_cleaned")
    output_table = kwargs.get("output_table") or get_output_table("orders_transformed")

    # Cleanup for local runs
    if os.getenv("ENV", "local") or not os.getenv("ENV"):
        table_path = f"spark-warehouse/{output_table}"
        if os.path.exists(table_path):
            print(f"💣 Clearing existing table path: {table_path}")
            shutil.rmtree(table_path)

    print(f"🪄 Reading from: {input_table}")
    df = read_table(spark, input_table)

    print("🔧 Transforming orders...")
    df_transformed = df.withColumn("order_total", F.col("quantity") * F.col("unit_price"))
    df_transformed.show()

    print(f"💾 Writing to table: {output_table}")
    write_df_as_table(df_transformed, output_table)
    print(f"✅ Transformation complete for table: {output_table}")
