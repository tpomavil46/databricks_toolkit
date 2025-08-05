# import os
# import shutil
# from utils.io import read_table, write_df_as_table
# from utils.config import get_output_table, get_input_table
# from utils.session import get_spark
# import pyspark.sql.functions as F


# def run(spark, **kwargs):
#     df = read_table(spark, "main.default.orders_cleaned")
#     spark = get_spark()

#     input_table = kwargs.get("input_table") or get_input_table("orders_cleaned")
#     output_table = kwargs.get("output_table") or get_output_table("orders_transformed_dev")

#     # Cleanup for local runs
#     if os.getenv("ENV", "local") or not os.getenv("ENV"):
#         table_path = f"spark-warehouse/{output_table}"
#         if os.path.exists(table_path):
#             print(f"💣 Clearing existing table path: {table_path}")
#             shutil.rmtree(table_path)

#     print(f"🪄 Reading from: {input_table}")
#     df = read_table(spark, input_table)

#     print("🔧 Transforming orders...")
#     df_transformed = df.withColumnRenamed("o_orderkey", "order_id") \
#                    .withColumnRenamed("o_custkey", "customer_id") \
#                    .withColumnRenamed("o_totalprice", "order_total")
#     df_transformed.show()

#     print(f"💾 Writing to table: {output_table}")
#     write_df_as_table(df_transformed, output_table)
#     print(f"✅ Transformation complete for table: {output_table}")


# silver/transform_orders.py


def run(df, **kwargs):
    # Here you would transform the customer data, ensuring you use the correct column names.
    # Assuming we're working with 'id' and 'name' columns:

    df_transformed = df.withColumnRenamed("id", "customer_id").withColumnRenamed(
        "name", "customer_name"
    )

    # You can now work with 'customer_id' and 'customer_name' in your transformation logic
    df_transformed.show()

    return df_transformed
