"""
Generate KPIs Job

This job reads the `orders_transformed` table and computes some basic KPIs.
Eventually, you can replace this with real aggregations, windowing, etc.
"""

from utils.config import get_output_table
from pyspark.sql import functions as F


def run(spark, **kwargs):
    print("📊 Starting KPI generation...")

    # Read transformed orders table
    df_orders = spark.table(get_output_table("orders_transformed"))

    # Simulate KPI logic
    kpi_df = df_orders.agg(
        F.count("*").alias("total_orders"),
        F.countDistinct("description").alias("unique_descriptions")
    )

    # Show results
    print("✅ KPI Results:")
    kpi_df.show()

    # Optionally write to another table (disabled by default)
    # kpi_df.write.mode("overwrite").saveAsTable("demo.order_kpis")