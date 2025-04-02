"""
Generate KPIs Job

This job reads from the cleaned customers table and creates a KPI summary.
"""

from pyspark.sql import functions as F


def run(spark, **kwargs):
    # Read transformed customer data
    df = spark.table("customers_cleaned")

    # Example KPI: total number of customers
    kpi_df = df.agg(F.count("*").alias("total_customers"))

    # Show the KPIs
    kpi_df.show()

    # Save to table (overwrite for now)
    df.write.mode("overwrite").saveAsTable("main.default.kpis_summary")
