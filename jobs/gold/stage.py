# import pyspark.sql.functions as F
from utils.logger import log_function_call  # Import the decorator for logging
from pyspark.sql import SparkSession
from .aggregations import create_kpis  # Import the aggregation function
from utils.io import write_df_as_table_or_path


@log_function_call
def create_gold_stage(
    spark: SparkSession,
    source_table: str,
    dest_name: str,
    view_or_table: str = "view",
    vendor_filter: int = None,
):
    """
    Creates a dematerialized view or a table in the gold layer from the silver layer.
    Optionally filters by vendor and writes KPIs to the catalog.
    """
    query = f"SELECT * FROM {source_table} "
    if vendor_filter:
        query += f"WHERE vendor = {vendor_filter}"

    if view_or_table.lower() == "view":
        query = f"CREATE OR REPLACE VIEW {dest_name} AS {query}"
    elif view_or_table.lower() == "table":
        query = f"CREATE OR REPLACE TABLE {dest_name} AS {query}"
    else:
        raise ValueError("Invalid option for view_or_table. Use 'view' or 'table'.")

    spark.sql(query)
    print(f"✅ Gold {view_or_table} created: {dest_name}")

    # KPI generation
    silver_df = spark.table(source_table)
    kpi_df = create_kpis(silver_df)
    write_df_as_table_or_path(spark, kpi_df, f"{dest_name}_kpis", format="delta")

    print(f"✅ Gold pipeline completed successfully for {dest_name}")
    return dest_name
