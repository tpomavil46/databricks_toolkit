from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
)
from utils.logger import log_function_call
import pandas as pd
import os
from utils.io import write_df_as_table_or_path

@log_function_call
def ingest_data(spark, input_path, bronze_output, format="delta"):
    """
    Ingests combined NYC Taxi datasets into the bronze layer (yellow + green).
    Applies a consistent schema and handles ingestion from DBFS or local.

    Args:
        spark (SparkSession): Spark session, optionally passed in.
        input_path (str): Ignored here — left for compatibility.
        bronze_output (str): Destination Delta table name.
        format (str): Format for output (default: "delta").

    Returns:
        pyspark.sql.DataFrame: Unified DataFrame ingested from both datasets.
    """
    # Standard NYC schema across yellow and green datasets
    nyc_schema = StructType([
        StructField("vendor", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("rate_code", StringType(), True),
        StructField("store_and_forward", StringType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("payment_type", StringType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("surcharge", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
    ])

    # Use fixed, known-good input paths
    yellow_path = "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz"
    green_path = "dbfs:/databricks-datasets/nyctaxi/tripdata/green/green_tripdata_2019-12.csv.gz"

    print(f"📥 Reading yellow data from: {yellow_path}")
    yellow_df = (
        spark.read.format("csv")
        .option("header", True)
        .schema(nyc_schema)
        .load(yellow_path)
    )

    print(f"📥 Reading green data from: {green_path}")
    green_df = (
        spark.read.format("csv")
        .option("header", True)
        .schema(nyc_schema)
        .load(green_path)
    )

    df = yellow_df.unionByName(green_df)

    df.show(5)
    df.printSchema()

    print(f"💾 Writing unified dataset to: {bronze_output}")
    spark.sql(f"DROP TABLE IF EXISTS {bronze_output}")
    write_df_as_table_or_path(spark, df, bronze_output, format=format)
    print(f"✅ Ingestion complete for: {bronze_output}")

    return df