# transformations/transform.py

import argparse
import os
import logging
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from utils.logger import log_function_call
from utils.structs import flatten_struct

# Initialize logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


@log_function_call
def run(spark: SparkSession, *args, **kwargs):
    """
    Silver-level transformation for the dataset.

    Args:
        spark (SparkSession): The active Spark session.
        kwargs (dict): Contains 'silver_output' and 'bronze_input' table names.

    Returns:
        DataFrame: Transformed DataFrame written to the silver layer.
    """
    silver_output = kwargs.get("silver_output")
    bronze_input = kwargs.get("bronze_input")

    if not silver_output or not bronze_input:
        logger.error("Missing required arguments: silver_output or bronze_input")
        return

    logger.info(f"📄 Reading table from bronze layer: {bronze_input}")
    df_bronze = spark.table(bronze_input)

    logger.info("🔧 Flattening struct columns...")
    df_flattened = flatten_struct(df_bronze)

    required = [
        "passenger_count",
        "tolls_amount",
        "fare_amount",
        "vendor",
        "trip_distance",
    ]
    missing = [col for col in required if col not in df_flattened.columns]
    if missing:
        raise ValueError(f"❌ Missing expected columns in bronze data: {missing}")

    logger.info("✨ Applying Silver transformations...")
    df_transformed = (
        df_flattened.withColumn(
            "passenger_type",
            F.when(df_flattened.passenger_count > 1, "multi").otherwise("single"),
        )
        .withColumn(
            "has_tolls", F.when(df_flattened.tolls_amount > 0, True).otherwise(False)
        )
        .withColumn("amount_rounded", F.ceil(df_flattened.fare_amount))
    ).select(
        "vendor",
        "passenger_type",
        "has_tolls",
        F.col("trip_distance").alias("distance"),
        F.col("amount_rounded").alias("amount"),
    )

    logger.info(f"💾 Writing transformed data to: {silver_output}")
    spark.sql(f"DROP TABLE IF EXISTS {silver_output}")
    df_transformed.write.format("delta").mode("overwrite").saveAsTable(silver_output)

    logger.info(f"✅ Silver transformation completed successfully: {silver_output}")
    return df_transformed


@log_function_call
def main():
    parser = argparse.ArgumentParser(description="Run Silver transformations.")
    parser.add_argument(
        "--bronze_input", type=str, required=True, help="Bronze input table name"
    )
    parser.add_argument(
        "--silver_output", type=str, required=True, help="Silver output table name"
    )
    args = parser.parse_args()

    spark = (
        DatabricksSession.builder.profile(os.getenv("DATABRICKS_PROFILE"))
        .clusterId(os.getenv("DATABRICKS_CLUSTER_ID"))
        .getOrCreate()
    )

    run(spark, bronze_input=args.bronze_input, silver_output=args.silver_output)


if __name__ == "__main__":
    main()
