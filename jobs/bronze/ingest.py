from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.io import write_df_as_table_or_path
from utils.logger import log_function_call


@log_function_call
def ingest_data(
    spark: SparkSession,
    dataset: str,
    input_paths: dict,
    bronze_output: str,
    schema: StructType = None,
    format: str = "delta",
):
    """
    Ingests one or more datasets into the Bronze layer.

    Args:
        spark (SparkSession): Spark session.
        dataset (str): Dataset name, e.g., 'nyctaxi'.
        input_paths (dict): Dict of label: path, e.g., {"yellow": "...", "green": "..."}.
        bronze_output (str): Output table name.
        schema (StructType): Optional schema to apply.
        format (str): Format to write (default: 'delta').

    Returns:
        DataFrame: Unified ingested DataFrame.
    """
    if not schema:
        raise ValueError("Schema must be provided to ingest_data().")

    frames = []
    for label, path in input_paths.items():
        print(f"📥 Reading {label} data from: {path}")
        df = (
            spark.read.format("csv")
            .option("header", True)
            .schema(schema)
            .load(path)
            .withColumn("source", F.lit(label))
        )
        frames.append(df)

    unified_df = frames[0]
    for frame in frames[1:]:
        unified_df = unified_df.unionByName(frame)

    print(f"💾 Writing unified dataset to: {bronze_output}")
    spark.sql(f"DROP TABLE IF EXISTS {bronze_output}")
    write_df_as_table_or_path(spark, unified_df, bronze_output, format=format)
    print(f"✅ Ingestion complete for: {bronze_output}")

    return unified_df
