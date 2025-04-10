# jobs/bronze/ingest.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from utils.io import write_df_as_table_or_path
from utils.logger import log_function_call
from utils.schema_normalizer import auto_normalize_columns


@log_function_call
def ingest_data(
    spark: SparkSession,
    dataset: str,
    input_paths: dict,
    bronze_output: str,
    column_mapping: dict,
    format: str = "delta",
    normalize: bool = True,
) -> DataFrame:
    """
    Ingests multiple files of a dataset into the Bronze layer and normalizes schema.

    Args:
        spark (SparkSession): Active Spark session.
        dataset (str): Dataset name, e.g., 'nyctaxi'.
        input_paths (dict): Dict like {"yellow": path1, "green": path2}.
        bronze_output (str): Output table name.
        column_mapping (dict): Dict of source: {col_map}.
        format (str): Output format (default "delta").
        normalize (bool): Whether to normalize column names.

    Returns:
        DataFrame: Unified normalized DataFrame.
    """
    frames = []
    for label, path in input_paths.items():
        print(f"📥 Reading {label} data from: {path}")
        df = (
            spark.read.format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .load(path)
            .withColumn("source", F.lit(label))
        )

        if normalize:
            df = auto_normalize_columns(df, column_mapping=column_mapping[label])

        frames.append(df)

    unified_df = frames[0]
    for frame in frames[1:]:
        unified_df = unified_df.unionByName(frame, allowMissingColumns=True)

    print(f"💾 Writing unified dataset to: {bronze_output}")
    spark.sql(f"DROP TABLE IF EXISTS {bronze_output}")
    write_df_as_table_or_path(spark, unified_df, bronze_output, format=format)
    print(f"✅ Ingestion complete for: {bronze_output}")

    return unified_df