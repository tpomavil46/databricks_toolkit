# utils/schema_utils.py

from pyspark.sql.types import StructType
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def infer_schema(
    spark, path: str, format="csv", sample_rows: int = 1000, **options
) -> StructType:
    """
    Infers schema from a sample of the data.
    """
    df = spark.read.format(format).options(**options).load(path).limit(sample_rows)
    return df.schema


def flatten_struct(df: DataFrame, prefix="") -> DataFrame:
    """
    Flattens all nested struct columns in a DataFrame.
    """
    flat_cols = []
    for field in df.schema.fields:
        col_name = field.name
        dtype = field.dataType
        full_name = f"{prefix}{col_name}"

        if isinstance(dtype, StructType):
            # Recursive flattening
            nested_df = df.select(F.col(f"{full_name}.*"))
            nested_flat = flatten_struct(nested_df, prefix=f"{full_name}_")
            df = df.drop(col_name).join(nested_flat)
        else:
            flat_cols.append(F.col(full_name).alias(full_name))

    return df.select(flat_cols)
