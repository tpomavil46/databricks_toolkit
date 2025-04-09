# utils/structs.py

import pyspark.sql.functions as F
import pyspark.sql.types as T  # Import the `pyspark.sql.types` module as T
from pyspark.sql import DataFrame  # noqa: F821
from utils.logger import log_function_call


@log_function_call
def flatten_struct(df: DataFrame) -> DataFrame:
    """
    Flattens any struct columns in a DataFrame. This function will go through all the columns in the DataFrame,
    and if a column is a struct, it will flatten it by creating new columns for each field within the struct.

    Args:
    df (DataFrame): The input DataFrame.

    Returns:
    DataFrame: The flattened DataFrame with struct fields as top-level columns.
    """
    flat_columns = []
    for column in df.columns:
        # Check if the column is a struct
        if isinstance(
            df.schema[column].dataType, T.StructType
        ):  # Use `T.StructType` here
            for field in df.schema[column].dataType.fields:
                # Create new column for each field in the struct
                flat_columns.append(
                    F.col(f"{column}.{field.name}").alias(f"{column}_{field.name}")
                )
        else:
            # Non-struct column, just add it as-is
            flat_columns.append(F.col(column))

    # Select the flattened columns
    return df.select(*flat_columns)
