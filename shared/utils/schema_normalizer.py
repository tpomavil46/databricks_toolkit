# utils/schema_normalizer.py

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from shared.utils.logger import log_function_call

# A simplified rules engine: known column name variants
COLUMN_STANDARDIZATION_RULES = {
    "tpep_pickup_datetime": "pickup_datetime",
    "lpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
    "store_and_fwd_flag": "store_and_forward",
    "RatecodeID": "rate_code",
    "VendorID": "vendor",
    "PULocationID": "pickup_location",
    "DOLocationID": "dropoff_location",
    "improvement_surcharge": "surcharge",
    "ehail_fee": "ehail_fee",
    "trip_type": "trip_type",
}

# utils/schema_normalizer.py

from pyspark.sql import DataFrame
from shared.utils.logger import log_function_call


@log_function_call
def auto_normalize_columns(df: DataFrame, column_mapping: dict) -> DataFrame:
    """
    Renames columns in a DataFrame based on a provided mapping.

    Args:
        df (DataFrame): Input DataFrame with raw column names.
        column_mapping (dict): Mapping from raw -> normalized column names.

    Returns:
        DataFrame: DataFrame with renamed columns.
    """
    for original, renamed in column_mapping.items():
        if original in df.columns:
            df = df.withColumnRenamed(original, renamed)
    return df
