from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    DoubleType,
)
from shared.utils.logger import log_function_call


@log_function_call
def get_column_mapping(dataset: str) -> dict:
    """
    Returns a mapping from original column names to normalized schema for a dataset.

    Args:
        dataset (str): Dataset identifier, e.g., 'yellow' or 'green'.

    Returns:
        dict: Mapping of column names to standard schema names.
    """
    if dataset == "yellow":
        return {
            "VendorID": "vendor",
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
            "passenger_count": "passenger_count",
            "trip_distance": "trip_distance",
            "RatecodeID": "rate_code",
            "store_and_fwd_flag": "store_and_forward",
            "PULocationID": "pickup_location",
            "DOLocationID": "dropoff_location",
            "payment_type": "payment_type",
            "fare_amount": "fare_amount",
            "extra": "extra",
            "mta_tax": "mta_tax",
            "tip_amount": "tip_amount",
            "tolls_amount": "tolls_amount",
            "improvement_surcharge": "surcharge",
            "total_amount": "total_amount",
            "congestion_surcharge": "congestion_surcharge",
        }
    elif dataset == "green":
        return {
            "VendorID": "vendor",
            "lpep_pickup_datetime": "pickup_datetime",
            "lpep_dropoff_datetime": "dropoff_datetime",
            "passenger_count": "passenger_count",
            "trip_distance": "trip_distance",
            "RatecodeID": "rate_code",
            "store_and_fwd_flag": "store_and_forward",
            "PULocationID": "pickup_location",
            "DOLocationID": "dropoff_location",
            "payment_type": "payment_type",
            "fare_amount": "fare_amount",
            "extra": "extra",
            "mta_tax": "mta_tax",
            "tip_amount": "tip_amount",
            "tolls_amount": "tolls_amount",
            "improvement_surcharge": "surcharge",
            "total_amount": "total_amount",
            "trip_type": "trip_type",
            "ehail_fee": "ehail_fee",
            "congestion_surcharge": "congestion_surcharge",
        }
    else:
        raise ValueError(f"Unknown dataset for column mapping: {dataset}")


@log_function_call
def flatten_struct(df, column_name: str):
    """
    Flattens a struct column in a DataFrame by expanding its nested fields.

    Args:
        df: PySpark DataFrame
        column_name (str): Name of the struct column to flatten

    Returns:
        DataFrame: DataFrame with flattened struct column
    """
    from pyspark.sql.functions import col

    # Get the struct schema
    struct_schema = df.schema[column_name].dataType

    # Create select expressions for all fields in the struct
    select_exprs = []
    for field in struct_schema.fields:
        field_name = field.name
        select_exprs.append(
            col(f"{column_name}.{field_name}").alias(f"{column_name}_{field_name}")
        )

    # Add all other columns
    for col_name in df.columns:
        if col_name != column_name:
            select_exprs.append(col(col_name))

    return df.select(*select_exprs)
