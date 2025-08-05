from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    DoubleType,
)
from utils.logger import log_function_call


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
