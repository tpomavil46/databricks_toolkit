# gold/aggregations.py

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from utils.logger import log_function_call


@log_function_call
def create_kpis(df: DataFrame) -> DataFrame:
    """
    Generates global KPIs from the transformed silver-level data.

    Calculates:
        - Total revenue
        - Average trip distance
        - Total number of trips

    Args:
        df (DataFrame): Transformed silver DataFrame.

    Returns:
        DataFrame: A single-row DataFrame with KPIs.
    """
    return df.agg(
        F.sum("amount").alias("total_revenue"),
        F.avg("distance").alias("avg_trip_distance"),
        F.count("*").alias("total_trips")
    )
