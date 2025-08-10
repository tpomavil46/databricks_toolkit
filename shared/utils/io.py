from pyspark.sql import SparkSession
from shared.utils.logger import log_function_call


@log_function_call
def write_df_as_table_or_path(spark: SparkSession, df, location: str, format="delta"):
    """
    Writes a DataFrame to a specified location (either a table or a file path).

    Parameters:
    ----------
    spark : SparkSession
        The active Spark session.

    df : pyspark.sql.DataFrame
        The DataFrame to write.

    location : str
        The destination path or table name.

    format : str
        The file format to use (default: "delta").
    """
    if location.startswith("dbfs:") or location.startswith("hdfs:"):
        # If location is a path (dbfs or hdfs), write to that path
        df.write.format(format).mode("overwrite").save(location)
        print(f"📂 Data written to: {location} in {format} format")
    else:
        # If location is a table name, write to the Databricks table
        df.write.format(format).mode("overwrite").saveAsTable(location)
        print(f"📋 Data written to table: {location} in {format} format")
