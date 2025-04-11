# bootstrap/dbfs_reader.py
from pyspark.sql import DataFrame
from utils.logger import log_function_call
from utils.session import MyDatabricksSession


@log_function_call
def preview_dbfs_file(
    file_path: str,
    file_format: str = "parquet",
    limit: int = 10,
    options: dict = None,
) -> DataFrame:
    """
    Reads and previews a file from DBFS using DataFrame API.

    Args:
        file_path (str): Path to file.
        file_format (str): Format like csv, parquet, delta.
        limit (int): Number of rows to preview.
        options (dict): Spark read options.

    Returns:
        DataFrame: Preview DataFrame.
    """
    spark = MyDatabricksSession.get_spark()
    reader = spark.read.format(file_format)

    if options:
        for k, v in options.items():
            reader = reader.option(k, v)
    elif file_format.lower() == "csv":
        # Default CSV options
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(file_path)

    print(f"📊 Previewing: {file_path}")
    df.show(limit)
    return df.limit(limit)