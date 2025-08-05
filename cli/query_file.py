import os
import sys

# Add project root to sys.path so utils can be imported
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
from pyspark.sql import DataFrame
from utils.logger import log_function_call
from utils.session import MyDatabricksSession


@log_function_call
def query_file(path: str, fmt: str, limit: int) -> DataFrame:
    """
    Query a file stored in DBFS using Spark SQL via a Databricks Connect session.

    Args:
        path (str): DBFS path to the file or folder.
        fmt (str): Format of the file (e.g., 'parquet', 'csv', 'json').
        limit (int): Maximum number of rows to return.

    Returns:
        DataFrame: Result of the SQL query.
    """
    spark = MyDatabricksSession.get_spark()

    fmt = fmt.lower()
    supported = {"parquet", "csv", "json", "delta", "orc"}
    if fmt not in supported:
        raise ValueError(f"Unsupported format '{fmt}'. Supported formats: {supported}")

    query = f"SELECT * FROM {fmt}.`{path}` LIMIT {limit}"
    print(f"\n📄 Executing query:\n{query}\n")
    return spark.sql(query)


@log_function_call
def main():
    parser = argparse.ArgumentParser(description="Query files in DBFS with SQL.")
    parser.add_argument(
        "--path", required=True, type=str, help="DBFS path to the file or directory."
    )
    parser.add_argument(
        "--format",
        required=True,
        type=str,
        help="File format (e.g., parquet, csv, json, delta).",
    )
    parser.add_argument(
        "--limit",
        required=True,
        type=int,
        help="Number of rows to return (LIMIT clause).",
    )

    args = parser.parse_args()
    df = query_file(path=args.path, fmt=args.format, limit=args.limit)
    df.show(truncate=False)


if __name__ == "__main__":
    main()
