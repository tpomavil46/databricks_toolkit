# bootstrap/table_loader.py

import os
from pyspark.sql import SparkSession
from jinja2 import Template

from utils.logger import log_function_call


@log_function_call
def copy_into_table(
    spark: SparkSession,
    table_name: str,
    file_path: str,
    file_format: str = "csv",
    sql_template_path: str = "sql/bootstrap/copy_into_generic.sql",
    options: dict = None,
):
    """
    Executes a parameterized COPY INTO statement using a templated SQL file.

    Args:
        spark (SparkSession): Spark session object.
        table_name (str): Full name of the table (catalog.schema.table).
        file_path (str): Path to the input files (e.g., CSV, Parquet).
        file_format (str): Input file format (default: "csv").
        sql_template_path (str): Path to templated SQL file.
        options (dict): Format-specific options like {"header": "true", "delimiter": ","}.
    """
    if options is None:
        options = {}

    if not os.path.exists(sql_template_path):
        raise FileNotFoundError(f"SQL template not found: {sql_template_path}")

    with open(sql_template_path, "r") as f:
        template_sql = f.read()

    sql = Template(template_sql).render(
        table_name=table_name,
        file_path=file_path,
        file_format=file_format,
        options=",\n  ".join([f"'{k}' = '{v}'" for k, v in options.items()])
    )

    print(f"🧾 Executing COPY INTO for table: {table_name}")
    spark.sql(sql)
    print("✅ COPY INTO completed.")