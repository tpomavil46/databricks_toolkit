# bootstrap/query_templates.py


def generate_select_all_query(
    file_path: str, file_format: str, limit: int, options: dict = None
) -> str:
    """
    Generates a Spark SQL query for reading a file in a given format with a row limit.

    Args:
        file_path (str): Path to the data file.
        file_format (str): Format of the data (e.g., parquet, csv, delta, json).
        limit (int): Max number of rows to return.
        options (dict): Format-specific options like {"header": "true"}

    Returns:
        str: A Spark SQL query string.
    """
    options_clause = ""
    if options:
        options_clause = (
            " OPTIONS (" + ", ".join([f"{k} '{v}'" for k, v in options.items()]) + ")"
        )

    return f"SELECT * FROM {file_format}.`{file_path}`{options_clause} LIMIT {limit}"
