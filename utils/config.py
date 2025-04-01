# utils/config.py

import os


def get_input_path(dataset_name: str) -> str:
    """Return the correct path for a dataset depending on environment."""
    env = os.getenv("ENV", "local")
    if env == "databricks":
        return f"/databricks-datasets/retail-org/{dataset_name}"
    return f"data/{dataset_name}.csv"
