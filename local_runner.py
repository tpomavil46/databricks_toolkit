import importlib
import argparse
import os
from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser(description="Run Databricks Toolkit Job")
    parser.add_argument("--job", type=str, help="Job name to run (e.g. ingest_customer)")
    parser.add_argument("--pipeline", type=str, help="Pipeline name to run (optional)")
    known_args, unknown_args = parser.parse_known_args()

    # Parse --key=value arguments into kwargs dict
    extra_kwargs = {}
    for arg in unknown_args:
        if arg.startswith("--") and "=" in arg:
            key, value = arg[2:].split("=", 1)
            extra_kwargs[key] = value

    return known_args, extra_kwargs


def main():
    os.environ["ENV"] = "local"  # Set default environment flag
    known_args, kwargs = parse_args()

    spark = SparkSession.builder.appName("LocalJobRunner").getOrCreate()

    if known_args.job:
        print(f"▶️ Running job: {known_args.job}")
        module = importlib.import_module(f"jobs.{known_args.job}")
        module.run(spark, **kwargs)

    elif known_args.pipeline:
        print(f"▶️ Running pipeline: {known_args.pipeline}")
        module = importlib.import_module(f"pipelines.{known_args.pipeline}")
        module.run(spark, **kwargs)

    else:
        raise ValueError("Please specify --job or --pipeline")


if __name__ == "__main__":
    main()