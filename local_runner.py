import argparse
import os
from pyspark.sql import SparkSession
from importlib import import_module


def parse_args():
    parser = argparse.ArgumentParser(description="Run Databricks Toolkit Job")
    parser.add_argument(
        "--job", type=str, help="Job name to run (e.g. ingest_customer)"
    )
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", help="Job module to run", required=False)
    parser.add_argument("--pipeline", help="Pipeline module to run", required=False)
    parser.add_argument("--input_path", help="Override input path", required=False)
    parser.add_argument("--output_table", help="Override output table", required=False)

    args = parser.parse_args()
    spark = SparkSession.builder.appName("DatabricksToolkit").getOrCreate()

    kwargs = {
        k: v
        for k, v in vars(args).items()
        if k not in ["job", "pipeline"] and v is not None
    }

    # 🌍 Environment awareness
    current_env = os.getenv("ENV", "local")
    print(f"🌍 Running in environment: {current_env}")

    if args.job:
        module = import_module(f"jobs.{args.job}")
        print(f"▶️ Running job: {args.job}")
        module.run(spark, **kwargs)
    elif args.pipeline:
        module = import_module(f"pipelines.{args.pipeline}")
        print(f"▶️ Running pipeline: {args.pipeline}")
        module.run(spark, **kwargs)
    else:
        raise ValueError("You must specify either --job or --pipeline")


if __name__ == "__main__":
    main()
