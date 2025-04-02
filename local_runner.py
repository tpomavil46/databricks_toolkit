import sys
import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", help="Run a single job by name")
    parser.add_argument("--pipeline", help="Run a full pipeline (comma-separated jobs)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("LocalRunner")
        .enableHiveSupport()
        .getOrCreate()
    )

    def run_job(job_name):
        if job_name == "ingest_customer":
            from jobs.ingest_customer import run
        elif job_name == "transform_orders":
            from jobs.transform_orders import run
        else:
            print(f"Unknown job: {job_name}")
            sys.exit(1)

        print(f"🚀 Running job: {job_name}")
        run(spark)

    if args.job:
        run_job(args.job)

    elif args.pipeline:
        job_list = [j.strip() for j in args.pipeline.split(",")]
        for job in job_list:
            run_job(job)
    else:
        print("No job or pipeline specified. Use --job or --pipeline.")
        sys.exit(1)

if __name__ == "__main__":
    main()