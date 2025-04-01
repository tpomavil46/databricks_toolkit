import sys
import argparse
from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--job", required=True, help="Job name to run (e.g. ingest_customer)"
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("LocalRunner").getOrCreate()

    if args.job == "ingest_customer":
        from jobs.ingest_customer import run
        run(spark)

    elif args.job == "transform_orders":
        from jobs.transform_orders import run

        run(spark)

    else:
        print(f"Unknown job: {args.job}")
        sys.exit(1)


if __name__ == "__main__":
    main()
