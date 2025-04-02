from jobs.ingest_customer import run as run_ingest
from jobs.transform_orders import run as run_transform


def run(spark, **kwargs):
    print("📥 Running ingest_customer...")
    run_ingest(spark, **kwargs)

    print("🧪 Running transform_orders...")
    run_transform(spark, **kwargs)