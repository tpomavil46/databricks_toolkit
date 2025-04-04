"""
KPI Pipeline

Runs:
1. ingest_customer
2. transform_orders
3. generate_kpis
"""


def run(spark, **kwargs):
    from databricks_toolkit.bronze.ingest_customer import run as ingest
    from databricks_toolkit.silver.transform_orders import run as transform
    from databricks_toolkit.gold.generate_kpis import run as generate

    print("📥 Running ingest_customer...")
    ingest(spark, **kwargs)

    print("🧪 Running transform_orders...")
    transform(spark, **kwargs)

    print("📊 Running generate_kpis...")
    generate(spark, **kwargs)
