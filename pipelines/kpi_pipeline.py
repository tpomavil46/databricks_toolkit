"""
KPI Pipeline

This pipeline simulates a multi-step process:

1. Ingest customer data
2. Transform orders
3. (Placeholder) Generate KPIs

Use this as a template for real-world multi-step workflows.

Supports:
- --input_path override
- --output_table override
"""

from jobs.ingest_customer import run as ingest_customer
from jobs.transform_orders import run as transform_orders


def run(spark, **kwargs):
    print("▶️ Step 1: Ingesting customer data...")
    ingest_customer(spark, **kwargs)

    print("▶️ Step 2: Transforming orders...")
    transform_orders(spark, **kwargs)

    print("▶️ Step 3: (TODO) Generating KPIs...")
    # Simulate a KPI step
    print("📊 Example KPI: Total orders = 42 🚀")