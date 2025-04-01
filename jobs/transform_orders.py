# jobs/transform_orders.py
def run(spark, **kwargs):
    print("🚀 Running transform_orders job")
    # dummy DataFrame for now
    df = spark.createDataFrame(
        [(1, "order_001"), (2, "order_002")], ["order_id", "description"]
    )
    df.show()
