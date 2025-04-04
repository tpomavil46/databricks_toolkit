from utils.io import read_table
from utils.config import get_input_table
from utils.session import get_spark
import pyspark.sql.functions as F


def run(**kwargs):
    spark = get_spark()

    input_table = kwargs.get("input_table") or get_input_table("orders_transformed")
    output_view = kwargs.get("output_view") or "customer_kpis"

    print(f"📊 Reading for KPI generation from: {input_table}")
    df = read_table(spark, input_table)

    print("📈 Generating KPIs...")
    df_kpis = (
        df.groupBy("customer_id")
          .agg(F.sum("order_total").alias("total_spent"))
    )
    df_kpis.createOrReplaceTempView(output_view)

    print(f"✅ Gold view created: {output_view}")
