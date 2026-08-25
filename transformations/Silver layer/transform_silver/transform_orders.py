from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, to_date

@dp.temporary_view(
    name = "orders_transform_silver"
)
def order_transform_silver():
    df=(spark.readStream
             .option("readChangeFeed", "true")
             .table("workspace.stream_data.orders_bronze"))
    
    df=df.dropDuplicates()
    df=df.filter(col("customer_id").isNotNull())
    df=df.filter(col("product_id").isNotNull())
    df=df.withColumn("total_amount", col("amount") * col("quantity"))
    df=df.withColumn("Order_date",coalesce(
                                      to_date(col("order_date"),"yyyy-MM-dd"),
                                      to_date(col("order_date"),"yyyy/MM/dd"),
                                      to_date(col("order_date"),"dd/MM/yyyy")
                                      )
                     )
    return df
