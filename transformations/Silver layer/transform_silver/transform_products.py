from pyspark import pipelines as dp
from pyspark.sql.functions import col, initcap, coalesce, to_date

@dp.temporary_view(
    name="products_transform_silver"
)
def products_transform_silver():
    df=(spark.readStream
             .option("readChangeFeed", "true")
             .table("workspace.stream_data.product_bronze"))
    
    df=df.dropDuplicates()
    df = df.withColumn("product_name",initcap(col("product_name")))
    df=df.withColumn(
           "expiry_date", coalesce(
               to_date(col("expiry_date"), "yyyy-MM-dd"),
               to_date(col("expiry_date"), "yyyy/MM/dd"),
               to_date(col("expiry_date"), "dd/MM/yyyy")
           )
    )
    return df