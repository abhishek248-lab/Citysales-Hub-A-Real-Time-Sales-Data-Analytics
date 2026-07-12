import dlt
from pyspark.sql.functions import expr,col,upper

@dlt.view(
  name="product_view"
)
def sales_view():
    df=spark.readStream.table("products")
    change_upper = df.withColumn("category", upper(col("category")))
    return change_upper


   




