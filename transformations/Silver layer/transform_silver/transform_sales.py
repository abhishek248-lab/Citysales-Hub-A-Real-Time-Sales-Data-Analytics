import dlt
from pyspark.sql.functions import expr,col

#perform transformation using  logical view and we do not want to store as table in file
@dlt.view(
  name="sales_view"
)
def sales_view():
    df=spark.readStream.table("append_sales")
    add_column=df.withColumn("Total_amount",col("quantity") * col("amount")) 
    df_filter=add_column.filter(col("quantity")>=2)
    return df_filter




   




