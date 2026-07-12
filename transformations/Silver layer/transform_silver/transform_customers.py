import dlt
from pyspark.sql.functions import expr,col,upper

@dlt.view(
  name="customers_view"
)
#tranformation
def customers_view():
    df=spark.readStream.table("customers")
    upper_df=df.withColumn("region",upper(col("region")))
    return upper_df
   



