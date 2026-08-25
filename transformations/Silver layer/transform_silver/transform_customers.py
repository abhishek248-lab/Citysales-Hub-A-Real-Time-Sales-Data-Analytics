from pyspark import pipelines as dp
from pyspark.sql.functions import col, upper, trim, coalesce, to_date

@dp.temporary_view(
     name="customers_transform_silver"
)
def customer_transform_silver():
    df=(spark.readStream
             .option("readChangeFeed", "true")
             .table("workspace.stream_data.customers_bronze"))
    
    df=df.dropDuplicates()
    df=df.withColumn("customer_name",upper(trim(col("customer_name"))))
    df=df.withColumn("Date_of_birth",
                          coalesce(
                                to_date(col("Date_of_birth"),"yyyy-MM-dd"),
                                to_date(col("Date_of_birth"), "yyyy/MM/dd"),
                                to_date(col("Date_of_birth"), "dd/MM/yyyy")
                          )
                    )
    return df
