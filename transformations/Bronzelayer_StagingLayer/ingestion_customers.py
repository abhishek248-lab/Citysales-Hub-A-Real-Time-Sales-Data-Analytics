from pyspark import pipelines as dp

customer_rules = {
    "customer_name" : "customer_name is not null",
    "region" : "region is not null"
}

dp.create_streaming_table(
         name = "customers",
         expect_all_or_drop = customer_rules,
         table_properties = {
             "delta.enableChangeDataFeed" :"true",
             "delta.columnMapping.mode" : "name",
             "delta.deletedFileRetentionDuration":"interval 120 days",
             "delta.logRetentionDuration" : "interval 120 days"
         }
         
)
@dp.append_flow(target = "customers")
def newCustomer():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/workspace/store/customer_info_raw/_schemas")
        .option("header", "true")
        .load("/Volumes/workspace/store/customer_info_raw")
        .drop("_rescued_data"))
