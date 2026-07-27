from pyspark import pipelines as dp


rules = {
    "customer_name" : "customer_name is not Null",
    "amount" : "amount > 0"
}

dp.create_streaming_table(
    name="append_orders",
    expect_all_or_drop=rules,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.columnMapping.mode": "name",
        "delta.deletedFileRetentionDuration": "interval 120 days", 
        "delta.logRetentionDuration": "interval 120 days"  
    }
)

@dp.append_flow(target="append_orders")
def bbsr():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/workspace/store/bhubaneswar_order_raw/_schemas")
        .option("header", "true")
        .load("/Volumes/workspace/store/bhubaneswar_order_raw")
        .drop("_rescued_data"))

@dp.append_flow(target="append_orders")
def khordha():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/workspace/store/khordha_order_raw/_schemas")
        .option("header", "true")
        .load("/Volumes/workspace/store/khordha_order_raw")
        .drop("_rescued_data"))
