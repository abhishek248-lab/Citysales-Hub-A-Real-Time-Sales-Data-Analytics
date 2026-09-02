from pyspark import pipelines as dp

product_rules = {
    "category" : "category is not null",
    "product_name" : "product_name is not null",
    "price" : "price > 0",
    "expiry_date" : "expiry_date is not null"
}

dp.create_streaming_table(
    name = "product_bronze",
    expect_all_or_drop = product_rules,
    table_properties = {
        "delta.enableChangeDataFeed": "true",
        "delta.columnMapping.mode" : "name",
        "delta.deletedFileRetentionDuration" : "interval 120 days",
        "delta.logRetentionDuration" : "interval 120 days"
    }
)

@dp.append_flow(target = "product_bronze")
def product_bronze():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format" ,"csv")
             .option("cloudFiles.schemaLocation","/Volumes/workspace/store/sales_schema/products_schema")
             .option("cloudFiles.schemaEvolutionMode","addNewColumns")
             .option("header","true")
             .option("inferSchema","true")
             .load("/Volumes/workspace/store/product_info_raw")
    )