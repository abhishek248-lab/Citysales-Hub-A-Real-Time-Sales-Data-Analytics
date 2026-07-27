from pyspark import pipelines as dp

product_rules = {
           "category" : "category is not null",
           "product_name" : "product_name is not null"
}

dp.create_streaming_table(
    name = "product",
    expect_all_or_drop = product_rules,
    table_properties = {
              "delta.enableChangeDataFeed" : "true",
              "delta.columnMapping.mode" : "name",
              "delta.deletedFileRetentionDuration" : "interval 120 days",
              "delta.logRetentionDuration" : "interval 120 days"
    }
)
@dp.append_flow(target="product")
def newproducts():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/workspace/store/products_info_raw/_schemas")
        .option("header", "true")
        .load("/Volumes/workspace/store/products_info_raw")
        .drop("_rescued_data"))
