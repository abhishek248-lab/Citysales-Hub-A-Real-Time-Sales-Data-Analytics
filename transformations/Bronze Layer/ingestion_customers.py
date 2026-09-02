from pyspark import pipelines as dp

customer_rules = {
        "email": "email is not null",
        "date_of_birth" : "date_of_birth is not null",
        "street_name" : "street_name is not null"
}

dp.create_streaming_table(
    name = "customers_bronze",
    expect_all_or_drop = customer_rules,
    table_properties = {
        "delta.enableChangeDataFeed" : "true",
        "delta.columnMapping.mode" : "name",
        "delta.deletedFileRetentionDuration" : "interval 30 days",
        "delta.logRetentionDuration" : "interval 30 days"
    }
)

@dp.append_flow(target = "customers_bronze")
def customer_info():
    return (
        spark.readStream
             .format("cloudfiles")
             .option("cloudFiles.format","csv")
             .option("cloudFiles.schemaLocation","/Volumes/workspace/store/sales_schema/customers_schema")
             .option("cloudFiles.schemaEvolutionMode","addNewColumns")
             .option("header","true")
             .option("inferSchema","true")
             .load("/Volumes/workspace/store/customer_info_raw")
    )