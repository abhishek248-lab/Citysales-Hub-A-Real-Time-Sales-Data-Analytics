from pyspark import pipelines as dp

rules = {
    "quantity" : "quantity > 0",
    "amount" : "amount > 0",
    "order_date" : "order_date is not null"
}

dp.create_streaming_table(
     name = "orders_bronze",
     expect_all_or_drop = rules,
     table_properties = {
         "delta.enableChangeDataFeed" : "true",
         "delta.columnMapping.mode" : "name",
         "delta.deletedFileRetentionDuration" : "interval 120 days",
         "delta.logRetentionDuration" : "interval 120 days"
     }
)

@dp.append_flow(target = "orders_bronze")
def bhubaneswar_orders():
     return (spark.readStream
                  .format("cloudfiles")
                  .option("cloudFiles.format","csv")
                  .option("cloudFiles.schemaLocation","/Volumes/workspace/store/sales_schema/bhubaneswar_order_schema")  
                  .option("cloudFiles.schemaEvolutionMode","addNewColumns") 
                  .option("header","true")
                  .option("inferSchema","true")
                  .load("/Volumes/workspace/store/bhubaneswar_order_raw")            
            )

@dp.append_flow(target ="orders_bronze")
def khordha_orders():
    return (
        spark.readStream
             .format("cloudfiles")
             .option("cloudFiles.format","csv")
             .option("cloudFiles.schemaLocation","/Volumes/workspace/store/sales_schema/khordha_order_schema")
             .option("cloudFiles.schemaEvolutionMode","addNewColumns")
             .option("header","true")
             .option("inferSchema","true")
             .load("/Volumes/workspace/store/khordha_order_raw")
    )

