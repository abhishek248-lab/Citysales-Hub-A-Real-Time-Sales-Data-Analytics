import dlt

product_rules = {
    "category": "category IS NOT NULL",
    "product_name": "product_name IS NOT NULL"
}

# Schema Evolution: mergeSchema handles new columns from source
dlt.create_streaming_table(
    name="products",
    expect_all_or_drop=product_rules,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.columnMapping.mode": "name",
        "delta.deletedFileRetentionDuration": "interval 120 days",  # Keep deleted files for 4 months
        "delta.logRetentionDuration": "interval 120 days"  # Keep transaction log for 4 months
    }
)

@dlt.append_flow(target="products")
def newproducts():
    # mergeSchema allows reading new columns from source
    df = spark.readStream.option("mergeSchema", "true").table("workspace.store.products_info")
    return df
