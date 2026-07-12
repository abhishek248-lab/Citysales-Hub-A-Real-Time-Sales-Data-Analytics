import dlt

# Define expectations:
rules = {
    "customer_name": "customer_name IS NOT NULL",
    "amount": "amount > 0"
}

# Schema Evolution: mergeSchema handles new columns from source
dlt.create_streaming_table(
    name="append_sales",
    expect_all_or_drop=rules,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.columnMapping.mode": "name",
        "delta.deletedFileRetentionDuration": "interval 120 days",  # Keep deleted files for 4 months
        "delta.logRetentionDuration": "interval 120 days"  # Keep transaction log for 4 months
    }
)

@dlt.append_flow(target="append_sales")
def bbsr():
    # mergeSchema allows reading new columns from Bhubaneswar source
    df = spark.readStream.option("mergeSchema", "true").table("workspace.store.bhubaneswar_order")
    return df

@dlt.append_flow(target="append_sales")
def khordha():
    # mergeSchema allows reading new columns from Khordha source
    df = spark.readStream.option("mergeSchema", "true").table("workspace.store.khordha_order")
    return df
