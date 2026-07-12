import dlt

# Define expectation
customer_rules = {
    "customer_name": "customer_name IS NOT NULL",
    "region": "region IS NOT NULL"
}

# Schema Evolution: mergeSchema handles new columns from source
dlt.create_streaming_table(
    name="customers",
    expect_all_or_drop=customer_rules,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.columnMapping.mode": "name",
        "delta.deletedFileRetentionDuration": "interval 120 days",  # Keep deleted files for 4 months
        "delta.logRetentionDuration": "interval 120 days"  # Keep transaction log for 4 months
    }
)

@dlt.append_flow(target="customers")
def newCustomers():
    # mergeSchema allows reading new columns from source
    df = spark.readStream.option("mergeSchema", "true").table("workspace.store.customers_info")
    return df
