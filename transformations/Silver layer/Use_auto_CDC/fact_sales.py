import dlt

# Schema Evolution: mergeSchema handles new columns from sales_view
@dlt.table(
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 120 days",  # Keep deleted files for 4 months
        "delta.logRetentionDuration": "interval 120 days"  # Keep transaction log for 4 months
    }
)
def fact_sales():
    # mergeSchema propagates new columns from upstream view
    return spark.readStream.option("mergeSchema", "true").table("sales_view")

# Fact tables are immutable transactions - each sale is appended once, no updates needed
