import dlt

# Schema Evolution: New columns from upstream automatically propagate through CDC
dlt.create_streaming_table(
    name="dim_customers",
    table_properties={
        "delta.deletedFileRetentionDuration": "interval 120 days",  # Keep deleted files for 4 months
        "delta.logRetentionDuration": "interval 120 days"  # Keep transaction log for 4 months
    }
)

dlt.create_auto_cdc_flow(
    target="dim_customers",
    source="customers_view",
    keys=["customer_id"],
    sequence_by="last_updated",
    stored_as_scd_type=1
)

# SCD Type 1: Updates existing records in place (no history tracking)
# Schema evolution works automatically - new columns from customers_view will appear here
