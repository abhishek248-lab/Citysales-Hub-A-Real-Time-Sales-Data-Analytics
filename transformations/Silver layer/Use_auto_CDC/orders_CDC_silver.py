from pyspark import pipelines as dp

dp.create_streaming_table(
    name = "orders_CDC_silver",
    table_properties = {
        "delta.deletedFileRetentionDuration" : "120 days",
        "delta.logRetentionDuration" : "120 days"
    }
)

dp.create_auto_cdc_flow(
    target = "orders_CDC_silver",
    source = "orders_transform_silver",
    keys = ["order_id"],
    sequence_by = "_commit_timestamp",
    except_column_list=[
        "_change_type",
        "_commit_version",
        "_commit_timestamp"
    ],
    stored_as_scd_type = 1
) 