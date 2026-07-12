from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, sum, max, min, count, avg, approx_count_distinct, stddev, percentile_approx, first, last, window, collect_set

@dp.table(
    name="sales_metrics",
    table_properties={
        "delta.autoOptimize.autoCompact": "true",
        "delta.deletedFileRetentionDuration": "interval 120 days",  # Keep deleted files for 4 months
        "delta.logRetentionDuration": "interval 120 days"  # Keep transaction log for 4 months
    }
)
def sales_metrics_aggregated():
    """Business KPIs and metrics aggregated by 1-minute window from enriched_sales"""
    df = (
        # Read from enriched_sales (Silver layer joined data)
        spark.readStream.option("mergeSchema", "true").table("enriched_sales")
        .withWatermark("sale_date", "1 minutes")
        .groupBy(
            window("sale_date", "1 minute")
        )
        .agg(
            # Revenue metrics
            sum("amount").alias("total_sales"),
            avg("amount").alias("avg_transaction_value"),
            stddev("amount").alias("sales_std_dev"),
            percentile_approx("amount", 0.5).alias("median_transaction"),
            max("amount").alias("max_transaction"),
            min("amount").alias("min_transaction"),
            
            # Volume metrics
            count("amount").alias("transaction_count"),
            
            # Customer metrics
            approx_count_distinct("customer_id").alias("unique_customers"),
            collect_set("customer_name").alias("customer_list"),
            
            # Product metrics
            approx_count_distinct("product_id").alias("unique_products"),
            collect_set("product_name").alias("product_list"),
            
            # Category metrics
            approx_count_distinct("category").alias("unique_categories"),
            collect_set("category").alias("category_list"),
            
            # Regional metrics
            collect_set("region").alias("region_list"),
            
            # Time metrics
            first("sale_date").alias("first_sale_time"),
            last("sale_date").alias("last_sale_time")
        )
        .withColumn("processed_time", current_timestamp())
        .withColumn("revenue_per_customer", col("total_sales") / col("unique_customers"))
        .withColumn("revenue_per_product", col("total_sales") / col("unique_products"))
        .withColumn("transaction_range", col("max_transaction") - col("min_transaction"))
        .withColumn("product_penetration_rate", col("unique_products") / col("transaction_count"))
    )
    
    return df

# VACUUM MAINTENANCE:
# Run this command periodically (weekly/monthly) to remove old files:
# VACUUM workspace.stream_data.sales_metrics RETAIN 120 HOURS
