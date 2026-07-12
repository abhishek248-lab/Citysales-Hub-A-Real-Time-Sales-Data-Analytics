from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

# Schema Evolution: Propagates new columns from all sources
@dp.table(
    name="enriched_sales",
    cluster_by=["customer_id", "product_id"],
    table_properties={
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 120 days",  # Keep deleted files for 4 months
        "delta.logRetentionDuration": "interval 120 days"  # Keep transaction log for 4 months
    }
)
def sales_detailed():
    """Enriched sales data with customer and product dimensions"""
    # mergeSchema handles new columns from fact_sales
    df_fact = spark.readStream.option("mergeSchema", "true").table("fact_sales")
    
    # Batch reads for dimensions (full table each time)
    df_dimCust = spark.read.table("dim_customers")
    df_dimProd = spark.read.table("dim_products")
    
    df_join = (
        df_fact
        .join(df_dimCust, df_fact.customer_id == df_dimCust.customer_id, "left")
        .join(df_dimProd, df_fact.product_id == df_dimProd.product_id, "left")
        .select(
            df_fact.customer_id,
            df_dimCust.customer_name,
            df_dimCust.region,
            df_fact.product_id,
            df_dimProd.product_name,
            df_dimProd.category,
            df_fact.amount,
            df_fact.sale_date,
            current_timestamp().alias("processed_at")
        )
    )
    
    return df_join
