from pyspark import pipelines as dp

from pyspark.sql.functions import (col, sum as _sum, count, avg, max as _max, min as _min,
                                    date_format, year, month, dayofmonth, approx_count_distinct, round as _round, when, lag, coalesce, lit, window, to_timestamp, to_date)
                                    
from pyspark.sql.window import Window


# CUSTOMER BEHAVIOR ANALYSIS
@dp.table(
    name="gold_customer_behavior_stream",
    comment="Real-time customer behavior with 10-min windows using latest SCD Type 2 order records"
)
def customer_behavior():

    orders = (
        spark.readStream
        .option("skipChangeCommits", "true")  # Skip CDC updates/deletes, only process appends
        .table("workspace.stream_data.orders_CDC_silver")
        .withColumn("Order_timestamp", to_timestamp(col("Order_date")))
        .withWatermark("Order_timestamp", "2 minutes")
    )

    customers = spark.read.table(
        "workspace.stream_data.customers_CDC_silver"
    )

    behavior = orders.join(
        customers,
        orders.customer_id == customers.customer_id,
        "inner"
    )

    return (
        behavior
        .groupBy(
            window(col("Order_timestamp"), "5 minutes"),
            customers.customer_id,
            customers.customer_name,
            customers.city,
            customers.state,
            customers.gender
        )
        .agg(
            _sum(col("amount").cast("double")).alias("total_spent"),
            count("*").alias("purchase_count"),
            _round(avg(col("amount").cast("double")), 2).alias("avg_purchase_value"),
            _max(to_date(col("Order_date"))).alias("last_purchase_date")
        )
        .withColumn(
            "customer_segment",
            when(col("total_spent") > 10000, "Premium")
            .when(col("total_spent") > 5000, "Gold")
            .when(col("total_spent") > 2000, "Silver")
            .otherwise("Bronze")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "customer_id",
            "customer_name",
            "city",
            "state",
            "gender",
            "total_spent",
            "purchase_count",
            "avg_purchase_value",
            "last_purchase_date",
            "customer_segment"
        )
    )


# PRODUCT PERFORMANCE METRICS

@dp.table(
    name="gold_product_performance_stream",
    comment="Real-time product performance with 10-min windows using latest SCD Type 2 order records"
)
def product_performance():

    orders = (
        spark.readStream
        .option("skipChangeCommits", "true")  # Skip CDC updates/deletes, only process appends
        .table("workspace.stream_data.orders_CDC_silver")
        .withColumn("Order_timestamp", to_timestamp(col("Order_date")))
        .withWatermark("Order_timestamp", "2 minutes")
    )

    products = (
        spark.read.table("workspace.stream_data.products_CDC_silver")
        .filter(col("__END_AT").isNull())  # Only current product versions
    )

    performance = orders.join(
        products,
        orders.product_id == products.product_id,
        "inner"
    )

    return (
        performance
        .groupBy(
            window(col("Order_timestamp"), "5 minutes"),
            products.product_id,
            products.product_name,
            products.category,
            products.price
        )
        .agg(
            _sum(col("amount").cast("double")).alias("total_revenue"),
            _sum(col("quantity").cast("int")).alias("units_sold"),
            count("*").alias("order_frequency"),
            _round(avg(col("amount").cast("double")), 2).alias("avg_revenue_per_order")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "product_id",
            "product_name",
            "category",
            "price",
            "total_revenue",
            "units_sold",
            "order_frequency",
            "avg_revenue_per_order"
        )
    )



# PAYMENT METHOD ANALYSIS
@dp.materialized_view(
    name="gold_payment_analysis_mv",
    comment="Payment method analysis using latest SCD Type 2 orders"
)
def payment_analysis():

    orders = spark.read.table("workspace.stream_data.orders_CDC_silver")

    return (
        orders
        .groupBy(
            col("payment_method")
        )
        .agg(
            _sum(col("amount").cast("double")).alias("total_revenue"),
            count(col("order_id")).alias("total_orders"),
            _round(avg(col("amount").cast("double")), 2).alias("avg_transaction_value"),
            _max(col("amount").cast("double")).alias("max_transaction"),
            _min(col("amount").cast("double")).alias("min_transaction")
        )
        .select(
            "payment_method",
            "total_revenue",
            "total_orders",
            "avg_transaction_value",
            "max_transaction",
            "min_transaction"
        )
    )


# PRODUCT PRICE vs DEMAND ANALYSIS (SCD Type 2)
@dp.materialized_view(
    name="gold_price_demand_analysis",
    comment="Track how price changes affect demand"
)
def price_demand_analysis():
    
    # Read product price history (all versions from SCD Type 2)
    products_all = spark.read.table("workspace.stream_data.products_CDC_silver")
    w = Window.partitionBy("product_id").orderBy("__START_AT")
    
    df_products = (
        products_all
        .withColumn("prev_price", coalesce(lag("price").over(w), col("price")))
        .withColumn(
            "price_direction",
            when(col("price").cast("double") > col("prev_price").cast("double"), "UP")
            .when(col("price").cast("double") < col("prev_price").cast("double"), "DOWN")
            .otherwise("SAME")
        )
    )
    
    # Read orders (SCD Type 1 - no history tracking)
    df_orders = spark.read.table("workspace.stream_data.orders_CDC_silver")
    
    # Get earliest __START_AT per product for fallback matching
    product_earliest = (
        df_products
        .groupBy("product_id")
        .agg(_min("__START_AT").alias("min_start"))
    )
    
    df_products_enriched = df_products.join(product_earliest, "product_id")
    
    # Temporal join: match each order to the product version active at order time
    # Orders before the earliest version fall back to that earliest version
    df_joined = (
        df_orders.alias("o")
        .join(
            df_products_enriched.alias("p"),
            (col("o.product_id") == col("p.product_id")) &
            (
                # Normal: order falls within version's validity window
                (col("o.Order_date") >= col("p.__START_AT")) &
                ((col("o.Order_date") < col("p.__END_AT")) | col("p.__END_AT").isNull())
            ) |
            (
                # Fallback: order is before earliest version, match to it
                (col("o.Order_date") < col("p.min_start")) &
                (col("p.__START_AT") == col("p.min_start"))
            ),
            "inner"
        )
    )
    
    # Aggregate demand per product per price period
    df_demand = (
        df_joined
        .groupBy(
            col("p.product_id"),
            col("p.category"),
            col("p.price").cast("double").alias("current_price"),
            col("p.prev_price").cast("double").alias("prev_price"),
            col("p.price_direction"),
            col("p.__START_AT")
        )
        .agg(
            _sum(col("o.quantity").cast("int")).alias("current_demand")
        )
    )
    
    # Calculate demand changes across consecutive price periods
    w2 = Window.partitionBy("product_id").orderBy("__START_AT")
    
    df_final = (
        df_demand
        .withColumn("prev_demand", coalesce(lag("current_demand").over(w2), col("current_demand")))
        .withColumn(
            "demand_direction",
            when(col("current_demand") > col("prev_demand"), "UP")
            .when(col("current_demand") < col("prev_demand"), "DOWN")
            .otherwise("SAME")
        )
        .select(
            "category",
            _round(col("prev_price"), 2).alias("prev_price"),
            _round(col("current_price"), 2).alias("current_price"),
            "price_direction",
            "prev_demand",
            "current_demand",
            "demand_direction"
        )
        .orderBy("category")
    )
    
    return df_final
