from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    sum as _sum,
    count,
    avg,
    max as _max,
    min as _min,
    date_format,
    year,
    month,
    dayofmonth,
    countDistinct,
    round as _round,
    when,
    lag,
    coalesce,
    lit
)
from pyspark.sql.window import Window



# 1. REAL-TIME SALES METRICS

@dp.materialized_view(
    name="gold_realtime_sales",
    comment="Sales metrics using latest SCD Type 2 order records"
)
def realtime_sales():

    orders = (
        spark.read.table(
            "workspace.stream_data.orders_CDC_silver"
        )
        .filter(
            col("__END_AT").isNull()
        )
    )

    return (
        orders
        .groupBy(
            date_format(
                col("Order_date"),
                "yyyy-MM-dd"
            ).alias("sale_date")
        )
        .agg(
            _sum(
                col("amount").cast("double")
            ).alias("total_revenue"),

            count(
                col("order_id")
            ).alias("total_orders"),

            countDistinct(
                col("customer_id")
            ).alias("unique_customers"),

            _round(
                avg(col("amount").cast("double")),
                2
            ).alias("avg_order_value"),

            _max(
                col("amount").cast("double")
            ).alias("max_order_value"),

            _min(
                col("amount").cast("double")
            ).alias("min_order_value")
        )
        .orderBy(
            col("sale_date").desc()
        )
    )



# 2. PRODUCT DEMAND ANALYSIS


@dp.materialized_view(
    name="gold_product_demand",
    comment="Product demand using latest SCD Type 2 order records"
)
def product_demand():

    orders = (
        spark.read.table(
            "workspace.stream_data.orders_CDC_silver"
        )
        .filter(
            col("__END_AT").isNull()
        )
    )

    products = (
        spark.read.table(
            "workspace.stream_data.products_CDC_silver"
        )
        .filter(
            col("__END_AT").isNull()
        )
    )

    demand = orders.join(
        products,
        orders.product_id == products.product_id,
        "inner"
    )

    return (
        demand
        .groupBy(
            products.product_id,
            products.product_name,
            products.category,
            products.price.cast("double").alias("price")
        )
        .agg(
            _sum(
                col("quantity").cast("int")
            ).alias("total_quantity_sold"),

            count(
                "*"
            ).alias("order_count")
        )
        .orderBy(
            col("total_quantity_sold").desc()
        )
    )


# ============================================================
# 3. CUSTOMER BEHAVIOR ANALYSIS
# ============================================================

@dp.materialized_view(
    name="gold_customer_behavior",
    comment="Customer behavior using latest SCD Type 2 order records"
)
def customer_behavior():

    orders = (
        spark.read.table(
            "workspace.stream_data.orders_CDC_silver"
        )
        .filter(
            col("__END_AT").isNull()
        )
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
            customers.customer_id,
            customers.customer_name,
            customers.city,
            customers.state,
            customers.gender
        )
        .agg(
            _sum(
                col("amount").cast("double")
            ).alias("total_spent"),

            count(
                "*"
            ).alias("purchase_count"),

            _round(
                avg(col("amount").cast("double")),
                2
            ).alias("avg_purchase_value"),

            _max(
                col("Order_date")
            ).alias("last_purchase_date")
        )
        .withColumn(
            "customer_segment",
            when(
                col("total_spent") > 10000,
                "Premium"
            )
            .when(
                col("total_spent") > 5000,
                "Gold"
            )
            .when(
                col("total_spent") > 2000,
                "Silver"
            )
            .otherwise(
                "Bronze"
            )
        )
        .orderBy(
            col("total_spent").desc()
        )
    )


# 4. PRODUCT PERFORMANCE METRICS

@dp.materialized_view(
    name="gold_product_performance",
    comment="Product performance using latest SCD Type 2 order records"
)
def product_performance():

    orders = (
        spark.read.table(
            "workspace.stream_data.orders_CDC_silver"
        )
        .filter(
            col("__END_AT").isNull()
        )
    )

    products = spark.read.table(
        "workspace.stream_data.products_CDC_silver"
    )

    performance = orders.join(
        products,
        orders.product_id == products.product_id,
        "inner"
    )

    return (
        performance
        .groupBy(
            products.product_id,
            products.product_name,
            products.category,
            products.price.cast("double").alias("price")
        )
        .agg(
            _sum(
                col("amount").cast("double")
            ).alias("total_revenue"),

            _sum(
                col("quantity").cast("int")
            ).alias("units_sold"),

            count(
                "*"
            ).alias("order_frequency"),

            _round(
                avg(col("amount").cast("double")),
                2
            ).alias("avg_revenue_per_order")
        )
        .orderBy(
            col("total_revenue").desc()
        )
    )


# ============================================================
# 5. GEOGRAPHIC MARKET ANALYSIS
# ============================================================

@dp.materialized_view(
    name="gold_geographic_market",
    comment="Geographic market analysis using latest SCD Type 2 orders"
)
def geographic_market():

    orders = (
        spark.read.table(
            "workspace.stream_data.orders_CDC_silver"
        )
        .filter(
            col("__END_AT").isNull()
        )
    )

    customers = spark.read.table(
        "workspace.stream_data.customers_CDC_silver"
    )

    geo_sales = orders.join(
        customers,
        orders.customer_id == customers.customer_id,
        "inner"
    )

    return (
        geo_sales
        .groupBy(
            customers.country,
            customers.state,
            customers.city
        )
        .agg(
            _sum(
                col("amount").cast("double")
            ).alias("total_revenue"),

            count(
                col("order_id")
            ).alias("total_orders"),

            _round(
                avg(col("amount").cast("double")),
                2
            ).alias("avg_order_value")
        )
        .orderBy(
            col("total_revenue").desc()
        )
    )


# ============================================================
# 6. PAYMENT METHOD ANALYSIS
# ============================================================

@dp.materialized_view(
    name="gold_payment_analysis",
    comment="Payment method analysis using latest SCD Type 2 orders"
)
def payment_analysis():

    orders = (
        spark.read.table(
            "workspace.stream_data.orders_CDC_silver"
        )
        .filter(
            col("__END_AT").isNull()
        )
    )

    return (
        orders
        .groupBy(
            col("payment_method")
        )
        .agg(
            _sum(
                col("amount").cast("double")
            ).alias("total_revenue"),

            count(
                col("order_id")
            ).alias("total_orders"),

            _round(
                avg(col("amount").cast("double")),
                2
            ).alias("avg_transaction_value"),

            _max(
                col("amount").cast("double")
            ).alias("max_transaction"),

            _min(
                col("amount").cast("double")
            ).alias("min_transaction")
        )
        .orderBy(
            col("total_revenue").desc()
        )
    )


# PRODUCT PRICE vs PURCHASE DEMAND ANALYSIS (SCD TYPE 2)

@dp.materialized_view(
    name="gold_price_demand"
)
def price_demand():

    # Get all product price history
    products = spark.read.table(
        "workspace.stream_data.products_CDC_silver"
    )

    orders = (
        spark.read.table(
            "workspace.stream_data.orders_CDC_silver"
        )
        .filter(
            col("__END_AT").isNull()
        )
    )

    # Left join to include ALL price periods (even with zero sales)
    sales = products.join(
        orders,
        (
            (orders.product_id == products.product_id)
            & (orders.Order_date >= products.__START_AT.cast("date"))
            & (
                products.__END_AT.isNull()
                | (orders.Order_date < products.__END_AT.cast("date"))
            )
        ),
        "left"
    )

    # Calculate units sold during each historical price period
    sales = sales.groupBy(
        products.product_id,
        products.product_name,
        products.price.cast("double").alias("price"),
        products.__START_AT
    ).agg(
        coalesce(_sum(orders.quantity.cast("int")), lit(0)).alias("units_sold")
    )

    # Compare current and previous price/sales
    w = Window.partitionBy("product_id").orderBy("__START_AT")

    result = (
        sales
        .withColumn("previous_price", lag("price").over(w))
        .withColumn("previous_units", lag("units_sold").over(w))
        .withColumn(
            "price_change_%",
            _round(
                ((col("price") - col("previous_price"))
                 / col("previous_price")) * 100,
                2
            )
        )
        .withColumn(
            "purchase_change_%",
            _round(
                ((col("units_sold") - col("previous_units"))
                 / col("previous_units")) * 100,
                2
            )
        )
    )

    return result.select(
        "product_id",
        "product_name",
        col("previous_price"),
        col("price").alias("current_price"),
        "price_change_%",
        col("previous_units"),
        col("units_sold").alias("current_units"),
        "purchase_change_%"
    )

