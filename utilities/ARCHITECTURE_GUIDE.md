# 🏗️ Architecture Guide - CitySales Hub

## 📋 Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Medallion Pattern](#medallion-pattern)
3. [Data Flow](#data-flow)
4. [Layer-by-Layer Details](#layer-by-layer-details)
5. [Advanced Patterns](#advanced-patterns)
6. [Performance Optimizations](#performance-optimizations)
7. [Design Decisions](#design-decisions)

---

## 🎯 Architecture Overview

### **Architecture Style:**
**Lambda-Lite Architecture** with real-time streaming path only (no separate batch path).

### **Core Principles:**
```
1. Separation of Concerns    → Bronze/Silver/Gold layers
2. Immutability             → Append-only fact tables
3. Schema Flexibility       → Additive schema evolution
4. Performance by Design    → Liquid clustering, auto-optimization
5. Quality at Ingestion     → Expectations in Bronze layer
```

### **Architecture Diagram:**
```
┌─────────────────────────────────────────────────────────────┐
│                      SOURCE SYSTEMS                         │
│  customers_info  │  products_info  │  bhubaneswar_order    │
│                  │                 │  khordha_order         │
└──────────────────┴─────────────────┴────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    🥉 BRONZE LAYER                          │
│              (Ingestion + Quality Gates)                    │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │  customers   │  │  products    │  │ append_sales │    │
│  │  (streaming) │  │  (streaming) │  │  (streaming) │    │
│  │              │  │              │  │              │    │
│  │ Expectations │  │ Expectations │  │ Expectations │    │
│  │ - NOT NULL   │  │ - NOT NULL   │  │ - NOT NULL   │    │
│  │              │  │              │  │ - amount > 0 │    │
│  └──────────────┘  └──────────────┘  └──────────────┘    │
│                                                             │
│  Features:                                                  │
│  • mergeSchema for schema evolution                        │
│  • CDC enabled (delta.enableChangeDataFeed)               │
│  • Column mapping (supports renames)                       │
│  • 4-month retention policy                                │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│               🥈 SILVER LAYER (1/2)                         │
│            (Transformation Views)                           │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │customers_view│  │ product_view │  │  sales_view  │    │
│  │ (temp view)  │  │ (temp view)  │  │ (temp view)  │    │
│  │              │  │              │  │              │    │
│  │ • UPPERCASE  │  │ • UPPERCASE  │  │ • Filter     │    │
│  │   regions    │  │   categories │  │   qty >= 2   │    │
│  └──────────────┘  └──────────────┘  └──────────────┘    │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│               🥈 SILVER LAYER (2/2)                         │
│         (CDC Tables + Enrichment)                           │
│                                                             │
│  Dimensions (SCD Type 1 CDC):                              │
│  ┌──────────────┐          ┌──────────────┐              │
│  │dim_customers │          │ dim_products │              │
│  │              │          │              │              │
│  │ Keys:        │          │ Keys:        │              │
│  │ customer_id  │          │ product_id   │              │
│  │              │          │              │              │
│  │ Sequence:    │          │ Sequence:    │              │
│  │ last_updated │          │ sale_date    │              │
│  └──────────────┘          └──────────────┘              │
│                                                             │
│  Fact Table (Append-Only):                                │
│  ┌──────────────┐                                         │
│  │  fact_sales  │                                         │
│  │  (streaming) │                                         │
│  │              │                                         │
│  │ • No CDC     │                                         │
│  │ • Immutable  │                                         │
│  └──────────────┘                                         │
│         ↓                                                  │
│  Stream-Batch Join:                                       │
│  ┌──────────────────────────────────────────────────┐   │
│  │           enriched_sales                         │   │
│  │           (streaming table)                      │   │
│  │                                                   │   │
│  │  fact_sales (stream) LEFT JOIN dim_customers     │   │
│  │                      LEFT JOIN dim_products      │   │
│  │                                                   │   │
│  │  Clustered by: [customer_id, product_id]        │   │
│  │  Auto-optimization: Enabled                      │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                  🥇 GOLD LAYER                              │
│               (Business KPIs)                               │
│                                                             │
│  ┌────────────────────────────────────────────────────┐   │
│  │            sales_metrics                           │   │
│  │            (streaming table)                       │   │
│  │                                                     │   │
│  │  Windowed Aggregation:                            │   │
│  │  • Window: 1 minute (tumbling)                    │   │
│  │  • Watermark: 1 minute                            │   │
│  │  • 17+ KPIs per window                            │   │
│  │                                                     │   │
│  │  Revenue │ Volume │ Customer │ Product │ Regional │   │
│  │  Metrics │ Metrics │ Metrics  │ Metrics │ Metrics  │   │
│  └────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## 🥉🥈🥇 Medallion Pattern

### **Why Medallion Architecture?**

| Benefit | Description |
|---------|-------------|
| **Separation** | Each layer has a clear responsibility |
| **Reusability** | Silver tables can serve multiple Gold aggregations |
| **Debugging** | Easy to trace issues through layers |
| **Governance** | Apply different access controls per layer |
| **Performance** | Optimize each layer independently |

### **Layer Responsibilities:**

#### **🥉 Bronze Layer - "Store Everything"**
```
Purpose: Raw data ingestion with minimal processing
Characteristics:
  ✅ 1:1 mapping with source systems
  ✅ Schema evolution enabled
  ✅ Quality gates (drop bad records)
  ✅ Append-only (immutable history)
  ✅ CDC enabled for downstream
  
Anti-patterns:
  ❌ NO transformations
  ❌ NO joins
  ❌ NO aggregations
  ❌ NO enrichment
```

#### **🥈 Silver Layer - "Clean and Enrich"**
```
Purpose: Business logic, transformations, enrichment
Characteristics:
  ✅ Cleaned data (uppercase, trimmed, standardized)
  ✅ CDC tables (slowly changing dimensions)
  ✅ Fact tables (immutable transactions)
  ✅ Joined/enriched data
  ✅ Clustered for performance
  
Anti-patterns:
  ❌ NO aggregations (save for Gold)
  ❌ NO window functions (save for Gold)
```

#### **🥇 Gold Layer - "Business KPIs"**
```
Purpose: Aggregated metrics for analytics/reporting
Characteristics:
  ✅ Windowed aggregations
  ✅ Business-friendly naming
  ✅ Ready for visualization
  ✅ Optimized for read performance
  
Anti-patterns:
  ❌ NO raw data storage
  ❌ NO detailed transactions
```

---

## 🔄 Data Flow Details

### **Flow 1: Customer Dimension**
```
workspace.store.customers_info (source)
  ↓ [streaming read + mergeSchema]
Bronze: customers
  ↓ [streaming read]
Silver: customers_view (UPPERCASE regions, temp view)
  ↓ [streaming read]
Silver: dim_customers (CDC: SCD Type 1)
  ↓ [batch read - snapshot join]
Silver: enriched_sales (joined with fact_sales)
  ↓ [streaming read]
Gold: sales_metrics (aggregated)
```

**Key Pattern:** Streaming ingestion → Temp view transformation → CDC table → Batch join → Aggregation

---

### **Flow 2: Product Dimension**
```
workspace.store.products_info (source)
  ↓ [streaming read + mergeSchema]
Bronze: products
  ↓ [streaming read]
Silver: product_view (UPPERCASE categories, temp view)
  ↓ [streaming read]
Silver: dim_products (CDC: SCD Type 1)
  ↓ [batch read - snapshot join]
Silver: enriched_sales (joined with fact_sales)
  ↓ [streaming read]
Gold: sales_metrics (aggregated)
```

**Key Pattern:** Same as customers, maintains consistency

---

### **Flow 3: Sales Fact**
```
workspace.store.bhubaneswar_order (source 1)  ┐
                                              ├→ Append Flow
workspace.store.khordha_order (source 2)     ┘
  ↓ [streaming read + mergeSchema]
Bronze: append_sales (combined via append flows)
  ↓ [streaming read]
Silver: sales_view (filter quantity >= 2, temp view)
  ↓ [streaming read]
Silver: fact_sales (append-only, no CDC)
  ↓ [streaming read]
Silver: enriched_sales (stream-batch join with dimensions)
  ↓ [streaming read + watermark]
Gold: sales_metrics (1-min windowed aggregation)
```

**Key Pattern:** Multi-source append → Filter → Append-only fact → Join → Aggregate

---

## 📊 Layer-by-Layer Details

### **🥉 Bronze Layer Implementation**

#### **ingestion_customers.py**
```python
import dlt

customer_rules = {
    "customer_name": "customer_name IS NOT NULL",
    "region": "region IS NOT NULL"
}

dlt.create_streaming_table(
    name="customers",
    expect_all_or_drop=customer_rules,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.columnMapping.mode": "name",
        "delta.deletedFileRetentionDuration": "interval 120 days",
        "delta.logRetentionDuration": "interval 120 days"
    }
)

@dlt.append_flow(target="customers")
def newCustomers():
    df = spark.readStream.option("mergeSchema", "true").table("workspace.store.customers_info")
    return df
```

**Design Decisions:**
* ✅ `create_streaming_table` + `append_flow` pattern
* ✅ `expect_all_or_drop` for data quality
* ✅ `mergeSchema` for schema evolution
* ✅ CDC enabled for downstream tracking
* ✅ Column mapping for rename support

---

#### **ingestion_sales.py (Multi-Source)**
```python
rules = {
    "customer_name": "customer_name IS NOT NULL",
    "amount": "amount > 0"
}

dlt.create_streaming_table(
    name="append_sales",
    expect_all_or_drop=rules,
    table_properties={...}
)

@dlt.append_flow(target="append_sales")
def bbsr():
    return spark.readStream.option("mergeSchema", "true").table("workspace.store.bhubaneswar_order")

@dlt.append_flow(target="append_sales")
def khordha():
    return spark.readStream.option("mergeSchema", "true").table("workspace.store.khordha_order")
```

**Why Append Flows (not UNION)?**
```
Append Flows:
✅ Handles different schemas gracefully
✅ Each source can fail independently
✅ Easy to add new sources (just add another flow)
✅ Better error handling

UNION:
❌ Requires identical schemas
❌ One source failure breaks entire UNION
❌ Complex to maintain with many sources
```

---

### **🥈 Silver Layer Implementation**

#### **Transformation Views**

**customers_view.py**
```python
from pyspark import pipelines as dp
from pyspark.sql.functions import upper

@dp.temporary_view()
def customers_view():
    return spark.readStream.table("customers") \
        .withColumn("region", upper("region"))
```

**Why Temporary Views?**
```
✅ Lightweight (no storage)
✅ Computed on-demand
✅ Pipeline-private (not published to catalog)
✅ Perfect for preprocessing before CDC
```

---

#### **CDC Tables (Dimensions)**

**dim_customers.py**
```python
import dlt

dlt.create_streaming_table(
    name="dim_customers",
    table_properties={
        "delta.deletedFileRetentionDuration": "interval 120 days",
        "delta.logRetentionDuration": "interval 120 days"
    }
)

dlt.create_auto_cdc_flow(
    target="dim_customers",
    source="customers_view",
    keys=["customer_id"],
    sequence_by="last_updated",
    stored_as_scd_type=1
)
```

**CDC Configuration Explained:**
```
target: "dim_customers"          → Target table to upsert into
source: "customers_view"         → Source stream (temp view)
keys: ["customer_id"]            → Primary key for matching
sequence_by: "last_updated"      → Column for ordering (newest wins)
stored_as_scd_type: 1            → SCD Type 1 (update in place)
```

**Why SCD Type 1 for Dimensions?**
```
Business Requirement: "Show current customer information"

SCD Type 1 (chosen):
✅ One record per customer (current state)
✅ Updates overwrite previous values
✅ Simple to join (no validity date logic)
✅ Lower storage cost

SCD Type 2 (not chosen):
❌ Multiple records per customer (history)
❌ Requires __START_AT, __END_AT columns
❌ Complex joins (must filter for current)
❌ Higher storage cost
```

---

#### **Fact Table (Append-Only)**

**fact_sales.py**
```python
import dlt

@dlt.table(
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 120 days",
        "delta.logRetentionDuration": "interval 120 days"
    }
)
def fact_sales():
    return spark.readStream.option("mergeSchema", "true").table("sales_view")
```

**Why NO CDC for Facts?**
```
Fact tables are IMMUTABLE transactions:
✅ Each sale happens once
✅ No updates needed (amount doesn't change)
✅ Append-only pattern is sufficient
❌ CDC adds complexity with zero benefit
```

---

#### **Enriched Sales (Stream-Batch Join)**

**sales_join.py**
```python
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

@dp.table(
    name="enriched_sales",
    cluster_by=["customer_id", "product_id"],
    table_properties={
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 120 days",
        "delta.logRetentionDuration": "interval 120 days"
    }
)
def sales_detailed():
    df_fact = spark.readStream.option("mergeSchema", "true").table("fact_sales")
    df_dimCust = spark.read.table("dim_customers")  # Batch read
    df_dimProd = spark.read.table("dim_products")  # Batch read
    
    df_join = (
        df_fact
        .join(df_dimCust, df_fact.customer_id == df_dimCust.customer_id, "left")
        .join(df_dimProd, df_fact.product_id == df_dimProd.product_id, "left")
        .select(...)
    )
    
    return df_join
```

**Stream-Batch Join Pattern:**
```
Fact Table (streaming):
  • Continuous flow of transactions
  • Stream read: spark.readStream

Dimension Tables (batch):
  • Current snapshot of dimensions
  • Batch read: spark.read
  • Joins get latest dimension values

Why This Works:
✅ Dimensions change slowly (SCD Type 1)
✅ Batch read gets current snapshot each micro-batch
✅ Stream-batch join is efficient pattern
✅ No watermark conflicts
```

**Liquid Clustering:**
```python
cluster_by=["customer_id", "product_id"]
```
**Why This Clustering?**
```
✅ Queries often filter by customer OR product
✅ Co-locates related data for faster reads
✅ Better than partitioning (more flexible)
✅ Auto-adjusts as data evolves
```

---

### **🥇 Gold Layer Implementation**

**sales_analytics.py**
```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, sum, window, ...

@dp.table(
    name="sales_metrics",
    table_properties={
        "delta.autoOptimize.autoCompact": "true",
        "delta.deletedFileRetentionDuration": "interval 120 days",
        "delta.logRetentionDuration": "interval 120 days"
    }
)
def sales_metrics_aggregated():
    df = (
        spark.readStream.option("mergeSchema", "true").table("enriched_sales")
        .withWatermark("sale_date", "1 minutes")
        .groupBy(window("sale_date", "1 minute"))
        .agg(
            sum("amount").alias("total_sales"),
            avg("amount").alias("avg_transaction_value"),
            approx_count_distinct("customer_id").alias("unique_customers"),
            # ... 14 more metrics
        )
        .withColumn("revenue_per_customer", col("total_sales") / col("unique_customers"))
        # ... 3 more computed columns
    )
    return df
```

**Windowed Aggregation Explained:**
```
withWatermark("sale_date", "1 minutes"):
  • Handles late data up to 1 minute
  • Events with timestamp < (max_timestamp - 1 min) are dropped

groupBy(window("sale_date", "1 minute")):
  • Creates 1-minute tumbling windows
  • Window: [10:00:00, 10:01:00), [10:01:00, 10:02:00), ...

agg(...):
  • 17 business metrics per window
  • Executes once per window after watermark passes
```

**Why 1-Minute Window?**
```
Business Requirement: "Real-time KPIs"

Trade-offs:
  Short window (1 min):
    ✅ Near-real-time visibility
    ✅ Fast alerts
    ❌ More computation

  Long window (15 min):
    ✅ Less computation
    ❌ Delayed insights
    ❌ Misses rapid changes

Decision: 1 min balances latency vs cost
```

---

## 🔥 Advanced Patterns

### **1. Schema Evolution**

**Implementation:**
```python
# Bronze layer
spark.readStream.option("mergeSchema", "true").table("source")

# Tables
table_properties={
    "delta.enableChangeDataFeed": "true",
    "delta.columnMapping.mode": "name"
}
```

**How It Works:**
```
Source adds column "email":
  ↓
Bronze: mergeSchema reads new column
  ↓
Silver view: Column passes through
  ↓
Silver CDC: Auto CDC includes new column
  ↓
Silver join: mergeSchema propagates column
  ↓
Gold: Column available for aggregation

NO CODE CHANGES NEEDED!
```

---

### **2. Late Data Handling**

**Watermark Strategy:**
```python
.withWatermark("sale_date", "1 minutes")
```

**Timeline Example:**
```
Current event time: 10:00:00
Watermark: 10:00:00 - 1 min = 09:59:00

Window [09:59:00, 10:00:00]:
  ✅ Sale at 09:59:55 arrives at 10:00:30 → INCLUDED
  ❌ Sale at 09:58:30 arrives at 10:00:30 → DROPPED

Trade-off: Longer watermark = more late data handled = higher latency
```

---

### **3. Multi-Source Append Flows**

**Pattern:**
```python
@dlt.append_flow(target="append_sales")
def source1():
    return spark.readStream.table("bhubaneswar_order")

@dlt.append_flow(target="append_sales")
def source2():
    return spark.readStream.table("khordha_order")
```

**Why This Works:**
```
✅ Each flow processes independently
✅ Different schemas handled gracefully
✅ Easy to add new cities (new flow)
✅ One source failure doesn't break others
```

---

## ⚡ Performance Optimizations

### **1. Liquid Clustering**
```python
cluster_by=["customer_id", "product_id"]
```
**Benefit:** 30-70% faster queries on clustered columns

### **2. Auto-Optimization**
```python
"delta.autoOptimize.autoCompact": "true"
```
**Benefit:** Compacts small files automatically during writes

### **3. Change Data Feed**
```python
"delta.enableChangeDataFeed": "true"
```
**Benefit:** Efficient downstream change tracking

### **4. VACUUM Retention**
```python
"delta.deletedFileRetentionDuration": "interval 120 days"
```
**Benefit:** 20-40% storage reduction after first VACUUM

---

## 🎯 Design Decisions

### **Decision 1: Why Streaming Tables (not Materialized Views)?**
```
Context: Real-time sales analytics

Streaming Tables:
  ✅ Continuous incremental processing
  ✅ Low latency (seconds)
  ✅ Append-only or CDC patterns
  ✅ Handles late data with watermarks

Materialized Views:
  ❌ Batch processing (full/partial refresh)
  ❌ Higher latency (minutes)
  ✅ Good for aggregations from batch sources
  ❌ No watermarking

Decision: Streaming tables for real-time requirement
```

---

### **Decision 2: Why SCD Type 1 (not Type 2)?**
```
Context: Customer and product dimensions

Business Need:
  "Show current customer/product information in reports"

SCD Type 1:
  ✅ One record per entity
  ✅ Simple joins
  ✅ Lower storage
  ✅ Matches business requirement

SCD Type 2:
  ❌ Multiple records per entity
  ❌ Complex join logic (__START_AT, __END_AT)
  ❌ Higher storage
  ✅ Preserves history (not needed here)

Decision: Type 1 for simplicity and cost
```

---

### **Decision 3: Why Stream-Batch Join?**
```
Context: Enriching sales with customer/product info

Stream-Batch:
  ✅ Fact stream + dimension snapshots
  ✅ Dimensions read fresh each micro-batch
  ✅ No watermark conflicts
  ✅ Efficient pattern

Stream-Stream:
  ❌ Requires watermarks on both sides
  ❌ Complex state management
  ❌ Higher memory usage
  ❌ Overkill for slowly-changing dimensions

Decision: Stream-batch for efficiency
```

---

### **Decision 4: Why 1-Minute Windows?**
```
Context: Business KPIs for dashboards

Options:
  • 10 seconds → Too granular, noisy data
  • 1 minute → Chosen (balance)
  • 5 minutes → Too coarse, delayed insights

Decision: 1-minute tumbling windows
  ✅ Near-real-time visibility
  ✅ Smooth metrics (not noisy)
  ✅ Acceptable latency for business
  ✅ Reasonable compute cost
```

---

## 📚 Further Reading

* [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md) - Executive summary
* [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md) - Build instructions
* [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) - Setup steps
* [SCHEMA_EVOLUTION_DEMO.md](SCHEMA_EVOLUTION_DEMO.md) - Schema evolution examples

---

**Architecture Summary:**
* ✅ Medallion pattern with clear layer separation
* ✅ Streaming + CDC for real-time dimension management
* ✅ Stream-batch join for enrichment
* ✅ Windowed aggregation for business KPIs
* ✅ Production features (schema evolution, VACUUM, clustering)

**This architecture is production-ready and interview-winning!** 🚀
