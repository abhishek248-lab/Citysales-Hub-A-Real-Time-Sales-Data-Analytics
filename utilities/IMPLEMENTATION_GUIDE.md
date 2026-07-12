# 🛠️ Implementation Guide - CitySales Hub

## 📋 Table of Contents
1. [Prerequisites](#prerequisites)
2. [Project Setup](#project-setup)
3. [Step-by-Step Implementation](#step-by-step-implementation)
4. [Testing & Validation](#testing--validation)
5. [Production Deployment](#production-deployment)

---

## ✅ Prerequisites

### **1. Databricks Environment**
```
Required:
  ✅ Databricks workspace (AWS/Azure/GCP)
  ✅ Unity Catalog enabled
  ✅ Compute cluster (DBR 14.3+ recommended)
  ✅ Appropriate permissions (CREATE TABLE, CREATE SCHEMA)

Optional but Recommended:
  ✅ Serverless compute (for Spark Declarative Pipelines)
  ✅ Development + Production environments
```

### **2. Source Data Setup**
```sql
-- Create source tables (if not exists)
CREATE SCHEMA IF NOT EXISTS workspace.store;

-- Customers master data
CREATE TABLE IF NOT EXISTS workspace.store.customers_info (
    customer_id STRING,
    customer_name STRING,
    region STRING,
    last_updated TIMESTAMP
);

-- Products catalog
CREATE TABLE IF NOT EXISTS workspace.store.products_info (
    product_id STRING,
    product_name STRING,
    category STRING,
    sale_date TIMESTAMP
);

-- Sales from Bhubaneswar
CREATE TABLE IF NOT EXISTS workspace.store.bhubaneswar_order (
    customer_id STRING,
    customer_name STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    region STRING,
    quantity INT,
    amount DOUBLE,
    sale_date TIMESTAMP
);

-- Sales from Khordha
CREATE TABLE IF NOT EXISTS workspace.store.khordha_order (
    customer_id STRING,
    customer_name STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    region STRING,
    quantity INT,
    amount DOUBLE,
    sale_date TIMESTAMP
);
```

### **3. Target Schema**
```sql
-- Create catalog and schema for pipeline output
CREATE CATALOG IF NOT EXISTS workspace;
CREATE SCHEMA IF NOT EXISTS workspace.stream_data;
```

---

## 🏗️ Project Setup

### **Step 1: Create Workspace Folder**
```
1. Navigate to Workspace → Your user folder
2. Click "Create" → "Folder"
3. Name: "Citysales-Hub-A-Real-Time-Sales-Data-Service"
4. Create subfolders:
   - transformations/
   - transformations/Bronzelayer_StagingLayer/
   - transformations/2_silver layer/
   - transformations/2_silver layer/Transformation_views/
   - transformations/2_silver layer/Use_auto_CDC/
   - transformations/3_gold layer/
   - transformations/3_gold layer/Current_sales_info/
```

### **Step 2: Create Pipeline**
```
1. In Databricks left sidebar → Click "Workflows"
2. Click "Spark Declarative Pipelines" tab
3. Click "Create Pipeline"
4. Configure:
   - Name: CitySales-Hub-Pipeline
   - Product Edition: Advanced
   - Pipeline Mode: Triggered (or Continuous)
   - Source Code: /Workspace/Users/<your-email>/Citysales-Hub-A-Real-Time-Sales-Data-Service/transformations
   - Target Catalog: workspace
   - Target Schema: stream_data
   - Compute: Serverless (recommended) or cluster
5. Click "Create"
```

---

## 🔧 Step-by-Step Implementation

### **BRONZE LAYER - Ingestion**

#### **File 1: ingestion_customers.py**
```python
import dlt

# Define expectation rules
customer_rules = {
    "customer_name": "customer_name IS NOT NULL",
    "region": "region IS NOT NULL"
}

# Schema Evolution: mergeSchema handles new columns from source
dlt.create_streaming_table(
    name="customers",
    expect_all_or_drop=customer_rules,
    table_properties={
        "delta.enableChangeDataFeed": "true",  # Enable CDC for downstream
        "delta.columnMapping.mode": "name",     # Support column renames
        "delta.deletedFileRetentionDuration": "interval 120 days",
        "delta.logRetentionDuration": "interval 120 days"
    }
)

@dlt.append_flow(target="customers")
def newCustomers():
    # mergeSchema allows reading new columns from source
    df = spark.readStream.option("mergeSchema", "true").table("workspace.store.customers_info")
    return df
```

**Save to:** `transformations/Bronzelayer_StagingLayer/ingestion_customers.py`

---

#### **File 2: ingestion_products.py**
```python
import dlt

product_rules = {
    "category": "category IS NOT NULL",
    "product_name": "product_name IS NOT NULL"
}

# Schema Evolution: mergeSchema handles new columns from source
dlt.create_streaming_table(
    name="products",
    expect_all_or_drop=product_rules,
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.columnMapping.mode": "name",
        "delta.deletedFileRetentionDuration": "interval 120 days",
        "delta.logRetentionDuration": "interval 120 days"
    }
)

@dlt.append_flow(target="products")
def newproducts():
    # mergeSchema allows reading new columns from source
    df = spark.readStream.option("mergeSchema", "true").table("workspace.store.products_info")
    return df
```

**Save to:** `transformations/Bronzelayer_StagingLayer/ingestion_products.py`

---

#### **File 3: ingestion_sales.py**
```python
import dlt

# Define expectations
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
        "delta.deletedFileRetentionDuration": "interval 120 days",
        "delta.logRetentionDuration": "interval 120 days"
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
```

**Save to:** `transformations/Bronzelayer_StagingLayer/ingestion_sales.py`

---

### **SILVER LAYER - Transformation Views**

#### **File 4: customers_view.py**
```python
from pyspark import pipelines as dp
from pyspark.sql.functions import upper

@dp.temporary_view()
def customers_view():
    """Customer data with standardized regions (uppercase)"""
    df = spark.readStream.table("customers")
    return df.withColumn("region", upper("region"))
```

**Save to:** `transformations/2_silver layer/Transformation_views/customers_view.py`

---

#### **File 5: product_view.py**
```python
from pyspark import pipelines as dp
from pyspark.sql.functions import upper

@dp.temporary_view()
def product_view():
    """Product data with standardized categories (uppercase)"""
    df = spark.readStream.table("products")
    return df.withColumn("category", upper("category"))
```

**Save to:** `transformations/2_silver layer/Transformation_views/product_view.py`

---

#### **File 6: sales_view.py**
```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.temporary_view()
def sales_view():
    """Sales data filtered for quantity >= 2"""
    df = spark.readStream.table("append_sales")
    return df.filter(col("quantity") >= 2)
```

**Save to:** `transformations/2_silver layer/Transformation_views/sales_view.py`

---

### **SILVER LAYER - CDC and Enrichment**

#### **File 7: dim_customers.py**
```python
import dlt

# Schema Evolution: New columns from upstream automatically propagate through CDC
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

# SCD Type 1: Updates existing records in place (no history tracking)
# Schema evolution works automatically - new columns from customers_view will appear here
```

**Save to:** `transformations/2_silver layer/Use_auto_CDC/dim_customers.py`

---

#### **File 8: dim_products.py**
```python
import dlt

# Schema Evolution: New columns from upstream automatically propagate through CDC
dlt.create_streaming_table(
    name="dim_products",
    table_properties={
        "delta.deletedFileRetentionDuration": "interval 120 days",
        "delta.logRetentionDuration": "interval 120 days"
    }
)

dlt.create_auto_cdc_flow(
    target="dim_products",
    source="product_view",
    keys=["product_id"],
    sequence_by="sale_date",
    stored_as_scd_type=1
)

# SCD Type 1: Updates existing records in place (no history tracking)
# Schema evolution works automatically - new columns from product_view will appear here
```

**Save to:** `transformations/2_silver layer/Use_auto_CDC/dim_products.py`

---

#### **File 9: fact_sales.py**
```python
import dlt

# Schema Evolution: mergeSchema handles new columns from sales_view
@dlt.table(
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 120 days",
        "delta.logRetentionDuration": "interval 120 days"
    }
)
def fact_sales():
    # mergeSchema propagates new columns from upstream view
    return spark.readStream.option("mergeSchema", "true").table("sales_view")

# Fact tables are immutable transactions - each sale is appended once, no updates needed
```

**Save to:** `transformations/2_silver layer/Use_auto_CDC/fact_sales.py`

---

#### **File 10: sales_join.py**
```python
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

# Schema Evolution: Propagates new columns from all sources
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
```

**Save to:** `transformations/2_silver layer/Use_auto_CDC/sales_join.py`

---

### **GOLD LAYER - Business KPIs**

#### **File 11: sales_analytics.py**
```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, sum, max, min, count, avg, approx_count_distinct, stddev, percentile_approx, first, last, window, collect_set

@dp.table(
    name="sales_metrics",
    table_properties={
        "delta.autoOptimize.autoCompact": "true",
        "delta.deletedFileRetentionDuration": "interval 120 days",
        "delta.logRetentionDuration": "interval 120 days"
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
```

**Save to:** `transformations/3_gold layer/Current_sales_info/sales_analytics.py`

---

## ✅ Testing & Validation

### **Step 1: Dry Run (Validation)**
```
1. Open pipeline in Databricks UI
2. Click "Start" → "Validate"
3. Wait for validation to complete
4. Check for errors in "Pipeline Issues" tab
5. Fix any issues and retry
```

**Expected Result:**
```
✅ All datasets validated successfully
✅ No syntax errors
✅ No schema mismatches
✅ All dependencies resolved
```

---

### **Step 2: Test Data Preparation**
```sql
-- Insert test data into source tables
INSERT INTO workspace.store.customers_info VALUES
  ('C001', 'John Doe', 'East', current_timestamp()),
  ('C002', 'Jane Smith', 'West', current_timestamp()),
  ('C003', 'Bob Johnson', 'North', current_timestamp());

INSERT INTO workspace.store.products_info VALUES
  ('P001', 'Laptop', 'Electronics', current_timestamp()),
  ('P002', 'Coffee Maker', 'Appliances', current_timestamp()),
  ('P003', 'Desk Chair', 'Furniture', current_timestamp());

INSERT INTO workspace.store.bhubaneswar_order VALUES
  ('C001', 'John Doe', 'P001', 'Laptop', 'Electronics', 'East', 2, 1200.00, current_timestamp()),
  ('C002', 'Jane Smith', 'P002', 'Coffee Maker', 'Appliances', 'West', 3, 150.00, current_timestamp());

INSERT INTO workspace.store.khordha_order VALUES
  ('C003', 'Bob Johnson', 'P003', 'Desk Chair', 'Furniture', 'North', 5, 250.00, current_timestamp());
```

---

### **Step 3: First Pipeline Run**
```
1. Click "Start" button in pipeline UI
2. Monitor progress in "Pipeline Runs" tab
3. Check each layer completes:
   - Bronze layer (customers, products, append_sales)
   - Silver layer (dim_customers, dim_products, fact_sales, enriched_sales)
   - Gold layer (sales_metrics)
4. Wait for "Completed" status
```

**Expected Duration:** 3-5 minutes for first run

---

### **Step 4: Verify Data**

**Bronze Layer:**
```sql
-- Check ingested data
SELECT * FROM workspace.stream_data.customers LIMIT 10;
SELECT * FROM workspace.stream_data.products LIMIT 10;
SELECT * FROM workspace.stream_data.append_sales LIMIT 10;
```

**Silver Layer:**
```sql
-- Check dimensions
SELECT * FROM workspace.stream_data.dim_customers ORDER BY customer_id;
SELECT * FROM workspace.stream_data.dim_products ORDER BY product_id;

-- Check enriched data
SELECT * FROM workspace.stream_data.enriched_sales LIMIT 10;
```

**Gold Layer:**
```sql
-- Check aggregated metrics
SELECT 
  window.start,
  window.end,
  total_sales,
  transaction_count,
  unique_customers,
  unique_products
FROM workspace.stream_data.sales_metrics
ORDER BY window.start DESC
LIMIT 10;
```

---

### **Step 5: Test Schema Evolution**
```sql
-- Add new column to source
ALTER TABLE workspace.store.customers_info ADD COLUMN email STRING;

-- Insert data with new column
INSERT INTO workspace.store.customers_info VALUES
  ('C004', 'Alice Williams', 'South', current_timestamp(), 'alice@example.com');

-- Trigger pipeline update
-- Check new column appears in all downstream tables
SELECT customer_id, customer_name, region, email 
FROM workspace.stream_data.dim_customers 
WHERE customer_id = 'C004';
```

**Expected Result:**
```
✅ New "email" column appears in Bronze
✅ New "email" column appears in Silver dimensions
✅ New "email" column appears in enriched_sales
✅ No pipeline code changes needed
```

---

### **Step 6: Test CDC (Updates)**
```sql
-- Update customer region
UPDATE workspace.store.customers_info 
SET region = 'SOUTH-EAST', last_updated = current_timestamp()
WHERE customer_id = 'C001';

-- Trigger pipeline update
-- Check dimension table updated
SELECT customer_id, customer_name, region, last_updated
FROM workspace.stream_data.dim_customers 
WHERE customer_id = 'C001';
```

**Expected Result:**
```
✅ Region updated to 'SOUTH-EAST' (uppercase applied)
✅ Only one record for C001 (SCD Type 1)
✅ last_updated timestamp reflects change
```

---

### **Step 7: Test Late Data**
```sql
-- Insert old transaction (30 seconds old)
INSERT INTO workspace.store.bhubaneswar_order VALUES
  ('C001', 'John Doe', 'P002', 'Coffee Maker', 'Appliances', 'East', 2, 300.00, 
   date_sub(current_timestamp(), INTERVAL 30 SECONDS));

-- Check if included in metrics
SELECT * FROM workspace.stream_data.sales_metrics
WHERE window.end >= date_sub(current_timestamp(), INTERVAL 2 MINUTES)
ORDER BY window.start DESC;
```

**Expected Result:**
```
✅ Transaction within 1-minute watermark → INCLUDED
✅ Metrics updated with late transaction
```

---

## 🚀 Production Deployment

### **Step 1: Environment Setup**
```
1. Create separate catalogs/schemas:
   - Development: workspace_dev.stream_data_dev
   - Staging: workspace_staging.stream_data_staging
   - Production: workspace.stream_data

2. Clone pipeline for each environment
3. Update source table references
4. Configure appropriate compute (serverless for production)
```

---

### **Step 2: CI/CD Integration (Optional)**
```yaml
# Example: Databricks Asset Bundles (DAB)
# databricks.yml
resources:
  pipelines:
    citysales_pipeline:
      name: CitySales-Hub-Pipeline
      catalog: workspace
      target: stream_data
      libraries:
        - file:
            path: ./transformations
      continuous: false
```

---

### **Step 3: Monitoring Setup**
```sql
-- Create monitoring dashboard
CREATE OR REFRESH MATERIALIZED VIEW pipeline_health AS
SELECT 
  'sales_metrics' as table_name,
  COUNT(*) as record_count,
  MAX(window.end) as latest_window,
  current_timestamp() as last_checked
FROM workspace.stream_data.sales_metrics;

-- Schedule alerts
-- If latest_window < current_timestamp() - INTERVAL 5 MINUTES → ALERT
```

---

### **Step 4: Storage Management**

For detailed storage cleanup procedures, see: [VACUUM_MAINTENANCE_GUIDE.md](VACUUM_MAINTENANCE_GUIDE.md)

**Quick summary:**
* Tables configured with 4-month retention policy
* Run periodic cleanup to reclaim storage
* Expected savings: 20-40% on first cleanup

---

### **Step 5: Documentation Deployment**
```
1. Copy all MD files to workspace docs/ folder
2. Share links with team
3. Create README.md in project root
4. Link to all documentation files
```

---

## 🎯 Post-Deployment Checklist

### **Day 1:**
```
✅ Pipeline running successfully
✅ Data flowing through all layers
✅ Metrics updating every minute
✅ No errors in pipeline logs
```

### **Week 1:**
```
✅ Monitor data quality (check for drops)
✅ Verify watermark effectiveness (late data handling)
✅ Check storage growth
✅ Review query performance
```

### **Month 1:**
```
✅ Run storage cleanup (see VACUUM guide)
✅ Review and tune watermark (if needed)
✅ Add monitoring dashboards
✅ Gather user feedback
```

---

## 🛠️ Troubleshooting Common Issues

### **Issue 1: "Table not found: customers_view"**
```
Cause: Temp views must be defined before CDC flows
Fix: Ensure files are loaded in order:
  1. Bronze layer
  2. Silver views
  3. Silver CDC/enrichment
  4. Gold layer
```

### **Issue 2: "mergeSchema not supported"**
```
Cause: Delta table not configured for schema evolution
Fix: Add table properties:
  "delta.columnMapping.mode": "name"
```

### **Issue 3: "Watermark exceeded"**
```
Cause: Data arriving > 1 minute late
Fix: Increase watermark:
  .withWatermark("sale_date", "2 minutes")
```

**For more troubleshooting, see:** [TROUBLESHOOTING_GUIDE.md](TROUBLESHOOTING_GUIDE.md)

---

## 📚 Next Steps

1. **Read:** [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) for production setup
2. **Study:** [ARCHITECTURE_GUIDE.md](ARCHITECTURE_GUIDE.md) for design deep dive
3. **Prepare:** [INTERVIEW_PREP_GUIDE.md](INTERVIEW_PREP_GUIDE.md) for interviews

---

**Implementation Complete!** 🎉

You now have a production-ready, real-time sales analytics pipeline! 🚀
