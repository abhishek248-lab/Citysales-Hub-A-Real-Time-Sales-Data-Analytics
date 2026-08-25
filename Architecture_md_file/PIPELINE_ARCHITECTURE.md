# City Sales Hub - Pipeline Architecture

## Overview
**Pipeline Name:** city-sales-hub  
**Pipeline ID:** 4c2537fd-6bf7-4fe3-b4fb-0a6e7a1be26f  
**Catalog:** workspace  
**Schema:** stream_data  
**Architecture:** Medallion (Bronze → Silver → Gold)  
**Processing Mode:** Serverless, Photon-enabled  
**Status:** Production (Development=false)  

---

## Pipeline Configuration

* **Root Path:** `/Workspace/Users/abhishekgajendra.04@gmail.com/city-sales-hub`
* **Source Files:** All files under `transformations/**`
* **Continuous Mode:** Disabled (batch processing)
* **Channel:** CURRENT
* **Latest Updates:** 5 successful completed runs

---

## Architecture Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                         RAW DATA SOURCES                        │
│                      (Unity Catalog Volumes)                    │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Ingestion)                     │
│                    Auto Loader (cloudFiles)                     │
│                                                                 │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐ │
│  │ customers_bronze │  │ product_bronze   │  │orders_bronze │ │
│  │ (Streaming Table)│  │ (Streaming Table)│  │(Streaming    │ │
│  │                  │  │                  │  │ Table)       │ │
│  │ - Change Feed    │  │ - Change Feed    │  │- Change Feed │ │
│  │ - Data Quality   │  │ - Data Quality   │  │- Multiple    │ │
│  │   Expectations   │  │   Expectations   │  │  Sources     │ │
│  └──────────────────┘  └──────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              SILVER LAYER - Transform (Temporary Views)         │
│                                                                 │
│  ┌─────────────────────┐ ┌─────────────────────┐ ┌──────────┐ │
│  │customers_transform  │ │products_transform   │ │orders_   │ │
│  │_silver              │ │_silver              │ │transform │ │
│  │(Temp View)          │ │(Temp View)          │ │_silver   │ │
│  │                     │ │                     │ │(TempView)│ │
│  │- Deduplication      │ │- Deduplication      │ │- Dedupe  │ │
│  │- Name formatting    │ │- Name formatting    │ │- Filters │ │
│  │- Date normalization │ │- Date normalization │ │- Calc    │ │
│  └─────────────────────┘ └─────────────────────┘ └──────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│           SILVER LAYER - CDC (Streaming Tables)                 │
│                   Auto CDC Flows                                │
│                                                                 │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐ │
│  │customers_CDC     │  │products_CDC      │  │orders_CDC    │ │
│  │_silver           │  │_silver           │  │_silver       │ │
│  │(Streaming Table) │  │(Streaming Table) │  │(Streaming    │ │
│  │                  │  │                  │  │ Table)       │ │
│  │- SCD Type 1      │  │- SCD Type 2      │  │- SCD Type 2  │ │
│  │- Key: customer_id│  │- Key: product_id │  │- Key:order_id│ │
│  │- Upserts only    │  │- History tracked │  │- History     │ │
│  └──────────────────┘  └──────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                GOLD LAYER (Business Metrics)                    │
│                  Materialized Views                             │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 1. gold_realtime_sales                                  │   │
│  │    - Daily sales metrics (revenue, orders, customers)   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 2. gold_product_demand                                  │   │
│  │    - Product quantity sold & order frequency            │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 3. gold_customer_behavior                               │   │
│  │    - Customer spending patterns & segmentation          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 4. gold_product_performance                             │   │
│  │    - Product revenue & unit sales analysis              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 5. gold_geographic_market                               │   │
│  │    - Revenue by country/state/city                      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 6. gold_payment_analysis                                │   │
│  │    - Payment method revenue & transaction metrics       │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 7. gold_price_demand                                    │   │
│  │    - Historical price changes vs purchase demand        │   │
│  │    - Uses SCD Type 2 temporal joins                     │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Layer Details

### BRONZE LAYER - Data Ingestion

#### 1. customers_bronze
**File:** `transformations/bronze_layer/ingestion_customers.py`  
**Type:** Streaming Table  
**Source:** `/Volumes/workspace/store/customer_info_raw` (CSV)  
**Schema Location:** `/Volumes/workspace/store/sales_schema/customers_schema`  

**Features:**
* Auto Loader (cloudFiles) with schema evolution
* Change Data Feed enabled
* Column mapping mode enabled
* 30-day retention for deleted files and logs

**Data Quality Rules:**
* `email is not null`
* `date_of_birth is not null`
* `street_name is not null`
* Violation Action: DROP ROW

**Key Columns:** customer_id, customer_name, email, date_of_birth, street_name, city, state, country, gender

---

#### 2. product_bronze
**File:** `transformations/bronze_layer/ingestion_products.py`  
**Type:** Streaming Table  
**Source:** `/Volumes/workspace/store/product_info_raw` (CSV)  
**Schema Location:** `/Volumes/workspace/store/sales_schema/products_schema`  

**Features:**
* Auto Loader (cloudFiles) with schema evolution
* Change Data Feed enabled
* Column mapping mode enabled
* 120-day retention for deleted files and logs

**Data Quality Rules:**
* `category is not null`
* `product_name is not null`
* `price > 0`
* `expiry_date is not null`
* Violation Action: DROP ROW

**Key Columns:** product_id, product_name, category, price, expiry_date

---

#### 3. orders_bronze
**File:** `transformations/bronze_layer/ingestion_orders.py`  
**Type:** Streaming Table  
**Sources:** MULTIPLE (Append Flows)
  * Bhubaneswar: `/Volumes/workspace/store/bhubaneswar_order_raw`
  * Khordha: `/Volumes/workspace/store/khordha_order_raw`  
**Schema Locations:**
  * `/Volumes/workspace/store/sales_schema/bhubaneswar_order_schema`
  * `/Volumes/workspace/store/sales_schema/khordha_order_schema`

**Features:**
* Multiple append flows (2 cities)
* Auto Loader (cloudFiles) with schema evolution
* Change Data Feed enabled
* Column mapping mode enabled
* 120-day retention for deleted files and logs

**Data Quality Rules:**
* `quantity > 0`
* `amount > 0`
* `order_date is not null`
* Violation Action: DROP ROW

**Key Columns:** order_id, customer_id, product_id, quantity, amount, order_date, payment_method

---

### SILVER LAYER - Transformation & CDC

#### Transform Layer (Temporary Views)

These are intermediate preprocessing views that clean and normalize data before CDC processing.

##### 1. customers_transform_silver
**File:** `transformations/Silver layer/transform_silver/transform_customers.py`  
**Type:** Temporary View (non-materialized)  
**Source:** workspace.stream_data.customers_bronze (with change feed)

**Transformations:**
* Remove duplicates
* Uppercase and trim customer names
* Normalize date_of_birth to multiple formats:
  * yyyy-MM-dd
  * yyyy/MM/dd
  * dd/MM/yyyy

---

##### 2. products_transform_silver
**File:** `transformations/Silver layer/transform_silver/transform_products.py`  
**Type:** Temporary View (non-materialized)  
**Source:** workspace.stream_data.product_bronze (with change feed)

**Transformations:**
* Remove duplicates
* Title case product names (initcap)
* Normalize expiry_date to multiple formats:
  * yyyy-MM-dd
  * yyyy/MM/dd
  * dd/MM/yyyy

---

##### 3. orders_transform_silver
**File:** `transformations/Silver layer/transform_silver/transform_orders.py`  
**Type:** Temporary View (non-materialized)  
**Source:** workspace.stream_data.orders_bronze (with change feed)

**Transformations:**
* Remove duplicates
* Filter out null customer_id
* Filter out null product_id
* Calculate total_amount (amount × quantity)
* Normalize Order_date to multiple formats:
  * yyyy-MM-dd
  * yyyy/MM/dd
  * dd/MM/yyyy

---

#### CDC Layer (Streaming Tables)

These tables use Auto CDC to maintain slowly changing dimensions.

##### 1. customers_CDC_silver
**File:** `transformations/Silver layer/Use_auto_CDC/customers_CDC_silver.py`  
**Type:** Streaming Table with Auto CDC  
**Source:** customers_transform_silver  
**SCD Type:** Type 1 (Upserts only - no history)

**CDC Configuration:**
* **Keys:** customer_id
* **Sequence By:** _commit_timestamp
* **Excluded Columns:** _change_type, _commit_version, _commit_timestamp
* **Retention:** 120 days for deleted files and logs

**Behavior:**
* Latest record per customer_id
* Updates overwrite previous values
* No historical tracking

---

##### 2. products_CDC_silver
**File:** `transformations/Silver layer/Use_auto_CDC/products_CDC_silver.py`  
**Type:** Streaming Table with Auto CDC  
**Source:** products_transform_silver  
**SCD Type:** Type 2 (Full history tracking)

**CDC Configuration:**
* **Keys:** product_id
* **Sequence By:** _commit_timestamp
* **Excluded Columns:** _change_type, _commit_version, _commit_timestamp
* **Retention:** 120 days for deleted files and logs

**Behavior:**
* Tracks all changes to products
* Adds __START_AT and __END_AT timestamps
* Current records have __END_AT = NULL
* Historical records preserved with validity periods

**Use Case:** Price history tracking, product attribute changes over time

---

##### 3. orders_CDC_silver
**File:** `transformations/Silver layer/Use_auto_CDC/orders_CDC_silver.py`  
**Type:** Streaming Table with Auto CDC  
**Source:** orders_transform_silver  
**SCD Type:** Type 2 (Full history tracking)

**CDC Configuration:**
* **Keys:** order_id
* **Sequence By:** _commit_timestamp
* **Excluded Columns:** _change_type, _commit_version, _commit_timestamp
* **Retention:** 120 days for deleted files and logs

**Behavior:**
* Tracks all changes to orders
* Adds __START_AT and __END_AT timestamps
* Current records have __END_AT = NULL
* Historical records preserved with validity periods

**Use Case:** Order modifications, cancellations, refunds tracking

---

### GOLD LAYER - Business Analytics

#### 1. gold_realtime_sales
**Type:** Materialized View  
**Sources:** orders_CDC_silver (current records only)

**Metrics:**
* Daily aggregation (sale_date)
* Total revenue
* Total orders
* Unique customers
* Average order value
* Max/min order values

**Query Pattern:** Filters __END_AT IS NULL for current records

---

#### 2. gold_product_demand
**Type:** Materialized View  
**Sources:** orders_CDC_silver + products_CDC_silver (current records)

**Metrics:**
* Total quantity sold per product
* Order count per product
* Product details (name, category, price)

**Join:** orders.product_id = products.product_id

---

#### 3. gold_customer_behavior
**Type:** Materialized View  
**Sources:** orders_CDC_silver (current) + customers_CDC_silver

**Metrics:**
* Total spent per customer
* Purchase count
* Average purchase value
* Last purchase date
* Customer segmentation:
  * Premium: > $10,000
  * Gold: > $5,000
  * Silver: > $2,000
  * Bronze: ≤ $2,000

**Join:** orders.customer_id = customers.customer_id

---

#### 4. gold_product_performance
**Type:** Materialized View  
**Sources:** orders_CDC_silver (current) + products_CDC_silver

**Metrics:**
* Total revenue per product
* Units sold
* Order frequency
* Average revenue per order

**Join:** orders.product_id = products.product_id

---

#### 5. gold_geographic_market
**Type:** Materialized View  
**Sources:** orders_CDC_silver (current) + customers_CDC_silver

**Metrics:**
* Total revenue by geography (country/state/city)
* Total orders
* Average order value

**Join:** orders.customer_id = customers.customer_id

---

#### 6. gold_payment_analysis
**Type:** Materialized View  
**Sources:** orders_CDC_silver (current records only)

**Metrics:**
* Revenue by payment method
* Order count by payment method
* Average transaction value
* Max/min transaction values

---

#### 7. gold_price_demand
**Type:** Materialized View  
**Sources:** products_CDC_silver (ALL history) + orders_CDC_silver (current)

**Special Feature:** Temporal Join with SCD Type 2

**Metrics:**
* Previous price vs current price
* Price change percentage
* Previous units sold vs current units sold
* Purchase change percentage

**Join Logic:**
* Matches orders to the product price that was active at order_date
* Uses temporal validity: `order_date >= __START_AT AND (order_date < __END_AT OR __END_AT IS NULL)`
* Left join to include price periods with zero sales

**Use Case:** Analyze price elasticity and demand response to price changes

---

## Data Flow Summary

### Ingestion Flow
1. **Raw CSV files** arrive in Unity Catalog Volumes
2. **Auto Loader** (cloudFiles) detects new files automatically
3. **Bronze tables** ingest with schema evolution and data quality checks
4. **Change Data Feed** enabled for downstream CDC processing

### Transformation Flow
1. **Bronze tables** stream to transform temporary views
2. **Temporary views** clean, deduplicate, and normalize data
3. **Auto CDC flows** apply change data capture:
   * Customers: SCD Type 1 (current state only)
   * Products: SCD Type 2 (full history)
   * Orders: SCD Type 2 (full history)
4. **CDC Silver tables** maintain slowly changing dimensions

### Analytics Flow
1. **Gold materialized views** read from Silver CDC tables
2. **Batch aggregations** compute business metrics
3. **Real-time analytics** filter current records (__END_AT IS NULL)
4. **Historical analysis** leverages full SCD Type 2 history
5. **Temporal joins** match historical states (price_demand view)

---

## Key Technical Patterns

### 1. Multi-Source Ingestion (Append Flows)
**Example:** orders_bronze
* Two append flows feed the same streaming table
* Data from Bhubaneswar and Khordha merged automatically
* Single downstream processing path

### 2. Temporary Views for Preprocessing
**Pattern:** Bronze → Transform View → CDC Silver
* Non-materialized temporary views reduce storage
* Clean separation of concerns
* Reusable transformation logic

### 3. Mixed SCD Types
* **Customers:** Type 1 (customers rarely change, current state sufficient)
* **Products:** Type 2 (price changes critical for analysis)
* **Orders:** Type 2 (order lifecycle tracking)

### 4. Change Data Feed Integration
* Bronze tables enable CDC feed
* Silver transform views consume change feed
* Supports incremental processing and CDC flows

### 5. Temporal Joins (SCD Type 2)
**Example:** gold_price_demand
* Joins orders to the product price active at order time
* Uses __START_AT and __END_AT for validity
* Enables historical "as-of" analysis

### 6. Data Quality Gates
* Bronze layer enforces expectations
* Invalid rows dropped immediately
* Clean data propagates to Silver and Gold

---

## Schema Conventions

### Bronze Layer
* Raw column names (as ingested)
* Change feed columns: _change_type, _commit_version, _commit_timestamp

### Silver Layer (CDC)
* Cleaned and normalized columns
* SCD Type 2 columns: __START_AT, __END_AT (when applicable)
* Primary keys defined in CDC configuration

### Gold Layer
* Business-friendly metric names
* Aggregated dimensions
* Calculated fields and segments

---

## File Organization

```
city-sales-hub/
├── transformations/
│   ├── bronze_layer/
│   │   ├── ingestion_customers.py
│   │   ├── ingestion_products.py
│   │   └── ingestion_orders.py
│   │
│   ├── Silver layer/
│   │   ├── transform_silver/
│   │   │   ├── transform_customers.py
│   │   │   ├── transform_products.py
│   │   │   └── transform_orders.py
│   │   │
│   │   └── Use_auto_CDC/
│   │       ├── customers_CDC_silver.py
│   │       ├── products_CDC_silver.py
│   │       └── orders_CDC_silver.py
│   │
│   └── Gold Layer/
│       └── gold_business_metrics.py
│
├── explorations/
│   └── constants.py
│
└── PIPELINE_ARCHITECTURE.md (this file)
```

---

## Table Retention Policies

| Layer | Deleted Files | Logs | Rationale |
|-------|--------------|------|----------|
| Bronze - Customers | 30 days | 30 days | Shorter retention for high-volume customer data |
| Bronze - Products | 120 days | 120 days | Longer retention for reference data |
| Bronze - Orders | 120 days | 120 days | Transaction data needs longer retention |
| Silver - All CDC | 120 days | 120 days | Historical analysis requires extended retention |

---

## Performance Optimizations

* **Serverless compute:** Automatic scaling and cost optimization
* **Photon engine:** Accelerated Spark processing
* **Schema evolution:** Automatic handling of new columns
* **Temporary views:** Reduced storage overhead
* **Batch materialized views:** Efficient aggregation in Gold layer
* **Filtered reads:** __END_AT IS NULL for current records only

---

## Monitoring & Operations

**Latest Status:** IDLE  
**Recent Updates:** 5 completed runs (all successful)  
**Last Update:** August 18, 2026

**Key Metrics to Monitor:**
1. Bronze ingestion lag (Auto Loader backlog)
2. CDC flow processing time
3. Gold view refresh duration
4. Data quality violation counts
5. Schema evolution events

---

## Use Cases Enabled

✅ **Real-time Sales Dashboards** - gold_realtime_sales  
✅ **Product Performance Tracking** - gold_product_demand, gold_product_performance  
✅ **Customer Segmentation** - gold_customer_behavior  
✅ **Geographic Market Analysis** - gold_geographic_market  
✅ **Payment Method Insights** - gold_payment_analysis  
✅ **Price Elasticity Analysis** - gold_price_demand (with temporal joins)  
✅ **Historical Audit Trail** - SCD Type 2 for products and orders  
✅ **Multi-City Order Consolidation** - Append flows in orders_bronze  

---

## Technical Stack

* **Framework:** Lakeflow Spark Declarative Pipelines (SDP)
* **Storage:** Delta Lake with Unity Catalog
* **Compute:** Serverless with Photon
* **Ingestion:** Auto Loader (cloudFiles)
* **CDC:** Auto CDC Flows (SCD Type 1 & 2)
* **Language:** Python (pyspark.pipelines)
* **Architecture:** Medallion (Bronze/Silver/Gold)

---

**Document Version:** 1.0  
**Last Updated:** August 26, 2026  
**Pipeline Version:** Production