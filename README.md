## Project Overview :
My project name is City Sales Hub a real-time sales data analytics service built for **Madhav E-commerce** to analyze regional sales in real time across the Bhubaneswar and Khordha regions.It is built on the Databricks Lakehouse Platform using modern data engineering technologies such as Lakeflow Spark Declarative Pipelines, Spark Structured Streaming, Delta Lake and Pyspark and python. The pipeline ingests, validates, transforms, and processes sales data in real time, enabling real-time sales dashboards, regional performance tracking, and faster business reporting.

## Problem Statement :

Traditional sales data processing happens through batch pipelines, where data is collected from different sources and processed in batches before being stored in a data warehouse. This makes the overall data processing a long process, as data is available in the data warehouse only after the batch processing is completed.

Because of this delay, it becomes difficult for business teams to identify the latest business performance, sales trends in real time. Another challenge is that different business requirements require managing separate pipelines, which increases development time, development cost, and maintenance effort.


## Solution :
To solve this problem, I designed a data pipeline using the Databricks Lakehouse Platform and followed the Medallion Architecture, which consists of Bronze, Silver, and Gold layers. In the Bronze layer, I ingest the raw sales data and apply data quality validations using expectations. In the Silver layer, I clean and transform the data using SQL and Spark transformations. In the Gold layer, I perform business-level analysis using Spark SQL and prepare the data for reporting and analytics.


## 🛠️ Tech Stack

### ☁️ Platform
- **Databricks Lakehouse Platform**

### 👨‍💻 Programming & Processing
- **Python**
- **PySpark**
- **Apache Spark Structured Streaming**

### 🗄️ Storage & Data Lake
- **Delta Lake**
- **Delta Tables**

### 🔄 Data Engineering
- **Lakeflow Declarative Pipelines**
- **Streaming Tables**
- **Auto CDC**
- **Change Data Feed (CDC)**
- **Schema Evolution (`mergeSchema`)**
- **Lakeflow Expectations**
- **Column Mapping**
- **Auto Optimize**

### 🏛️ Data Architecture
- **Medallion Architecture (Bronze, Silver, Gold)**
- **Dimensional Modeling**
- **SCD Type 1 add SCD Type 2**
- **Window Aggregation**
- **Watermarking**

### Data Pipeline Architecture :
<img width="1536" height="1024" alt="ChatGPT Image Aug 31, 2026, 01_18_52 PM" src="https://github.com/user-attachments/assets/507763f0-5a2b-4208-8158-8611ba7df6b4" />

## Features & Implementation Details:
<img width="1412" height="702" alt="Screenshot 2026-09-02 164947" src="https://github.com/user-attachments/assets/56a7a253-5b20-41fc-a0a5-1944d752e613" />

## 📌 Synthetic Data Generator

- Developed a **synthetic streaming data generator** to simulate real-time business transactions for **Customers, Products, and Sales** across multiple regions (**Bhubaneswar** and **Khordha**).
- Created **Delta source tables** with the initial schema during the first execution and continuously generated **incremental records** in subsequent runs.
- Simulated independent streaming sources to validate **real-time ingestion**, **schema evolution**, and **incremental processing**.
- Enabled **`mergeSchema`** to automatically accommodate new columns without modifying the pipeline.
- Implemented a **multi-source append pattern**, allowing new regional sales streams to be added easily while preventing duplicate processing and ensuring fault isolation.

---

# 🥉 Bronze Layer (Raw Data Ingestion)

### Purpose
The Bronze layer ingests raw streaming data while ensuring data quality and schema consistency.

### Features

- Ingests streaming **Customer**, **Product**, and **Sales** data into Bronze Streaming Tables:
  - `customers`
  - `products`
  - `append_sales`
- Combines multiple city-wise sales streams using **Append Flows** to create a unified streaming sales dataset.
- Implements **Lakeflow Expectations** to enforce data quality rules:
  - NOT NULL validation
  - `amount > 0`
  - `quantity > 0`
- Supports **Schema Evolution** using **`mergeSchema`**, allowing upstream schema changes without manual intervention.
- Enables **Change Data Feed (CDC)** for downstream change tracking.
- Uses **Column Mapping** to support schema modifications such as column renames.
- Configures **120-day Delta retention** for governance, auditing, and Delta Time Travel.
- Stores validated raw data as the foundation for downstream transformations.

---

# 🥈 Silver Layer (Transformation & Enrichment)

## Transformation Layer

### Purpose
Transforms and standardizes raw data before dimensional modeling.

### Features

- Uses **Temporary Views** for lightweight preprocessing without persisting intermediate datasets.
- Standardizes business attributes:
  - Converts **Region** to uppercase.
  - Converts **Category** to uppercase.
- Filters records using business rules:
  - `quantity >= 2`
- Keeps transformations pipeline-private while minimizing storage overhead.

---

## Dimension & Fact Layer

### Features

- Implements **Auto CDC** using **SCD Type 1** to maintain the latest version of **Customer** and **Product** dimensions.
- Creates **Append-Only Fact Tables** to store immutable sales transactions.
- Performs an efficient **Stream-Batch Join** between streaming fact tables and batch dimension tables to generate an enriched sales dataset.
- Enriches transactional data with:
  - Customer information
  - Product information
  - Regional information
- Applies **Liquid Clustering** on:
  - `customer_id`
  - `product_id`
- Enables **Auto Optimize** for automatic file compaction and storage optimization.
- Supports **Schema Evolution** throughout the enrichment process, automatically propagating newly added columns.

---

# 🥇 Gold Layer (Business Analytics)

### Purpose
Generates real-time business KPIs for reporting and analytics.

### Features

- Builds a real-time analytics layer using **1-minute Tumbling Windows**.
- Configures a **1-minute Watermark** to handle late-arriving streaming events.
- Generates **21+ real-time Business KPIs** across multiple analytical domains.
- Computes:
  - 💰 Revenue Metrics
  - 👥 Customer Metrics
  - 📦 Product Metrics
  - 📈 Sales Volume Metrics
  - 🌍 Regional Metrics
  - ⏱️ Time-Based Metrics
- Produces optimized analytical datasets for business reporting and decision-making.
- Delivers low-latency metrics suitable for enterprise-scale real-time analytics.


### Output:
<img width="1770" height="711" alt="Screenshot 2026-07-12 160420" src="https://github.com/user-attachments/assets/c78e829d-27fb-4627-b81a-9b7dc6eb0788" />
<img width="1776" height="755" alt="Screenshot 2026-07-12 160349" src="https://github.com/user-attachments/assets/766122d1-7cc7-4118-abf5-29e0739e8c13" />


## 🚧 Challenges And Solved

### 1st:
- My CDC source contained technical metadata columns such as _commit_version, _commit_timestamp, and _change_type.
These columns were added because they are required by AUTO CDC to identify the type of change and process CDC records in the correct sequence
using sequence_by, which is important for implementing SCD Type 1 and SCD Type 2. However, I did not want these technical columns to remain in
my final Silver table because they are not business-related data.
- I resolved this by using the except_column_list option in create_auto_cdc_flow(). This allowed AUTO CDC to use the technical metadata columns
during CDC and SCD processing, while excluding them from the final Silver table, resulting in a clean table containing only business-relevant columns.

### 2nd:
- The Silver source contains change commits such as UPDATE and DELETE, while the Gold streaming query expects append-style data, so when Spark encounters these different types of changes, it gets confused about the events coming into the window; because of this, the watermark may not progress as expected, so Spark keeps the window open thinking that more relevant events may still arrive, and when the trigger runs during that window, we may see 0 rows because Spark has not yet finalized the aggregation; once the watermark passes the window end, Spark considers the window complete, aggregates all the events belonging to that window, and finalizes the result.
- I resolved this by using .option("skipChangeCommits", "true") in the Gold streaming query. It tells Spark to skip the UPDATE and DELETE change commits coming from the Silver source, so the Gold stream can focus on the append-style data. This allows the watermark to progress as expected, the window to close, and the aggregation result to be calculated and finalized, so when the trigger occurs, we get the correct aggregation result for that window.

### 3rd:
- The actual time information was lost in the Silver timestamp column because we were using `to_date()`, which keeps only the date part and removes the time portion. As a result, the existing data in `orders_CDC_silver` had timestamps like `2026-08-31 00:00:00` for all records. Since this column is used as the event-time column in the Gold window function, all events fell into one 5-minute window. Spark could not properly progress the watermark and finalize that window. So, when the trigger ran, the window was still open and the aggregation result was not available, which is why we got 0 rows.
- To solve this, I replaced to_date() with to_timestamp() and used the correct format dd/MM/yyyy HH:mm:ss. This preserves both the date and the actual time in the Silver timestamp column. After a full refresh, the Silver table contains the correct event timestamps, allowing the Gold window function and watermark to work correctly. The window can then be finalized, and when the trigger runs, the aggregation result is produced.
---

## ⚡ Performance Optimization

- Implemented incremental streaming processing to process only newly arriving records instead of reprocessing the complete dataset.
- Applied Lakeflow Expectations in the Bronze layer to filter invalid records early, reducing unnecessary downstream processing.
- Used Temporary Views for lightweight transformations without creating unnecessary intermediate tables.
- Optimized enrichment using Stream-Batch Joins, reducing state and memory overhead compared with Stream-Stream joins.
- Leveraged Delta Lake Change Data Feed (CDF) for efficient incremental change tracking and downstream processing.
- Enabled Delta Lake column mapping and schema evolution to support flexible schema changes while minimizing pipeline disruptions.
- Configured 120-day Delta retention to balance governance, auditing, and Delta Time Travel requirements.
