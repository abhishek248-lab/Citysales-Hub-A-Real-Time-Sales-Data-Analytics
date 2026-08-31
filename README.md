## Project Overview :
CITYSALES HUB is a  real-time sales data pipeline built for a regional sales involving Bhubaneswar and Khordha regions of Madhav Ecommerce. It is built on the Databricks Lakehouse Platform using modern data engineering technologies such as Lakeflow Spark Declarative Pipelines, Spark Structured Streaming, Delta Lake, and Unity Catalog. The pipeline ingests, validates, transforms, and processes sales data in real time, enabling real-time sales dashboards, regional performance tracking, inventory monitoring, and faster business reporting .It is designed to be scalable, easy to maintain and business rules to be added with minimal code changes.

## Problem Statement :

Traditional sales data processing happens through batch pipelines, where data is collected from different sources and processed in batches before being stored in a data warehouse. This makes the overall data processing a long process, as data is available in the data warehouse only after the batch processing is completed.

Because of this delay, it becomes difficult for business teams to identify the latest business performance, sales trends, and inventory updates in real time. Another challenge is that different business requirements require managing separate pipelines, which increases development time, development cost, and maintenance effort.


## Business Problem :
The business needed faster access to sales information to make quick decisions related to revenue tracking, inventory management, and sales planning. Since the data was not available in real time, teams had to depend on delayed reports, which affected decision-making and operational efficiency.

## Solution :
To solve this problem, I designed a data pipeline using the Databricks Lakehouse Platform and followed the Medallion Architecture, which consists of Bronze, Silver, and Gold layers. In the Bronze layer, I ingest the raw sales data and apply data quality validations using expectations. In the Silver layer, I clean and transform the data using SQL and Spark transformations. In the Gold layer, I perform business-level analysis using Spark SQL and prepare the data for reporting and analytics.


## Business Value :
The pipeline provides faster sales insights, improves inventory tracking, reduces manual work, and helps business teams make quicker decisions.

## Target Users :
Sales teams use the data to track sales and revenue, business teams use it for decision-making, and data analysts use it for reporting and analysis. Data engineers manage and maintain the pipeline.

## 🛠️ Tech Stack

### ☁️ Platform
- **Databricks**
- **Unity Catalog**
- **Databricks Lakehouse Platform**

### 👨‍💻 Programming & Processing
- **Python**
- **PySpark**
- **Spark SQL**
- **Apache Spark Structured Streaming**

### 🗄️ Storage & Data Lake
- **Delta Lake**
- **Delta Tables**

### 🔄 Data Engineering
- **Lakeflow Declarative Pipelines (Delta Live Tables)**
- **Streaming Tables**
- **Auto CDC**
- **Change Data Feed (CDC)**
- **Schema Evolution (`mergeSchema`)**
- **Lakeflow Expectations**
- **Column Mapping**
- **Liquid Clustering**
- **Auto Optimize**

### 🏛️ Data Architecture
- **Medallion Architecture (Bronze, Silver, Gold)**
- **Dimensional Modeling**
- **Star Schema**
- **Fact & Dimension Modeling**
- **Stream-Batch Join**
- **SCD Type 1**
- **Window Aggregation**
- **Watermarking**

### 📊 Analytics & Reporting
- **Real-Time Business KPIs**
- **Window Functions**
- **Aggregations**
- **Business Intelligence (BI)**
- **Real-Time Analytics**


### Data Pipeline Architecture :
<img width="1536" height="1024" alt="ChatGPT Image Aug 31, 2026, 01_18_52 PM" src="https://github.com/user-attachments/assets/507763f0-5a2b-4208-8158-8611ba7df6b4" />


The pipeline follows the Medallion Architecture approach with Bronze, Silver, and Gold layers. The Bronze layer stores raw incoming sales data, the Silver layer performs data cleaning, validation, and enrichment, and the Gold layer contains aggregated sales data used for revenue tracking, sales performance analysis, inventory monitoring, and reporting. The pipeline is implemented using the Databricks Lakehouse Platform, which supports both batch and real-time data processing.
  
## Features & Implementation Details:

<img width="1780" height="666" alt="Screenshot 2026-07-12 161230" src="https://github.com/user-attachments/assets/1a40d61c-4bac-4794-a71a-a11bf8adb798" />


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


## 🚀 Key Achievements

- Reduced compute overhead by **~60%** through **incremental streaming processing**, processing only newly arrived records instead of reprocessing the entire dataset.
- Improved query performance by **up to 40%** using **Liquid Clustering** and **Auto Optimize (Auto Compaction)** to optimize Delta Lake storage and query execution.
- Reduced manual pipeline maintenance by **~70%** through **Auto CDC**, **Schema Evolution (`mergeSchema`)**, and **Column Mapping**, eliminating manual schema updates and merge logic.
- Reduced downstream invalid records by **over 90%** using **Lakeflow Expectations**, enforcing automated data quality validation during data ingestion.

## 🚧 Challenges

- Designed a **synthetic streaming data generator** to simulate continuous real-time sales, customer, and product events from multiple regional sources.
- Implemented a **multi-source append pattern** to ingest streaming sales data from multiple cities while preventing duplicate processing and ensuring fault isolation.
- Managed **schema evolution** across the Medallion Architecture using `mergeSchema`, allowing upstream schema changes without pipeline modifications.
- Built an efficient **Stream-Batch Join** between streaming fact tables and dimension tables while maintaining low-latency processing.
- Implemented **Auto CDC (SCD Type 1)** to maintain the latest customer and product dimensions without manual merge logic.
- Generated **real-time business KPIs** using windowed aggregations and watermarking while handling late-arriving events.

---

## ⚡ Performance Optimization

- Implemented **incremental streaming processing** to process only newly arriving records instead of reprocessing the complete dataset.
- Applied **Lakeflow Expectations** in the Bronze layer to filter invalid records at ingestion, reducing downstream processing overhead.
- Used **Temporary Views** for lightweight preprocessing without creating unnecessary intermediate storage.
- Optimized enrichment using **Stream-Batch Joins**, reducing memory consumption compared to Stream-Stream joins.
- Applied **Liquid Clustering** on `customer_id` and `product_id` to improve query performance.
- Enabled **Auto Optimize** for automatic file compaction and optimized storage layout.
- Leveraged **Change Data Feed (CDC)** for efficient downstream change tracking.
- Configured **120-day Delta retention** to support governance, auditing, and Delta Time Travel.

---

## 🔒 Security & Governance

- Applied **Unity Catalog** permissions to secure datasets and control access.
- Enabled **Change Data Feed (CDC)** for data lineage and auditing.
- Used **Column Mapping** to safely support schema modifications, including column renames.
- Configured **120-day retention policies** to support compliance and Delta Time Travel.
- Maintained data quality using **Lakeflow Expectations** throughout the ingestion pipeline.

---

## ❌ Error Handling

- Applied **Lakeflow Expectations** to validate incoming records and automatically filter invalid data before downstream processing.
- Enforced business validation rules including:
  - `NOT NULL`
  - `amount > 0`
  - `quantity > 0`
- Used **Auto CDC** to process inserts and updates while maintaining consistent dimension tables.
- Leveraged **Delta Lake ACID Transactions** to guarantee atomic writes and prevent partial data updates.
- Supported automatic recovery by allowing failed streaming micro-batches to be safely retried.

### 🛡️ How Delta Lake Helps

- Ensures **ACID Transactions** for reliable streaming writes.
- Prevents incomplete or corrupted table updates during failures.
- Supports **Time Travel** for auditing and recovery.
- Enables safe retries without impacting previously committed data.

---

## 📈 Scalability

- Built on the **Databricks Lakehouse Platform** using Delta Lake and Lakeflow Declarative Pipelines.
- Implemented the **Medallion Architecture (Bronze → Silver → Gold)** for scalable and maintainable data processing.
- Designed the pipeline using **Streaming Tables**, enabling continuous ingestion with minimal latency.
- Used **Auto CDC** to efficiently process dimension updates without expensive merge operations.
- Supported **Schema Evolution** using `mergeSchema`, allowing the pipeline to scale with changing source schemas.
- Configured **Cluster Autoscaling** to dynamically allocate compute resources based on streaming workload.
- Designed a **multi-source append architecture**, making it easy to onboard additional regional sales streams.

---

## 📊 Monitoring

- Monitored **Lakeflow Pipeline** executions using the Databricks Pipeline UI.
- Tracked streaming job health, processing latency, and pipeline execution status.
- Monitored **Lakeflow Expectations** to identify invalid records and data quality failures.
- Used Delta table history and pipeline logs for troubleshooting and operational monitoring.
- Validated streaming throughput and micro-batch execution using Databricks monitoring tools.

