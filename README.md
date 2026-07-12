## Project Overview :
CITYSALES HUB is a  real-time sales data pipeline built for a regional sales involving Bhubaneswar and Khordha regions of Madhav Ecommerce. It is built on the Databricks Lakehouse Platform using modern data engineering technologies such as Lakeflow Spark Declarative Pipelines, Spark Structured Streaming, Delta Lake, and Unity Catalog. The pipeline ingests, validates, transforms, and processes sales data in real time, enabling real-time sales dashboards, regional performance tracking, inventory monitoring, and faster business reporting .It is designed to be scalable, easy to maintain and business rules to be added with minimal code changes.

## Problem Statement :
Traditional sales data processing relied on batch-based pipelines, where data was collected and processed at scheduled intervals. It was difficult for business teams to track the latest sales data, inventory updates, and overall sales performance because the data was not available in real time. Managing separate pipelines for different requirements also increased development time and maintenance effort.

## Business Problem :
The business needed faster access to sales information to make quick decisions related to revenue tracking, inventory management, and sales planning. Since the data was not available in real time, teams had to depend on delayed reports, which affected decision-making and operational efficiency.

## Solution :
To solve this problem, I built a real-time sales data pipeline using Databricks Lakeflow Declarative Pipeline and Apache Spark Structured Streaming. The pipeline continuously ingests sales data, applies validations and transformations, and stores the processed data in the Lakehouse, allowing business teams to track sales and inventory updates quickly.

## Business Value :
The pipeline provides faster sales insights, improves inventory tracking, reduces manual work, and helps business teams make quicker decisions.

## Target Users :
Sales teams use the data to track sales and revenue, business teams use it for decision-making, and data analysts use it for reporting and analysis. Data engineers manage and maintain the pipeline.

## Tech Stack :
* **Databricks**
* **Unity Catalog**
* **Python**
* **PySpark**
* **Spark SQL**
* **Spark Structured Streaming**
* **Delta Table**
* **Lakeflow Declarative Pipelines**
* **Dimentional Modeling**
* **Medallion Architecture (Bronze, Silver, Gold layers)**

## OLTP database / Source table:



### Data Pipeline Architecture :
<img width="1693" height="929" alt="ChatGPT Image Jul 12, 2026, 05_37_30 PM" src="https://github.com/user-attachments/assets/c02ce027-129b-492b-aa6a-ab538afdddc9" />


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


### Performance:

<img width="1776" height="755" alt="Screenshot 2026-07-12 160349" src="https://github.com/user-attachments/assets/766122d1-7cc7-4118-abf5-29e0739e8c13" />


## Challenges :
* Generating and handling continuous streaming data to mimic real-time sales events from sales, customer, and product sources.
* Building incremental data processing logic to load only new records and avoid duplicate data.
* Maintaining data consistency while moving data across Bronze, Silver, and Gold layers.
* Ensuring accurate sales insights such as revenue tracking, regional performance, and inventory updates.

## Performance Optimization :
* Used incremental processing to process only new incoming records instead of reprocessing the entire dataset.
* Applied data quality checks using Lakeflow expectations at the Bronze layer to identify invalid records early and reduce unnecessary processing.
* Used Auto CDC with SCD Type 1 and Type 2 to efficiently manage updates and maintain historical data.
* Used Delta Lake OPTIMIZE to improve table performance by compacting small files and improving query efficiency.

## Security :
* Applied permissions and data governance to protect sales data.

## Error Handling

- Applied data quality rules to identify issues like null values and invalid records before moving data to the Silver layer.
- Used Auto CDC to handle inserts and updates efficiently while maintaining data consistency in Silver tables.
- Used Delta Lake transaction support to ensure consistent data writes and prevent partial updates.

### How Delta Lake Helps

- If a write operation fails, Delta Lake prevents incomplete or corrupted table updates.
- Transactions ensure data remains consistent during pipeline execution.
- Failed processing can be safely retried without affecting existing data.

## Scalability :
* Built on the Databricks Lakehouse Platform using Delta Lake features, which provides scalable storage and supports increasing data volumes.
* Used Medallion Architecture (Bronze, Silver, and Gold) to organize and process data efficiently as the pipeline grows.
* Used Spark dynamic scaling (cluster autoscaling) to adjust compute resources based on workload and handle increasing data volumes.

## Monitoring:
* Monitored pipeline execution, data quality checks, and failures using Databricks monitoring features.
* Tracked Lakeflow pipeline runs to identify errors, performance issues, and processing status.

## Future Improvements :
* Add real-time dashboards for sales performance, revenue trends, and inventory insights.
* Implement automated alerts for pipeline failures and data quality issues.
* Improve scalability by adding more data sources and automated pipeline configurations.
