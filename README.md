## Project Overview :
My project name is City Sales Hub a real-time sales analytics service built for **Madhav E-commerce** to analyze regional sales in real time across the Bhubaneswar and Khordha regions.It is built on the Databricks Lakehouse Platform using modern data engineering technologies such as Lakeflow Spark Declarative Pipelines, Spark Structured Streaming, Delta Lake and Pyspark and python. The pipeline ingests, validates, transforms, and processes sales data in real time, enabling real-time sales dashboards, Sales performance tracking, and faster business reporting.

## Problem Statement :

Traditional sales data processing happens through batch pipelines, where data is collected from different sources and processed in batches before being stored in a data warehouse. This makes the overall data processing a long process, as data is available in the data warehouse only after the batch processing is completed.

Because of this delay, it becomes difficult for business teams to identify the latest business performance, sales trends . Another challenge is that different business requirements require managing separate pipelines, which increases development time, development cost, and maintenance effort.


## Solution :
To solve this problem, I designed a data pipeline using the Databricks Lakehouse Platform and followed the Medallion Architecture, which consists of Bronze, Silver, and Gold layers. In the Bronze layer, I ingest the raw sales data and apply data quality validations using expectations. In the Silver layer, I clean and transform using Spark Structured Streaming.. In the Gold layer, I perform business-level analysis using Spark SQL and prepare the data for reporting and analytics.


# Tech Stack

- Databricks Lakehouse Platform
- Python
- PySpark
- Apache Spark Structured Streaming
- Delta Lake
- Lakeflow Declarative Pipelines
- Streaming Tables
- Auto CDC
- Delta Change Data Feed (CDF)
- Schema Evolution
- Lakeflow Expectations
- Column Mapping
- Auto Optimize
- Medallion Architecture (Bronze, Silver, Gold)
- SCD Type 1 & Type 2
- Window Aggregations
- Watermarking
- Stream-Batch Joins

### Data Pipeline Architecture :
<img width="1536" height="1024" alt="ChatGPT Image Aug 31, 2026, 01_18_52 PM" src="https://github.com/user-attachments/assets/507763f0-5a2b-4208-8158-8611ba7df6b4" />

## Implementation Details:
<img width="1417" height="637" alt="Screenshot 2026-09-02 174605" src="https://github.com/user-attachments/assets/df3baa15-e160-4430-b2a8-03f5d7073205" />


## Synthetic Data Generator

- Developed a **continuous synthetic streaming data generator** to simulate real-time business transactions for **Customers, Products, and Sales** across multiple regions (**Bhubaneswar** and **Khordha**).
- Defined **explicit PySpark schemas** for Customers, Products, and Orders to ensure consistent structure across continuously generated source data.
- Generated **incremental batches of 5–20 records every 60 seconds** and wrote them as CSV files to **Unity Catalog Volumes**, simulating live streaming source feeds for Bronze-layer Auto Loader ingestion.
- Implemented **multi-source regional sales feeds** with separate order ID ranges for Bhubaneswar and Khordha, supporting independent ingestion and append flows.
- Deliberately introduced **data-quality issues**, including null values and negative prices, to validate downstream **Lakeflow Expectations** and Bronze-layer data-quality rules.
- Generated **multiple date formats** (`yyyy-MM-dd`, `yyyy/MM/dd`, `dd/MM/yyyy`) to validate robust date parsing and transformation logic in the Silver layer.
- Designed the generator to produce **continuous incremental data**, enabling validation of **Auto Loader ingestion, streaming transformations, schema handling, and incremental processing**.
- Used existing Customer and Product IDs to generate realistic Order relationships and simulate **cross-source transactional data**.

---

# Pipeline Layer Summary

## **Bronze Layer** (Raw Data Ingestion)

- The Bronze layer ingests raw CSV data from Unity Catalog Volumes using Auto Loader.
- The `customers_bronze` table ingests customer data and applies data quality checks.
- The `product_bronze` table ingests product catalog data and validates product information.
- The `orders_bronze` table combines orders from Bhubaneswar and Khordha using append flows.
- Data quality rules are applied to remove invalid records.
- Change Data Feed (CDF) is enabled on Bronze tables for downstream CDC processing.

## **Silver Layer** (Cleansed & Transformed)

- The Silver layer cleans and standardizes data received from the Bronze layer.
- Duplicate records are removed and unnecessary whitespace is trimmed.
- Customer names are standardized, and multiple date formats are converted into a consistent format.
- Lakeflow Auto CDC processes inserts, updates, and deletes from the transformed data.
- The `customers_CDC_silver` table maintains customer data using **SCD Type 1**.
- The `products_CDC_silver` table maintains product data using **SCD Type 2** to track price history.
- The `orders_CDC_silver` table maintains order data using **SCD Type 1**.

## **Gold Layer** (Business Metrics)

- The Gold layer provides aggregated data for business reporting and analytics.
- The `gold_customer_behavior_stream` table provides real-time customer behavior metrics using 5-minute windows.
- The `gold_product_performance_stream` table provides real-time product sales metrics using 5-minute windows.
- The `gold_payment_analysis_mv` materialized view provides payment method and transaction analysis.
- The `gold_price_demand_analysis` materialized view analyzes the relationship between product prices and demand.
- The pipeline follows the **Medallion Architecture: Bronze → Silver → Gold**.


### Output:
<img width="1787" height="767" alt="Screenshot 2026-09-02 171324" src="https://github.com/user-attachments/assets/611a147a-aeb1-4263-b6b1-05403eb1a2be" />
<img width="1793" height="812" alt="Screenshot 2026-09-02 171247" src="https://github.com/user-attachments/assets/d62f2da0-81c9-483d-8de0-28e189bdfa0f" />



## Challenges And Solved

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

##  Performance Optimization

- Applied Lakeflow Expectations in the Bronze layer to filter invalid records early, reducing unnecessary downstream processing.
- Used Temporary Views for lightweight transformations without creating unnecessary intermediate tables.
- Optimized enrichment using Stream-Batch Joins, reducing state and memory overhead compared with Stream-Stream joins.
- Leveraged Delta Lake Change Data Feed (CDF) for efficient incremental change tracking and downstream processing.
- Enabled Delta Lake column mapping and schema evolution to support flexible schema changes while minimizing pipeline disruptions.
- Configured 120-day Delta retention to balance governance, auditing, and Delta Time Travel requirements.
