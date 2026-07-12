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


The pipeline follows the Medallion Architecture approach with Bronze, Silver, and Gold layers. The Bronze layer stores raw incoming sales data, the Silver layer performs data cleaning, validation, and enrichment, and the Gold layer contains aggregated sales data used for revenue tracking, sales performance analysis, inventory monitoring, and reporting. The pipeline is implemented using the Databricks Lakehouse Platform, which supports both batch and real-time data processing.
  
## Features & Implementation Details:

<img width="1780" height="666" alt="Screenshot 2026-07-12 161230" src="https://github.com/user-attachments/assets/1a40d61c-4bac-4794-a71a-a11bf8adb798" />


### Incremental Data Ingestion:
* Generated streaming data using a loop for sales (Bhubaneswar and Khordha), customer, and product records to act as streaming sources.
* Created Delta tables with schema and initial data during the first run, then processed only new incoming records in the following runs.
* Improved data loading by processing only new records and avoiding duplicates.

### Bronze Layer (Data Staging) :
* Ingests streaming sales data from Bhubaneswar and Khordha regions into the append_sales streaming table, along with customer and product data from streaming sources.
* Uses Lakeflow expectations to apply data quality checks such as null validation and basic data cleansing.
* Stores raw data reliably in the Bronze layer for further processing in the Silver layer.

### Silver Layer (Cleaned & Enriched Data)

* Applies advanced transformations such as column standardization and filtering based on business rules.
* Implements Slowly Changing Dimensions (SCD) Type 1 and Type 2 using Auto CDC for customer and product dimension history tracking.

### Gold Layer (Aggregated Analytics)

* Generates unique regional sales IDs using ROW_NUMBER() for tracking sales records.
* Joins fact and dimension tables to create enriched sales datasets with customer, product, and region details.
* Builds real-time aggregate views for business reporting, such as revenue tracking, regional sales performance, product-wise sales trends, and inventory analysis.

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
