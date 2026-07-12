# 🔧 Troubleshooting Guide - CitySales Hub

## 📋 Quick Navigation
* [Pipeline Failures](#pipeline-failures)
* [Data Quality Issues](#data-quality-issues)
* [Performance Problems](#performance-problems)
* [Schema Evolution Issues](#schema-evolution-issues)
* [CDC Issues](#cdc-issues)
* [Monitoring & Debugging](#monitoring--debugging)

---

## 🚨 Pipeline Failures

### **Issue 1: "Table not found: customers_view"**

**Symptom:**
```
AnalysisException: Table or view not found: customers_view
```

**Root Cause:**
* Temporary views must be created before they're referenced
* Pipeline loading files in wrong order

**Solution:**
```
1. Check file organization:
   - Bronze files load first
   - Silver views before Silver CDC
   - Silver CDC before Gold

2. Verify file names don't have special characters

3. Restart pipeline with full refresh
```

---

### **Issue 2: "Stream-static join without watermark"**

**Symptom:**
```
AnalysisException: Stream-stream join without equality condition or watermark
```

**Root Cause:**
* Trying to use streaming read for dimension tables
* Should be batch read (`spark.read` not `spark.readStream`)

**Solution:**
```python
# ❌ WRONG - Streaming read for dimension
df_dimCust = spark.readStream.table("dim_customers")

# ✅ CORRECT - Batch read for dimension
df_dimCust = spark.read.table("dim_customers")
```

---

### **Issue 3: "Cannot start pipeline in FAILED state"**

**Symptom:**
```
Pipeline update failed to start: Previous update still in FAILED state
```

**Solution:**
```
1. Check pipeline issues tab for error details
2. Fix the underlying issue
3. Click "Start" again (no need to reset)
4. Or use "Start with full refresh" if data corruption suspected
```

---

### **Issue 4: "Concurrent pipeline runs not allowed"**

**Symptom:**
```
Cannot start update: Pipeline already running
```

**Solution:**
```
1. Wait for current run to complete
2. Or: Stop current run
3. Then start new run
4. Configure: Set continuous=false for triggered mode
```

---

## 📊 Data Quality Issues

### **Issue 5: "Too many records dropped by expectations"**

**Symptom:**
```
90% of records failing expectation checks
```

**Root Cause:**
* Source data quality degraded
* Expectation rules too strict

**Debug Steps:**
```sql
-- Check what's failing
SELECT 
  customer_name,
  region,
  CASE 
    WHEN customer_name IS NULL THEN 'name_null'
    WHEN region IS NULL THEN 'region_null'
    ELSE 'ok'
  END as failure_reason
FROM workspace.store.customers_info
LIMIT 100;
```

**Solution:**
```
1. Fix source data OR
2. Relax expectations temporarily:
   - Change expect_all_or_drop to expect_all (warn only)
   - Investigate source issue
   - Re-enable dropping after fix
```

---

### **Issue 6: "Duplicate records in dimension tables"**

**Symptom:**
```sql
SELECT customer_id, COUNT(*) 
FROM workspace.stream_data.dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;
-- Returns duplicates
```

**Root Cause:**
* CDC sequence column not set correctly
* Same timestamp for multiple records

**Solution:**
```python
# Check sequence_by column has unique ordering
dlt.create_auto_cdc_flow(
    target="dim_customers",
    source="customers_view",
    keys=["customer_id"],
    sequence_by="last_updated",  # Ensure this is updated correctly
    stored_as_scd_type=1
)

# Add milliseconds to timestamp if needed:
# .withColumn("last_updated", current_timestamp())
```

---

### **Issue 7: "Late data being dropped"**

**Symptom:**
```
Records with older timestamps not appearing in aggregations
```

**Root Cause:**
* Watermark too short for actual data latency

**Debug:**
```sql
-- Check actual data latency
SELECT 
  sale_date,
  current_timestamp() as processing_time,
  (unix_timestamp(current_timestamp()) - unix_timestamp(sale_date))/60 as latency_minutes
FROM workspace.stream_data.append_sales
ORDER BY sale_date DESC
LIMIT 100;
```

**Solution:**
```python
# Increase watermark from 1 to 2-5 minutes
.withWatermark("sale_date", "2 minutes")  # Was "1 minutes"
```

---

## ⚡ Performance Problems

### **Issue 8: "Queries taking too long"**

**Symptom:**
```
SELECT * FROM enriched_sales WHERE customer_id = 'C001'
-- Takes 30+ seconds
```

**Root Cause:**
* Too many small files
* Liquid clustering not optimized

**Solution:**
```sql
-- 1. Check file count
DESCRIBE DETAIL workspace.stream_data.enriched_sales;
-- If numFiles > 1000, optimize

-- 2. Optimize table
OPTIMIZE workspace.stream_data.enriched_sales;

-- 3. Verify clustering columns match query patterns
-- Should cluster by columns in WHERE clauses
```

---

### **Issue 9: "Pipeline running very slowly"**

**Symptom:**
```
Pipeline takes 30+ minutes to process 1000 records
```

**Debug:**
```sql
-- Check for data skew
SELECT 
  window.start,
  COUNT(*) as records_per_window
FROM workspace.stream_data.sales_metrics
GROUP BY window.start
ORDER BY records_per_window DESC
LIMIT 10;
```

**Solution:**
```
1. Increase cluster size (add workers)
2. Enable auto-scaling
3. Check for shuffle operations
4. Consider partitioning large tables
```

---

### **Issue 10: "High memory usage / OOM errors"**

**Symptom:**
```
OutOfMemoryError: Java heap space
```

**Root Cause:**
* collect_set() on high-cardinality columns
* Window aggregation state too large

**Solution:**
```python
# Replace collect_set with approx_count_distinct for large datasets
# ❌ Memory intensive
collect_set("customer_name").alias("customer_list")

# ✅ Memory efficient
approx_count_distinct("customer_name").alias("unique_customers")

# Or limit collection size
collect_set("customer_name").alias("customer_list")[:100]
```

---

## 🔄 Schema Evolution Issues

### **Issue 11: "New column not appearing downstream"**

**Symptom:**
```
Added column "email" to source
Column appears in Bronze but not in Gold
```

**Root Cause:**
* Missing `mergeSchema` option on intermediate reads
* Streaming checkpoint needs clearing

**Solution:**
```python
# Ensure ALL streaming reads have mergeSchema
spark.readStream.option("mergeSchema", "true").table("source_table")

# If still not working, force refresh:
# 1. Stop pipeline
# 2. Start with full refresh
# 3. New column should appear
```

---

### **Issue 12: "Schema mismatch error"**

**Symptom:**
```
AnalysisException: Cannot write incompatible data to table
Existing schema: [col1 INT]
New schema: [col1 STRING]
```

**Root Cause:**
* Data type change (not supported by additive evolution)

**Solution:**
```
Data type changes are NOT automatically handled.
Must manually migrate:

1. Stop pipeline
2. Drop downstream tables
3. Fix source data type
4. Restart pipeline with full refresh
```

---

## 🔁 CDC Issues

### **Issue 13: "CDC not updating records"**

**Symptom:**
```
Changed customer region in source
Dimension table still shows old value
```

**Debug:**
```sql
-- Check if updates are in source
SELECT * FROM workspace.store.customers_info 
WHERE customer_id = 'C001' 
ORDER BY last_updated DESC;

-- Check if updates reached Bronze
SELECT * FROM workspace.stream_data.customers 
WHERE customer_id = 'C001' 
ORDER BY last_updated DESC;
```

**Root Cause:**
* sequence_by column not updating
* CDC flow not processing

**Solution:**
```
1. Ensure source updates sequence_by column:
   UPDATE customers_info 
   SET region = 'NEW', last_updated = current_timestamp()

2. Verify CDC flow is running (not just dry-run)

3. Check CDC flow configuration
```

---

### **Issue 14: "Old records not being deleted"**

**Symptom:**
```
Deleted record from source
Record still in dimension table
```

**Root Cause:**
* CDC requires explicit deletes with `apply_as_deletes`
* Or source must send DELETE operations

**Solution:**
```python
# Add delete handling to CDC flow
dlt.create_auto_cdc_flow(
    target="dim_customers",
    source="customers_view",
    keys=["customer_id"],
    sequence_by="last_updated",
    stored_as_scd_type=1,
    apply_as_deletes="operation = 'DELETE'"  # If source has delete flag
)
```

---

## 📈 Monitoring & Debugging

### **Debug Technique 1: Check Pipeline Logs**
```
1. Open pipeline in Databricks UI
2. Click "Pipeline Runs" tab
3. Click on failed run
4. Check "Event Log" for detailed errors
5. Look for red error messages
```

### **Debug Technique 2: Sample Intermediate Tables**
```sql
-- Don't query temp views (will fail)
-- Sample materialized tables only:

-- Bronze
SELECT * FROM workspace.stream_data.customers LIMIT 10;

-- Silver
SELECT * FROM workspace.stream_data.dim_customers LIMIT 10;
SELECT * FROM workspace.stream_data.enriched_sales LIMIT 10;

-- Gold
SELECT * FROM workspace.stream_data.sales_metrics LIMIT 10;
```

### **Debug Technique 3: Trace Data Flow**
```sql
-- Start with specific customer_id, trace through layers

-- 1. Source
SELECT * FROM workspace.store.customers_info WHERE customer_id = 'C001';

-- 2. Bronze
SELECT * FROM workspace.stream_data.customers WHERE customer_id = 'C001';

-- 3. Silver Dimension
SELECT * FROM workspace.stream_data.dim_customers WHERE customer_id = 'C001';

-- 4. Silver Enriched
SELECT * FROM workspace.stream_data.enriched_sales WHERE customer_id = 'C001';

-- 5. Gold Metrics
SELECT * FROM workspace.stream_data.sales_metrics 
WHERE array_contains(customer_list, 'John Doe');
```

### **Debug Technique 4: Check Table History**
```sql
-- View recent changes
DESCRIBE HISTORY workspace.stream_data.dim_customers LIMIT 10;

-- Check for suspicious operations:
-- - Multiple full refreshes
-- - Frequent schema changes
-- - Failed operations
```

### **Debug Technique 5: Validate Data Counts**
```sql
-- Compare counts across layers
SELECT 'source' as layer, COUNT(*) as count FROM workspace.store.customers_info
UNION ALL
SELECT 'bronze', COUNT(*) FROM workspace.stream_data.customers
UNION ALL
SELECT 'silver', COUNT(*) FROM workspace.stream_data.dim_customers
UNION ALL
SELECT 'enriched', COUNT(*) FROM workspace.stream_data.enriched_sales;

-- Drops between layers indicate quality issues or filtering
```

---

## 🎯 Common Error Messages

| Error Message | Meaning | Quick Fix |
|---------------|---------|-----------|
| "Table not found" | File load order wrong | Reorganize files by layer |
| "Stream-stream join" | Wrong read type | Use spark.read for dimensions |
| "Cannot merge schemas" | Column mapping missing | Add table property |
| "Watermark exceeded" | Watermark too short | Increase watermark duration |
| "Concurrent modification" | Multiple writers | Stop other pipeline runs |
| "Out of memory" | State/collect too large | Use approx functions |
| "Schema incompatible" | Data type changed | Manual migration needed |

---

## 🔍 Advanced Debugging

### **Enable Verbose Logging:**
```python
# Add to notebook for detailed logs
spark.conf.set("spark.databricks.dlt.verbose", "true")
```

### **Query Streaming State:**
```sql
-- Check streaming checkpoint status
DESCRIBE EXTENDED workspace.stream_data.sales_metrics;
-- Look for checkpoint location
```

### **Test in Isolation:**
```python
# Test transformation logic outside pipeline
df_test = spark.read.table("workspace.stream_data.customers")
df_transformed = df_test.withColumn("region", upper("region"))
df_transformed.display()
```

---

## 📞 When to Escalate

Escalate to senior engineer if:
* ✅ Pipeline down > 1 hour
* ✅ Data loss detected
* ✅ Tried all troubleshooting steps
* ✅ Security or access issues
* ✅ Billing or cost concerns

---

## 📚 Related Resources

* [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) - Production setup
* [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md) - Build steps
* [VACUUM_MAINTENANCE_GUIDE.md](VACUUM_MAINTENANCE_GUIDE.md) - Storage issues
* [Databricks Documentation](https://docs.databricks.com/workflows/delta-live-tables/index.html)

---

**Remember:** Most issues are resolved by:
1. Checking logs for error details
2. Verifying file load order
3. Ensuring correct read types (streaming vs batch)
4. Running full refresh when in doubt

**Happy Debugging!** 🐛🔧
