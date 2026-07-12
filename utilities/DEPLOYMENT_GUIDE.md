# 🚀 Deployment Guide - CitySales Hub

## 📋 Quick Links
* [Prerequisites](#prerequisites)
* [Environment Setup](#environment-setup)
* [Pipeline Configuration](#pipeline-configuration)
* [Production Checklist](#production-checklist)
* [Monitoring & Alerts](#monitoring--alerts)

---

## ✅ Prerequisites

### **Access Requirements:**
```
✅ Databricks workspace admin or contributor role
✅ Unity Catalog CREATE SCHEMA privileges
✅ Compute cluster creation permissions
✅ Source table READ access
```

### **Recommended Compute:**
```
Production:
  • Serverless compute (recommended)
  • Or: 2-node cluster, i3.xlarge (AWS) / Standard_D4s_v3 (Azure)
  • DBR 14.3 LTS or higher
  • Auto-scaling: 2-8 workers

Development:
  • Single node cluster
  • DBR 14.3+
  • Smaller instance type OK
```

---

## 🏗️ Environment Setup

### **1. Create Catalogs & Schemas**
```sql
-- Development Environment
CREATE CATALOG IF NOT EXISTS workspace_dev;
CREATE SCHEMA IF NOT EXISTS workspace_dev.stream_data_dev;

-- Staging Environment
CREATE CATALOG IF NOT EXISTS workspace_staging;
CREATE SCHEMA IF NOT EXISTS workspace_staging.stream_data_staging;

-- Production Environment
CREATE CATALOG IF NOT EXISTS workspace;
CREATE SCHEMA IF NOT EXISTS workspace.stream_data;
```

### **2. Grant Permissions**
```sql
-- Grant pipeline service account access
GRANT USE CATALOG ON CATALOG workspace TO SERVICE_PRINCIPAL `pipeline-sa`;
GRANT USE SCHEMA ON SCHEMA workspace.stream_data TO SERVICE_PRINCIPAL `pipeline-sa`;
GRANT CREATE TABLE ON SCHEMA workspace.stream_data TO SERVICE_PRINCIPAL `pipeline-sa`;
GRANT SELECT ON SCHEMA workspace.stream_data TO `data-analysts`;
```

---

## ⚙️ Pipeline Configuration

### **Development Pipeline Settings**
```json
{
  "name": "CitySales-Hub-DEV",
  "catalog": "workspace_dev",
  "target": "stream_data_dev",
  "continuous": false,
  "development": true,
  "libraries": [
    {
      "file": {
        "path": "/Workspace/Users/<your-email>/Citysales-Hub-A-Real-Time-Sales-Data-Service/transformations"
      }
    }
  ],
  "clusters": [
    {
      "label": "default",
      "num_workers": 1
    }
  ]
}
```

### **Production Pipeline Settings**
```json
{
  "name": "CitySales-Hub-PROD",
  "catalog": "workspace",
  "target": "stream_data",
  "continuous": false,
  "development": false,
  "edition": "ADVANCED",
  "serverless": true,
  "libraries": [
    {
      "file": {
        "path": "/Workspace/Users/<your-email>/Citysales-Hub-A-Real-Time-Sales-Data-Service/transformations"
      }
    }
  ],
  "notifications": [
    {
      "email_recipients": ["team@company.com"],
      "alerts": ["on-update-failure", "on-flow-failure"]
    }
  ]
}
```

---

## 📝 Production Checklist

### **Pre-Deployment (Day -7 to -1):**
```
Week Before:
  ✅ Run pipeline in DEV for 7 days
  ✅ Monitor for failures
  ✅ Test schema evolution
  ✅ Test CDC updates
  ✅ Verify late data handling
  ✅ Check storage growth rate
  ✅ Run performance benchmarks
```

### **Deployment Day:**
```
Morning:
  ✅ Create production pipeline
  ✅ Configure notifications
  ✅ Run dry-run validation
  ✅ Start first full refresh
  ✅ Monitor first run completion

Afternoon:
  ✅ Verify all tables created
  ✅ Validate data quality
  ✅ Check row counts vs source
  ✅ Test query performance
  ✅ Create monitoring dashboard
```

### **Post-Deployment (Week 1):**
```
Daily:
  ✅ Check pipeline run status
  ✅ Monitor data latency
  ✅ Review expectation violations
  ✅ Check storage usage
  
Weekly:
  ✅ Review performance metrics
  ✅ Tune watermark if needed
  ✅ Update documentation
  ✅ Gather user feedback
```

---

## 📊 Monitoring & Alerts

### **Key Metrics to Monitor:**

#### **1. Pipeline Health**
```sql
-- Pipeline run status
SELECT 
  update_id,
  state,
  creation_time,
  end_time,
  cause
FROM system.event_log.pipeline_updates
WHERE pipeline_id = '<your-pipeline-id>'
ORDER BY creation_time DESC
LIMIT 10;
```

#### **2. Data Freshness**
```sql
-- Check latest data timestamp
SELECT 
  MAX(window.end) as latest_window,
  current_timestamp() as now,
  (unix_timestamp(current_timestamp()) - unix_timestamp(MAX(window.end)))/60 as minutes_behind
FROM workspace.stream_data.sales_metrics;

-- Alert if > 5 minutes behind
```

#### **3. Data Quality**
```sql
-- Check expectation violations
SELECT 
  dataset,
  expectation,
  passed_records,
  failed_records,
  failed_records * 100.0 / (passed_records + failed_records) as failure_rate
FROM system.event_log.expectations
WHERE pipeline_id = '<your-pipeline-id>'
ORDER BY creation_time DESC;

-- Alert if failure_rate > 5%
```

#### **4. Storage Usage**
```sql
-- Check table sizes
SELECT 
  table_name,
  sizeInBytes / 1024 / 1024 / 1024 as size_gb,
  numFiles
FROM (
  DESCRIBE DETAIL workspace.stream_data.customers
  UNION ALL
  DESCRIBE DETAIL workspace.stream_data.products
  UNION ALL
  DESCRIBE DETAIL workspace.stream_data.append_sales
  UNION ALL
  DESCRIBE DETAIL workspace.stream_data.dim_customers
  UNION ALL
  DESCRIBE DETAIL workspace.stream_data.dim_products
  UNION ALL
  DESCRIBE DETAIL workspace.stream_data.fact_sales
  UNION ALL
  DESCRIBE DETAIL workspace.stream_data.enriched_sales
  UNION ALL
  DESCRIBE DETAIL workspace.stream_data.sales_metrics
);
```

---

### **Alerting Rules:**

| Metric | Threshold | Action |
|--------|-----------|--------|
| Pipeline failure | Any | Immediate alert |
| Data latency | > 5 minutes | Alert + investigate |
| Expectation failures | > 5% | Alert + check source data |
| Storage growth | > 100GB/day | Review + consider cleanup |
| Query time | > 10 seconds | Optimize queries |

---

## 🔄 CI/CD Pipeline (Optional)

### **Using Databricks Asset Bundles:**

**1. Project Structure:**
```
citysales-hub/
├── databricks.yml
├── resources/
│   └── citysales_pipeline.yml
└── src/
    └── transformations/
        ├── Bronzelayer_StagingLayer/
        ├── 2_silver layer/
        └── 3_gold layer/
```

**2. databricks.yml:**
```yaml
bundle:
  name: citysales-hub

environments:
  dev:
    default: true
    workspace:
      host: https://your-workspace.cloud.databricks.com
  
  prod:
    workspace:
      host: https://your-workspace.cloud.databricks.com
    
resources:
  pipelines:
    citysales_pipeline:
      name: CitySales-Hub-${bundle.environment}
      catalog: workspace_${bundle.environment}
      target: stream_data_${bundle.environment}
      libraries:
        - file:
            path: ./src/transformations
```

**3. Deploy Command:**
```bash
# Deploy to dev
databricks bundle deploy --environment dev

# Deploy to prod
databricks bundle deploy --environment prod

# Run pipeline
databricks bundle run citysales_pipeline --environment prod
```

---

## 🔐 Security Best Practices

### **1. Access Control:**
```sql
-- Least privilege principle
GRANT SELECT ON SCHEMA workspace.stream_data TO `analysts`;
GRANT MODIFY ON SCHEMA workspace.stream_data TO `pipeline-sa`;
DENY SELECT ON TABLE workspace.stream_data.customers TO `external-users`;
```

### **2. Data Masking (Optional):**
```sql
-- Mask sensitive columns
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
RETURN CONCAT(SUBSTRING(email, 1, 2), '***@', SPLIT(email, '@')[1]);

-- Apply to view
CREATE VIEW workspace.stream_data.customers_masked AS
SELECT 
  customer_id,
  customer_name,
  mask_email(email) as email,
  region
FROM workspace.stream_data.dim_customers;
```

### **3. Audit Logging:**
```sql
-- Enable audit logs
ALTER SCHEMA workspace.stream_data SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- Query audit trail
SELECT * FROM table_changes('workspace.stream_data.dim_customers', 0)
ORDER BY _commit_timestamp DESC;
```

---

## 🎯 Performance Tuning

### **1. Cluster Configuration:**
```python
# Optimize for streaming workloads
spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
               "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
spark.conf.set("spark.sql.streaming.minBatchesToRetain", 5)
```

### **2. Liquid Clustering Maintenance:**
```sql
-- Optimize tables quarterly
OPTIMIZE workspace.stream_data.enriched_sales;
OPTIMIZE workspace.stream_data.sales_metrics;
```

### **3. Z-Order (Alternative to Liquid Clustering):**
```sql
-- If not using liquid clustering
OPTIMIZE workspace.stream_data.enriched_sales
ZORDER BY (customer_id, product_id);
```

---

## 📚 Rollback Procedures

### **Emergency Rollback:**
```
1. Stop production pipeline
2. Restore from snapshot:
   - Use Delta time travel
   - Or restore from backup
3. Verify data integrity
4. Resume pipeline
```

**Example Time Travel:**
```sql
-- Restore table to 2 hours ago
CREATE OR REPLACE TABLE workspace.stream_data.sales_metrics AS
SELECT * FROM workspace.stream_data.sales_metrics 
TIMESTAMP AS OF date_sub(current_timestamp(), INTERVAL 2 HOURS);
```

---

## 🔧 Maintenance Schedule

| Task | Frequency | Duration |
|------|-----------|----------|
| Pipeline monitoring | Daily | 5 min |
| Storage cleanup | Weekly | 30 min |
| Performance review | Monthly | 2 hours |
| Schema optimization | Quarterly | 1 day |
| Disaster recovery test | Annually | 1 day |

---

## 📞 Support Contacts

```
Pipeline Owners: Data Engineering Team
Slack Channel: #citysales-pipeline
On-Call Rotation: PagerDuty schedule
Escalation: Senior Data Architect
```

---

## 📖 Related Documentation

* [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md) - Project summary
* [ARCHITECTURE_GUIDE.md](ARCHITECTURE_GUIDE.md) - Technical architecture
* [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md) - Build instructions
* [TROUBLESHOOTING_GUIDE.md](TROUBLESHOOTING_GUIDE.md) - Common issues
* [VACUUM_MAINTENANCE_GUIDE.md](VACUUM_MAINTENANCE_GUIDE.md) - Storage cleanup

---

**Deployment Status Checklist:**

```
✅ Pipeline deployed to production
✅ Monitoring dashboard created
✅ Alerts configured
✅ Documentation updated
✅ Team trained
✅ Runbook created
✅ DR procedures tested
```

**Ready for Production!** 🚀
