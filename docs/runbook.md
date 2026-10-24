# Runbook — UrbanPulse Operations Guide

> This document describes how to operate, monitor, and troubleshoot the UrbanPulse pipeline.
> It is the first thing to read when something goes wrong.

---

## Daily Operations Checklist

```
□ Check Airflow UI for failed DAGs (http://localhost:8080 or EC2 IP:8080)
□ Review GE Data Docs for any failed expectations
□ Check AWS Cost Explorer for budget anomalies
□ Verify S3 latest partition has data for expected date range
```

---

## Common Operations

### Run the Full Pipeline Manually

```bash
# For a specific month
make pipeline-run YEAR=2024 MONTH=03

# Or trigger via Airflow UI:
# DAG: ingest_sf_taxi → Trigger with conf: {"year": "2024", "month": "03"}
```

### Check S3 Data Freshness

```bash
# List latest partitions in raw zone
aws s3 ls s3://urbanpulse-lake-dev/raw/taxi/ --recursive | tail -20

# Count files in staging zone
aws s3 ls s3://urbanpulse-lake-dev/staging/taxi/ --recursive | wc -l
```

### Query Redshift via CLI

```bash
# Connect to Redshift (requires psql or redshift-connector)
psql -h $REDSHIFT_HOST -p 5439 -U $REDSHIFT_USER -d $REDSHIFT_DB

# Check row counts
SELECT table_schema, table_name, estimated_visible_rows
FROM svv_table_info
WHERE table_schema IN ('core', 'marts')
ORDER BY 1, 2;
```

### Re-run a Failed Airflow Task

1. Open Airflow UI → DAGs → Find the failed DAG run
2. Click on the failed task (red box)
3. Click "Clear" to reset and re-run from that task

### Force Re-ingest a Month

```bash
# This will overwrite existing S3 files
make ingest-taxi YEAR=2024 MONTH=01 FORCE=true

# Then re-run Spark
make spark-all YEAR=2024 MONTH=01
```

---

## Troubleshooting

### "No data in S3 raw zone"
**Cause**: Ingestion script failed silently  
**Fix**:
```bash
# Run ingestion with verbose logging
python -m ingestion.batch.sf_taxi_loader --year 2024 --month 01 --log-level DEBUG
# Check DataSF API is up: curl "https://data.sfgov.org/resource/yitu-d5am.json?$limit=5"
```

### "Spark job failed with OOM error"
**Cause**: Not enough executor memory for the data volume  
**Fix**:
```bash
# Increase executor memory in spark-submit
spark-submit --executor-memory 4g --driver-memory 2g spark/jobs/bronze_to_silver.py ...
# Or increase EMR instance type in terraform/modules/emr/main.tf
```

### "dbt model failed: column not found"
**Cause**: Schema drift — source changed a column name  
**Fix**:
```bash
cd dbt && dbt debug  # Test connection
cd dbt && dbt compile  # Identify the failing model
# Check data_dictionary.md for correct column names
# Update stg_ model to handle the new column name
```

### "Great Expectations checkpoint failed"
**Cause**: Data quality issue in the batch  
**Fix**:
```bash
# View the HTML report
open quality/uncommitted/data_docs/local_site/index.html
# Identify which expectations failed and their actual vs expected values
# Decide: fix the data (re-run ingestion) or update the expectation (if source changed)
```

---

## Deployment

### Deploy Infrastructure Changes
```bash
git checkout -b infra/add-new-resource
# Make changes to terraform/
make tf-plan ENV=dev    # Review changes
make tf-apply ENV=dev   # Apply to dev
# Open PR → review → merge → GitHub Actions auto-applies to prod
```

### Deploy dbt Model Changes
```bash
git checkout -b dbt/new-mart-model
# Add new model to dbt/models/marts/
cd dbt && dbt compile  # Test SQL compiles
make dbt-run           # Test in dev
make dbt-test          # Run tests
# Open PR → CI runs dbt compile → merge → GitHub Actions runs dbt build on prod
```

---

## Cost Control

| Action | Estimated Saving |
|---|---|
| Pause Redshift cluster overnight (8pm-8am) | ~\$2/day |
| Use S3 Lifecycle rules on raw zone | ~\$5/month per 100GB |
| Use EMR Spot instances for Spark | 60-70% vs on-demand |
| Delete old Spark logs from artifacts bucket | Variable |

```bash
# Pause Redshift cluster (do this when not in use)
aws redshift pause-cluster --cluster-identifier urbanpulse-cluster-dev

# Resume it
aws redshift resume-cluster --cluster-identifier urbanpulse-cluster-dev
```
