# Architecture Deep Dive — UrbanPulse

> This document explains **every architectural decision** in the UrbanPulse platform.
> Read this before touching any code. Update it when you change anything significant.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Data Flow — End to End](#2-data-flow--end-to-end)
3. [Medallion Architecture (Bronze / Silver / Gold)](#3-medallion-architecture)
4. [Storage Layer — Amazon S3](#4-storage-layer--amazon-s3)
5. [Ingestion Layer](#5-ingestion-layer)
6. [Processing Layer — PySpark on EMR](#6-processing-layer--pyspark-on-emr)
7. [Orchestration — Apache Airflow](#7-orchestration--apache-airflow)
8. [Data Warehouse — Amazon Redshift](#8-data-warehouse--amazon-redshift)
9. [Transformation — dbt Core](#9-transformation--dbt-core)
10. [Data Quality — Great Expectations](#10-data-quality--great-expectations)
11. [Streaming — Kafka MSK + Spark Structured Streaming](#11-streaming--kafka-msk--spark-structured-streaming)
12. [CI/CD — GitHub Actions](#12-cicd--github-actions)
13. [Security & Networking](#13-security--networking)
14. [Monitoring & Alerting](#14-monitoring--alerting)
15. [Local Development Environment](#15-local-development-environment)

---

## 1. System Overview

UrbanPulse is built around two complementary pipelines that run side-by-side:

### Batch Pipeline (Monthly)
Processes historical SF taxi trip data from the DataSF open data portal. This is the **primary analytical pipeline** — it produces the clean, enriched datasets that power our dashboards and business reports.

```
DataSF API (CSV) ──▶ Python Loader ──▶ S3 Raw ──▶ PySpark EMR ──▶ S3 Staging ──▶ S3 Curated
Open-Meteo API   ──▶ Python Loader ──▶ S3 Raw ──┘                                      │
                                                                               Redshift COPY
                                                                                      │
                                                                               dbt models
                                                                                      │
                                                                              Metabase dashboards
```

### Streaming Pipeline (Continuous)
Simulates real-time taxi events using a Python Kafka producer. Spark Structured Streaming consumes events and writes micro-batches to S3. This pipeline is used for **live dashboarding** and demonstrates streaming architecture patterns.

```
Python Event Producer ──▶ Kafka MSK ──▶ Spark Structured Streaming ──▶ S3 Curated (micro-batch)
```

---

## 2. Data Flow — End to End

```
┌─────────────────────────────────────────────────────────────────┐
│ STEP 1: INGESTION                                               │
│                                                                 │
│  DataSF Taxi API                                                │
│  GET /resource/yitu-d5am.json?year=2024&month=01                │
│  → pandas DataFrame                                             │
│  → PyArrow → Parquet                                            │
│  → boto3.upload → s3://urbanpulse-lake/raw/taxi/year=2024/      │
│                              month=01/trips.parquet             │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│ STEP 2: BATCH PROCESSING (PySpark on EMR)                       │
│                                                                 │
│  Bronze → Silver:                                               │
│    read_parquet(s3://raw/taxi/...)                               │
│    → enforce schema (StructType)                                │
│    → cast types (string → timestamp, string → decimal)          │
│    → drop duplicates on trip_id                                 │
│    → filter invalid rows (fare < 0, distance <= 0)              │
│    → join with SF neighborhoods GeoJSON (spatial lookup)        │
│    → write_parquet(s3://staging/taxi/..., partitionBy=date)      │
│                                                                 │
│  Silver → Gold:                                                 │
│    read_parquet(s3://staging/taxi/...)                           │
│    → join with weather (nearest hourly observation)             │
│    → calculate derived metrics (speed, fare_per_km, tip_pct)   │
│    → write_parquet(s3://curated/taxi/..., partitionBy=date)      │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│ STEP 3: DATA WAREHOUSE LOAD (Redshift COPY)                     │
│                                                                 │
│  COPY landing.taxi_trips                                        │
│  FROM 's3://urbanpulse-lake/curated/taxi/'                      │
│  IAM_ROLE 'arn:aws:iam::xxxx:role/urbanpulse-redshift'          │
│  FORMAT PARQUET;                                                │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│ STEP 4: TRANSFORMATION (dbt Core)                               │
│                                                                 │
│  landing.taxi_trips                                             │
│    → stg_taxi_trips (rename, type cast)                         │
│    → dim_location   (distinct pickup/dropoff locations)         │
│    → dim_date       (date dimension with attributes)            │
│    → fact_trips     (one row per trip, FK references)           │
│    → mart_daily_ops (aggregated daily metrics)                  │
│    → mart_zone_perf (zone-level performance metrics)            │
│    → mart_revenue   (revenue breakdown)                         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Medallion Architecture

We use the industry-standard **Bronze / Silver / Gold** (also called Raw / Staging / Curated) pattern.

| Zone | S3 Prefix | Format | What it contains | Who writes it | Who reads it |
|---|---|---|---|---|---|
| **Bronze (Raw)** | `raw/` | Parquet | Exact copy of source data, no changes | Python ingestion scripts | PySpark jobs |
| **Silver (Staging)** | `staging/` | Parquet (partitioned) | Cleaned, typed, deduplicated, enriched | PySpark bronze→silver job | PySpark silver→gold job + Redshift |
| **Gold (Curated)** | `curated/` | Parquet (partitioned) | Aggregated, business-ready, metrics computed | PySpark silver→gold job | Redshift COPY, Athena |

### Why Parquet Everywhere?
- **Columnar storage** — analytics reads specific columns, not full rows → 10x faster
- **Compression** — Parquet + Snappy codec reduces file sizes by ~75% vs CSV
- **Schema embedded** — data types enforced at the file level, no guessing
- **S3 + Spark native** — zero-copy reads between S3 and EMR/Glue

### S3 Partition Strategy
Files are partitioned by `year=/month=` at the folder level:
```
s3://urbanpulse-lake-dev/
├── raw/
│   ├── taxi/
│   │   ├── year=2022/month=01/trips.parquet
│   │   ├── year=2022/month=02/trips.parquet
│   │   └── ...
│   └── weather/
│       ├── date=2022-01-01/weather.parquet
│       └── ...
├── staging/
│   └── taxi/
│       └── year=2022/month=01/part-00000.parquet
└── curated/
    └── taxi/
        └── year=2022/month=01/part-00000.parquet
```
Partitioning means Spark and Redshift Spectrum can **skip irrelevant files** — reading only the partition you query.

---

## 4. Storage Layer — Amazon S3

### Two Buckets
| Bucket | Purpose |
|---|---|
| `urbanpulse-lake-{env}` | All data (raw/staging/curated) |
| `urbanpulse-artifacts-{env}` | Airflow logs, dbt docs, Spark JARs, GE reports |

### Lifecycle Rules (Cost Optimisation)
- Raw zone data → **S3 Infrequent Access** after 30 days (60% cheaper)
- Raw zone data → **S3 Glacier** after 90 days (80% cheaper)
- Staging zone → Infrequent Access after 60 days
- Curated zone → no tiering (actively queried)

### Versioning
S3 versioning is **enabled** on the lake bucket. This protects against:
- Accidental overwrites during re-processing
- Auditing who changed what and when

---

## 5. Ingestion Layer

### Batch Ingestion — DataSF Taxi API
**File**: `ingestion/batch/sf_taxi_loader.py`

```
DataSF Socrata API
  ↓ paginated GET requests (2000 rows/page)
  ↓ pandas.concat → single DataFrame
  ↓ minimal cleaning (rename columns to snake_case)
  ↓ pandas → PyArrow Table
  ↓ pyarrow.parquet.write_table → in-memory buffer
  ↓ boto3.s3.put_object → s3://raw/taxi/year=YYYY/month=MM/trips.parquet
```

**Design Decisions**:
- **PyArrow over CSV**: Write Parquet directly, never store CSV in the lake
- **Pagination**: DataSF limits 50k rows/request → loop until no more pages
- **Idempotent**: If the S3 file already exists, skip (use `--force` to override)
- **Retry logic**: `tenacity` library retries failed API calls up to 3 times with exponential backoff

### Batch Ingestion — Open-Meteo Weather API
**File**: `ingestion/batch/weather_ingester.py`

Open-Meteo provides free historical hourly weather data. We fetch:
- `temperature_2m` — temperature in Celsius at 2m height
- `precipitation` — hourly rainfall in mm
- `windspeed_10m` — wind speed at 10m in km/h
- `visibility` — visibility in metres
- `weathercode` — WMO weather code (0=clear, 61=rain, 71=snow etc.)

### Streaming — Kafka Event Producer
**File**: `ingestion/streaming/event_producer.py`

Generates synthetic taxi trip events mimicking real telemetry:
- Randomizes trip start time, pickup/dropoff location, duration, fare
- Produces one event per 100ms (configurable)
- Sends JSON messages to Kafka topic `sf-taxi-events`

---

## 6. Processing Layer — PySpark on EMR

### Why EMR (not Glue)?
- **EMR**: Full control over Spark version, cluster size, instance types. Better for complex jobs.
- **Glue**: Serverless, less configuration. Better for simple ETL with Glue Catalog.
- **Decision**: Primary jobs use EMR. Glue used only for Glue Data Catalog metadata updates.

### Job: Bronze → Silver
**File**: `spark/jobs/bronze_to_silver.py`

Transformations applied:
1. **Schema enforcement** — apply `StructType` schema, reject rows that don't conform
2. **Type casting** — all string columns cast to correct types (timestamp, decimal, integer)
3. **Deduplication** — drop rows where `trip_id` appears more than once
4. **Null handling** — drop rows where `pickup_datetime`, `dropoff_datetime`, or `fare_amount` is null
5. **Range validation** — filter out trips where `fare_amount < 0` or `trip_distance <= 0`
6. **Spatial enrichment** — broadcast join with SF neighborhoods GeoJSON to add `neighborhood_name` and `district`
7. **Derived fields** — `trip_duration_minutes = (dropoff_dt - pickup_dt).seconds / 60`

### Job: Silver → Gold
**File**: `spark/jobs/silver_to_gold.py`

Transformations applied:
1. **Weather join** — join each trip with the nearest hourly weather observation
2. **Efficiency metrics** — `speed_kmh = distance / duration`, `fare_per_km = fare / distance`
3. **Tip percentage** — `tip_pct = tip_amount / fare_amount * 100`
4. **Partitioned write** — write to `curated/` partitioned by `year` and `month`

---

## 7. Orchestration — Apache Airflow

### DAG Design Principles
- **One DAG per domain** — separates ingestion from processing from quality from transformation
- **Idempotent tasks** — every task can be re-run safely without side effects
- **Parameterized** — DAGs accept `year` and `month` as `dag_run.conf` parameters
- **Sensor-based dependencies** — downstream DAGs wait for upstream S3 files before starting

### DAG Dependency Chain
```
ingest_sf_taxi ──▶ spark_batch ──▶ quality_check ──▶ run_dbt
ingest_weather ──┘
```

| DAG | Schedule | Trigger |
|---|---|---|
| `ingest_sf_taxi` | `@monthly` | Downloads SF taxi CSV for previous month |
| `ingest_weather` | `@daily` | Downloads yesterday's weather |
| `spark_batch` | Triggered by `ingest_sf_taxi` success | Runs bronze→silver→gold |
| `quality_check` | Triggered by `spark_batch` success | Runs GE checkpoints |
| `run_dbt` | Triggered by `quality_check` success | Runs dbt build + test |

---

## 8. Data Warehouse — Amazon Redshift

### Schema Design
We use **three schemas** to separate concerns:

| Schema | Purpose | Who writes | Who reads |
|---|---|---|---|
| `landing` | Raw COPY from S3, minimal transformation | Airflow COPY task | dbt staging models |
| `core` | Star schema facts and dimensions | dbt | dbt marts, BI tools |
| `marts` | Pre-aggregated business tables | dbt | Metabase, QuickSight |

### Why Redshift (not Snowflake or BigQuery)?
- **Native AWS integration** — COPY from S3 is extremely fast (parallelised across slices)
- **Columnar MPP** — handles analytical queries over billions of rows efficiently
- **Cost predictable** — reserved instances give 70% savings vs on-demand
- **dbt native** — first-class dbt adapter, full feature support

### Distribution & Sort Keys
Key design decisions for query performance:
- `fact_trips`: `DISTKEY(pickup_date)`, `SORTKEY(pickup_datetime)` — most queries filter by date
- `dim_location`: `DISTSTYLE ALL` — small table, broadcast to all nodes
- `dim_date`: `DISTSTYLE ALL` — small table, broadcast to all nodes

---

## 9. Transformation — dbt Core

### Model Layers

```
landing schema (Redshift raw COPY)
    │
    ▼
staging/ (stg_*)     ← rename, cast, simple filters. 1:1 with source tables
    │
    ▼
core/ (dim_*, fact_*)  ← star schema. joins across staging models
    │
    ▼
marts/ (mart_*)      ← pre-aggregated, business-facing. powers dashboards
```

### Naming Conventions
| Layer | Prefix | Example |
|---|---|---|
| Staging | `stg_` | `stg_taxi_trips` |
| Dimension | `dim_` | `dim_location` |
| Fact | `fact_` | `fact_trips` |
| Mart | `mart_` | `mart_daily_ops` |

### Testing Strategy
- **Schema tests**: `not_null`, `unique`, `accepted_values`, `relationships` on all key columns
- **Custom tests**: `test_fare_positive`, `test_duration_reasonable` (0–480 min)
- **Freshness checks**: Assert that source data is never more than 35 days stale

---

## 10. Data Quality — Great Expectations

### Three Checkpoints
| Checkpoint | Runs After | Checks |
|---|---|---|
| `bronze_checkpoint` | Python ingestion | File exists, non-empty, expected columns present, date range valid |
| `silver_checkpoint` | Spark bronze→silver | No nulls in PK columns, fare > 0, distance > 0, timestamps in range |
| `gold_checkpoint` | Spark silver→gold | Row counts match expected ranges, all required partitions present |

### Failure Handling
- GE validation failure → raises exception → Airflow task marked FAILED
- Airflow sends Slack alert with link to GE Data Docs HTML report
- Pipeline halts — dbt does NOT run on bad data

---

## 11. Streaming — Kafka MSK + Spark Structured Streaming

### Topic Design
| Topic | Producer | Consumer | Retention |
|---|---|---|---|
| `sf-taxi-events` | Python Event Producer | Spark Structured Streaming | 7 days |
| `sf-weather-events` | Python Weather Ingester | Spark Structured Streaming | 7 days |

### Spark Streaming Design
- **Trigger**: `processingTime='1 minute'` — micro-batch every 60 seconds
- **Checkpoint**: S3-backed checkpoint for exactly-once guarantees
- **Output**: Append mode to `s3://curated/streaming/taxi/` partitioned by `event_hour`
- **Watermark**: 10-minute watermark for late-arriving events

---

## 12. CI/CD — GitHub Actions

### PR Workflow (`.github/workflows/pr_checks.yml`)
Runs on every pull request. Blocks merge if any check fails.

```
┌─────────────────────────────────────────┐
│  PR opened / updated                    │
│      │                                  │
│      ├─ terraform fmt --check           │
│      ├─ terraform validate              │
│      ├─ ruff check . (Python lint)      │
│      ├─ pytest (unit tests)             │
│      └─ dbt compile (SQL syntax check)  │
└─────────────────────────────────────────┘
```

### Deploy Workflow (`.github/workflows/deploy_infra.yml`)
Runs on merge to `main`. Deploys to dev environment automatically.

```
merge to main
    │
    ├─ terraform apply (dev)
    └─ dbt build (dev Redshift)
```

---

## 13. Security & Networking

### VPC Design
```
VPC: 10.0.0.0/16
  ├── Public Subnets (10.0.1.0/24, 10.0.2.0/24)   ← Airflow EC2, NAT Gateway
  └── Private Subnets (10.0.10.0/24, 10.0.11.0/24) ← Redshift, MSK, EMR
```

### Principle of Least Privilege
- **Redshift role**: Read-only S3 access scoped to the lake bucket only
- **EMR role**: Read from staging, write to curated, no Redshift access
- **Airflow role**: Trigger EMR steps, read S3 outputs, no direct DB access
- **Glue role**: Read from staging, update Glue Catalog, write to curated

### Secrets Management
- All passwords and API keys stored in **AWS Secrets Manager**
- No credentials in code, environment variables, or Terraform state
- Airflow retrieves secrets at DAG runtime via `SecretsManagerBackend`

---

## 14. Monitoring & Alerting

| What | How | Alert When |
|---|---|---|
| Pipeline failures | Airflow → Slack webhook | Any task fails |
| Data quality failures | Great Expectations → Airflow → Slack | Any expectation fails |
| AWS cost overrun | CloudWatch Budget → SNS → email | Monthly spend > \$50 |
| S3 storage growth | CloudWatch → dashboard | > 100GB in raw zone |
| Redshift query perf | Redshift Query Monitor → CloudWatch | Query > 5 minutes |

---

## 15. Local Development Environment

### Full Stack (`make local-up`)
Starts with Docker Compose:
- **PostgreSQL** — Airflow metadata database
- **Airflow Webserver + Scheduler** — `localhost:8080`
- **Kafka + Zookeeper** — `localhost:9092`
- **Kafka UI** — `localhost:8090`
- **Metabase** — `localhost:3000` (connects to local Postgres as data source)

### Local vs AWS Differences
| Component | Local | AWS |
|---|---|---|
| Storage | Local filesystem `/tmp/urbanpulse/` | Amazon S3 |
| Spark | `local[*]` mode | EMR cluster |
| Kafka | Docker container | MSK managed service |
| Database | PostgreSQL | Redshift |
| Secrets | `.env` file | AWS Secrets Manager |

Configuration is controlled by `ENVIRONMENT=local|dev|prod` in `.env`.
