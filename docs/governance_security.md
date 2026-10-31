# 🛡️ UrbanPulse Governance, Security & Cost Controls

This document details the enterprise security posture, data governance policies, cost control guardrails, and compliance standards implemented across the UrbanPulse platform.

---

## 🏛️ Security Philosophy & Guardrails

UrbanPulse enforces **defense-in-depth** across every layer of the cloud modern data stack:
1. **Zero Standing Hardcoded Credentials:** All credentials reside in AWS Secrets Manager and SSM Parameter Store; code and Git history contain zero plain-text tokens or passwords.
2. **Least Privilege Principle:** IAM roles grant only the exact permissions needed for each service (Redshift, Glue, Airflow EC2) to perform its specific role.
3. **Pervasive Encryption:** All data at rest is encrypted with Customer Managed Keys (KMS CMK); all data in flight requires TLS 1.2+ / TLS 1.3.
4. **Strict Cloud Budgeting:** Pre-configured AWS Budgets ($50/month) with proactive SNS alarms prevent cloud bill surprises.
5. **PII Anonymization:** Raw mobility data undergoes hashing / masking so that driver identities and vehicle medallion numbers cannot be reverse-engineered.

---

## 🔑 Identity & Access Management (IAM) Matrix

| Principal / Service | IAM Role Name | Allowed Resources | Denied Actions |
|---|---|---|---|
| **Redshift Cluster** | `urbanpulse-redshift-role` | `s3:GetObject`, `s3:ListBucket` on S3 Data Lake | Write/Delete on S3; IAM changes; EC2 modifications |
| **AWS Glue / EMR Spark** | `urbanpulse-glue-role` | Read `raw/`, Read/Write `staging/` & `curated/` | Delete lake root; access to unrelated AWS accounts |
| **Airflow EC2 Host** | `urbanpulse-airflow-role` | EMR step execution, MSK metadata, Secrets Manager read | IAM administrative actions; destructive operations |
| **Data Analysts (Metabase)** | Read-Only DB User | `SELECT` on `marts.*` views in Redshift | Direct access to raw S3 data; write or DDL on warehouse |

---

## 🔒 Data Encryption Standard

### 1. Encryption at Rest
- **Amazon S3 Data Lake:** Server-Side Encryption with AWS KMS Customer Managed Keys (`SSE-KMS`). Automatic annual key rotation is enabled via `aws_kms_key.lake_key`.
- **Amazon Redshift:** Encrypted using KMS CMK. All data blocks, commit logs, and automated snapshots are encrypted before persistence to disk.
- **Amazon MSK (Kafka):** Storage volume encryption with KMS; broker log streams in S3 are encrypted.

### 2. Encryption in Transit
- **TLS 1.2+:** Mandatory across all HTTP/Socrata API calls, Open-Meteo REST endpoints, and Airflow web server access.
- **Kafka MSK:** Inter-broker and client-to-broker communication is strictly configured with TLS encryption on port `9094`.
- **VPC S3 Gateway Endpoint:** S3 data lake access from Redshift and EMR remains entirely within the AWS private network backbone, bypassing the public internet without incurring NAT Gateway data transfer fees.

---

## 🗄️ AWS Lake Formation Governance

AWS Lake Formation manages fine-grained catalog security and centralized access permissions:

1. **Resource Registration:**
   The S3 Data Lake bucket (`arn:aws:s3:::urbanpulse-lake-${env}`) is registered with Lake Formation via `aws_lakeformation_resource`.
2. **Metadata Catalog Separation:**
   - `urbanpulse_raw_db`: Raw Socrata and Open-Meteo schemas (restricted to Ingestion and Quality roles).
   - `urbanpulse_staging_db`: Deduplicated and geocoded Silver datasets.
   - `urbanpulse_curated_db`: Weather-joined and KPI-enriched Gold datasets (accessible to Analytics and BI).
3. **Column-Level Security:**
   Sensitive raw fields (such as exact internal taxi meter identifiers) are excluded from analyst query roles.

---

## 💵 Cost Controls & Budget Guardrails

To ensure zero budget overruns in dev and production environments, the platform defines the following IaC cost controls in `terraform/modules/governance/`:

```
                           ┌──────────────────────────────┐
                           │ AWS Budgets ($50.00 / month) │
                           └──────────────┬───────────────┘
                                          │
                  ┌───────────────────────┴───────────────────────┐
                  ▼                                               ▼
         [80% ACTUAL Spend]                            [100% FORECASTED Spend]
                  │                                               │
                  ▼                                               ▼
        [SNS Warning Alert]                            [SNS Critical Alert]
                  │                                               │
                  └───────────────▶ [Email Notification] ◀────────┘
```

### Auto-Termination Policies
- **Amazon EMR Clusters:** Configured with `auto_termination_policy` terminating idle clusters after 20 minutes of inactivity.
- **Amazon Redshift:** Configured for single-node `dc2.large` in development ($0.25/hour) and paused when not in use.
- **S3 Storage Lifecycle:**
  - `raw/` files: Expire or transition to Glacier Instant Retrieval after 90 days.
  - `staging/` intermediate files: Expire after 30 days.
  - S3 incomplete multipart uploads: Aborted automatically after 7 days.

---

## 🕵️ Data Privacy, Anonymization & PII Protection

San Francisco taxi trip records contain spatial-temporal information that could theoretically be used for re-identification if unprotected:

1. **Coordinate Truncation:**
   GPS coordinates are rounded to 6 decimal places (~0.1m precision) in Silver, and mapped to neighborhood centroids (`pickup_neighborhood`, `dropoff_neighborhood`) in Gold and BI Marts.
2. **Surrogate Key Generation:**
   Primary keys are constructed using SHA-256 surrogate keys (`generate_surrogate_key` macro) combining `trip_id`, `pickup_datetime`, and `vehicle_id`.
3. **Driver Anonymization:**
   Driver medallion IDs and personal vehicle registration numbers are never stored in plain text; only pseudonymized fleet identifiers (`V-XXX`) are retained.

---

## 📜 Audit Logging & Observability

- **AWS CloudTrail:** Captures all management API calls across S3, KMS, Redshift, and IAM.
- **S3 Server Access Logs:** Delivered to `urbanpulse-artifacts-${env}/logs/s3/` for access auditing.
- **Airflow Task Logs:** Streamed to S3 remote logging prefix `logs/airflow/` for debugging.
- **Great Expectations Quality Reports:** Validation reports persisted to `quality/reports/validation_report.json` and logged with timestamp and rule status.
