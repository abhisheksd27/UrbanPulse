# ==============================================================================
# File: terraform/modules/s3/main.tf
# Role: S3 Data Lake & Artifacts Storage Implementation
#
# WHAT THIS FILE DOES:
# 1. Provisions the primary Data Lake bucket ('urbanpulse-lake-dev').
# 2. Configures Bucket Versioning (recovers from accidental deletes/overwrites).
# 3. Enforces Server-Side Encryption (AES256) at rest.
# 4. Blocks ALL public access to prevent data leaks.
# 5. Configures S3 Lifecycle Rules:
#    - Automatically transitions older raw data to cheaper storage tiers (Standard-IA, Glacier IR).
#    - Deletes incomplete multipart uploads to stop zombie charges.
# 6. Provisions the Artifacts bucket ('urbanpulse-artifacts-dev') for Airflow/dbt/Spark binaries.
# 7. Creates prefix placeholder objects establishing the raw/staging/curated lake zones.
#
# HOW IT HELPS:
# - Cost Optimization: S3 Lifecycle rules automatically cut raw data storage costs by up to 83%.
# - Security: Public access block prevents accidental exposure of sensitive trip/fare data.
# - Clean Architecture: Sets up the classic Medallion/CEDW Lakehouse layers (raw, staging, curated).
#
# HOW IT CONNECTS:
# - Connects to IAM Module: Outputs 'lake_bucket_arn' and 'artifacts_bucket_arn' so IAM policies
#   can grant Redshift, Glue, and Airflow permission to read/write these exact buckets.
# - Connects to VPC Module: Outputs 'lake_bucket_id' so the VPC S3 Gateway Endpoint can route
#   traffic directly to S3 without going through expensive NAT Gateways.
# - Connects to Step 2 & 3: Python ingestion scripts write directly to 's3://<lake_bucket>/raw/'.
# ==============================================================================

locals {
  # Unique lowercase suffix prevents bucket name collisions & AWS S3 uppercase rejection.
  suffix = "${lower(var.project_name)}-${var.environment}"
}

# ─────────────────────────────────────────────────────────────
# 1. Primary Data Lake Bucket (Raw / Staging / Curated layers)
# ─────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "lake" {
  bucket = "${lower(var.project_name)}-lake-${var.environment}"

  # Set to true in production to prevent accidental destruction via 'terraform destroy'
  lifecycle {
    prevent_destroy = false
  }
}

# Protects data integrity: keeps past versions of objects to recover from accidental deletion
resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Encrypts all data at rest using AWS-managed AES-256 keys with zero key management overhead
resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Defense-in-depth security: blocks public ACLs and bucket policies from making lake files public
resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ─────────────────────────────────────────────────────────────
# S3 Lifecycle Rules: Automated Cost Tiering & Cleanup
# ─────────────────────────────────────────────────────────────
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  # Rule 1: Tier old raw taxi and weather data down to cheaper storage classes
  rule {
    id     = "raw-data-tiering"
    status = "Enabled"

    filter {
      prefix = "raw/" # Targets only the raw zone where historical files accumulate
    }

    # After 30 days: Move from S3 Standard ($0.023/GB) to Standard-IA ($0.0125/GB) -> 46% savings
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    # After 90 days: Move to Glacier Instant Retrieval ($0.004/GB) -> 83% savings with ms retrieval
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }

    # After 365 days: Delete raw file (data can be re-fetched from TLC source if ever needed)
    expiration {
      days = 365
    }
  }

  # Rule 2: Clean up failed/abandoned multipart uploads
  # When large files (e.g. 500MB Parquet) fail mid-upload, abandoned parts linger and accrue charges.
  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {} # Applies to all prefixes in the lake bucket

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# ─────────────────────────────────────────────────────────────
# 2. Pipeline Artifacts Bucket (Airflow Logs, dbt docs, Spark JARs)
# ─────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "artifacts" {
  bucket = "${lower(var.project_name)}-artifacts-${var.environment}"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

# ─────────────────────────────────────────────────────────────
# 3. Data Lake Folder Prefixes (Zero-Byte Object Placeholders)
# ─────────────────────────────────────────────────────────────
# S3 is technically a flat key-value store, but downstream tools (Athena, Spark, Airflow)
# expect these standard directory prefixes to exist.
resource "aws_s3_object" "lake_prefixes" {
  for_each = toset([
    "raw/trips/",          # Landed NYC Taxi Parquet files (Step 3)
    "raw/weather/",        # Landed Open-Meteo weather JSON files (Step 3)
    "raw/stream/",         # Landed real-time streaming micro-batches (Step 9)
    "staging/trips/",      # Cleaned, de-duplicated Parquet files (Step 8 Spark)
    "staging/weather/",    # Standardized weather Parquet files (Step 8 Spark)
    "curated/facts/",      # Joined, gold-tier analytical facts (Step 8 Spark / Step 7 dbt)
    "curated/dimensions/", # Conformed dimensions (zones, calendar) (Step 6 / 7)
  ])

  bucket  = aws_s3_bucket.lake.id
  key     = each.value
  content = "" # Empty content acts as a directory marker in the AWS console
}
