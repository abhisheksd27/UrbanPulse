locals {
  # Unique suffix prevents bucket name collisions across AWS accounts
  suffix = "${var.project_name}-${var.environment}"
}

# ─────────────────────────────────────────────────────────────
# 1. Data Lake Bucket  (raw / staging / curated prefixes)
# ─────────────────────────────────────────────────────────────


resource "aws_s3_bucket" "lake"{
    bucket = "data-lake-${local.suffix}"

    #prevent accidental deletion of the lake during terraform destroy

    lifecycle {
        prevent_destroy = false
    }
}

resource "aws_s3_bucket_versioning" "lake" {
    bucket = aws_s3_bucket.lake.id
    versioning_configuration {
        status = "Enabled"
    }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
    bucket = aws_s3_bucket.lake.id
    rule {
        apply_server_side_encryption_by_default {
            sse_algorithm = "AES256"
        }
    }
}

resource "aws_s3_bucket_public_access_block" "lake" {
    bucket = aws_s3_bucket.lake.id
    block_public_acls       = true
    block_public_policy     = true
    ignore_public_acls      = true
    restrict_public_buckets = true
}

# ─────────────────────────────────────────────────────────────
# Lifecycle rules: automatically tier old data to save costs
# ─────────────────────────────────────────────────────────────

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  # Rule 1: Move old raw data to cheaper storage tiers
  rule {
    id     = "raw-data-tiering"
    status = "Enabled"
    filter {
      prefix = "raw/"   # only applies to the raw zone
    }
    # After 30 days in S3 Standard, move to S3 Standard-IA (Infrequent Access)
    # Cost: $0.023/GB → $0.0125/GB  (46% cheaper, retrieval fee applies)
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    # After 90 days, move to Glacier Instant Retrieval
    # Cost: $0.004/GB (83% cheaper, millisecond retrieval)
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
    # After 365 days, delete (raw data is re-processable from source)
    expiration {
      days = 365
    }
  }
  # Rule 2: Clean up incomplete multipart uploads (common S3 gotcha)
  # Large Parquet files uploaded in parts leave orphaned parts if upload fails
  # These cost money but serve no purpose
  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    filter {}   # applies to entire bucket
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}


# ─────────────────────────────────────────────────────────────
# 2. Artifacts Bucket  (Airflow logs, dbt docs, Spark jars)
# ─────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "artifacts" {
  bucket = "nyc-taxi-artifacts-${local.suffix}"
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
# Folder "placeholders" — S3 is flat but tools expect prefixes
# These empty objects create the visual folder structure in S3 console
# ─────────────────────────────────────────────────────────────
resource "aws_s3_object" "lake_prefixes" {
  for_each = toset([
    "raw/trips/",
    "raw/weather/",
    "raw/stream/",
    "staging/trips/",
    "staging/weather/",
    "curated/facts/",
    "curated/dimensions/",
  ])
  bucket  = aws_s3_bucket.lake.id
  key     = each.value
  content = ""   # empty object, just to create the prefix
}
