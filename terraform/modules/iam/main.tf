# ==============================================================================
# File: terraform/modules/iam/main.tf
# Role: IAM Least-Privilege Roles, Policies & Instance Profiles
#
# WHAT THIS FILE DOES:
# Creates three dedicated service roles with temporary STS-assumed credentials:
# 1. Redshift Role: Enables the Redshift cluster to execute COPY (from S3) and UNLOAD (to S3).
# 2. Glue / Spark Role: Enables AWS Glue PySpark ETL jobs to read/write lake buckets and write logs.
# 3. Airflow EC2 Instance Role: Allows the Airflow server to trigger Glue jobs, read/write S3,
#    and read connection strings securely from AWS SSM Parameter Store.
#
# HOW IT HELPS:
# - Zero Hardcoded Secrets: No long-lived AWS Access Keys (AKIA...) stored on disk or in Git.
#   AWS STS issues auto-rotating temporary credentials to instances and services.
# - Least Privilege: Scoped strictly to project S3 buckets (passed from S3 module ARNs).
#
# HOW IT CONNECTS:
# - Inputs from S3 Module: Uses 'var.lake_bucket_arn' and 'var.artifacts_bucket_arn' in policy statements.
# - Output to Redshift Module: Passes 'redshift_role_arn' to 'module.redshift' in root 'main.tf'
#   so Redshift attaches this role upon cluster creation.
# - Output to Step 4 (Airflow): Exports 'airflow_instance_profile_name' to attach to the Airflow EC2 node.
# - Output to Step 8 (Spark): Exports 'glue_role_arn' to configure AWS Glue PySpark jobs.
# ==============================================================================

# Dynamic lookups for current AWS Account ID and Region (eliminates hardcoding)
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ─────────────────────────────────────────────────────────────
# 1. Redshift IAM Role & Policy (Used in Step 5 for COPY/UNLOAD)
# ─────────────────────────────────────────────────────────────

# Trust Policy: declares WHO can assume this role (redshift.amazonaws.com)
resource "aws_iam_role" "redshift" {
  name = "${var.project_name}-redshift-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Permission Policy: declares WHAT Redshift is allowed to do once it assumes the role
resource "aws_iam_policy" "redshift_s3" {
  name        = "${var.project_name}-redshift-s3-policy-${var.environment}"
  description = "Allows Redshift to read from the data lake bucket and write query unloads"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Redshift must list the bucket to discover Parquet files when running COPY commands
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = [var.lake_bucket_arn]
      },
      {
        # Redshift reads the raw/staging Parquet files from S3 directly into its compute slices
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${var.lake_bucket_arn}/*"]
      },
      {
        # Redshift writes UNLOAD command results to the artifacts bucket
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["${var.artifacts_bucket_arn}/redshift-unload/*"]
      }
    ]
  })
}

# Bind the policy to the Redshift role
resource "aws_iam_role_policy_attachment" "redshift_s3" {
  role       = aws_iam_role.redshift.name
  policy_arn = aws_iam_policy.redshift_s3.arn
}

# ─────────────────────────────────────────────────────────────
# 2. Glue / Spark IAM Role & Policies (Used in Step 8 for PySpark)
# ─────────────────────────────────────────────────────────────

# Trust Policy: allows the AWS Glue ETL service to assume this role
resource "aws_iam_role" "glue" {
  name = "${var.project_name}-glue-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Attach AWS managed policy for standard Glue service capabilities (CloudWatch logging, Glue Catalog access)
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom policy granting Glue full Read/Write access to project S3 buckets for ETL transformations
resource "aws_iam_policy" "glue_s3" {
  name        = "${var.project_name}-glue-s3-policy-${var.environment}"
  description = "Allows Glue PySpark jobs to read raw data and write transformed silver/gold Parquet"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
        "s3:GetBucketLocation", "s3:ListBucket"
      ]
      Resource = [
        var.lake_bucket_arn,
        "${var.lake_bucket_arn}/*",
        var.artifacts_bucket_arn,
        "${var.artifacts_bucket_arn}/*"
      ]
    }]
  })
}

# Bind S3 policy to the Glue role
resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue.name
  policy_arn = aws_iam_policy.glue_s3.arn
}

# ─────────────────────────────────────────────────────────────
# 3. Airflow EC2 Instance Role (Used in Step 4 for Orchestration)
# ─────────────────────────────────────────────────────────────

# Trust Policy: allows an EC2 instance hosting Airflow to assume this role
resource "aws_iam_role" "airflow_ec2" {
  name = "${var.project_name}-airflow-ec2-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Comprehensive operational policy for Airflow orchestrator
resource "aws_iam_policy" "airflow_permissions" {
  name        = "${var.project_name}-airflow-policy-${var.environment}"
  description = "Permissions needed by Airflow DAGs to trigger jobs, read secrets, and store logs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Airflow writes remote task execution logs and uploads temporary artifacts to S3
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:GetBucketLocation", "s3:ListBucket"
        ]
        Resource = [
          var.lake_bucket_arn, "${var.lake_bucket_arn}/*",
          var.artifacts_bucket_arn, "${var.artifacts_bucket_arn}/*"
        ]
      },
      {
        # Airflow DAGs can trigger and monitor AWS Glue PySpark ETL runs (Step 8)
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJob"]
        Resource = ["arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/*"]
      },
      {
        # Secure credentials retrieval: Airflow pulls DB connection strings from SSM Parameter Store
        Effect   = "Allow"
        Action   = ["ssm:GetParameter", "ssm:GetParameters"]
        Resource = ["arn:aws:ssm:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:parameter/${var.project_name}/*"]
      },
      {
        # CloudWatch metrics and logging for Airflow observability (Step 10)
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData", "logs:CreateLogGroup",
          "logs:CreateLogStream", "logs:PutLogEvents"
        ]
        Resource = ["*"]
      }
    ]
  })
}

# Bind policy to Airflow role
resource "aws_iam_role_policy_attachment" "airflow_ec2" {
  role       = aws_iam_role.airflow_ec2.name
  policy_arn = aws_iam_policy.airflow_permissions.arn
}

# Instance profile: wrapper that allows attaching the IAM role directly to an EC2 instance
resource "aws_iam_instance_profile" "airflow_ec2" {
  name = "${var.project_name}-airflow-instance-profile-${var.environment}"
  role = aws_iam_role.airflow_ec2.name
}