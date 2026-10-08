# ==============================================================================
# File: terraform/modules/iam/variables.tf
# Role: Input Interface for IAM Security Module
#
# WHAT THIS FILE DOES:
# Declares the inputs required by the IAM module to construct least-privilege policies.
#
# HOW IT HELPS:
# Prevents using wildcard ("*") resource permissions in IAM policies. By requiring the specific
# S3 bucket ARNs, we enforce least privilege: Redshift and Glue can ONLY access our project buckets.
#
# HOW IT CONNECTS:
# - 'project_name', 'environment': Passed from root 'variables.tf'.
# - 'lake_bucket_arn', 'artifacts_bucket_arn': Passed from 'module.s3' outputs in root 'main.tf'.
# ==============================================================================

variable "project_name" {
  description = "Project name prefix for IAM role naming"
  type        = string
}

variable "environment" {
  description = "Deployment environment suffix"
  type        = string
}

variable "lake_bucket_arn" {
  description = "ARN of the S3 data lake bucket, used to scope IAM policies"
  type        = string
}

variable "artifacts_bucket_arn" {
  description = "ARN of the S3 artifacts bucket, used to scope IAM policies"
  type        = string
}