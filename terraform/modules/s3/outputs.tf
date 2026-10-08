# ==============================================================================
# File: terraform/modules/s3/outputs.tf
# Role: Output Interface for S3 Storage Module
#
# WHAT THIS FILE DOES:
# Exposes the created bucket IDs and ARNs (Amazon Resource Names) to the parent module.
#
# HOW IT HELPS:
# In Terraform, child module resources are private unless explicitly exported via outputs.
#
# HOW IT CONNECTS:
# - 'lake_bucket_arn': Exported to parent 'main.tf' -> passed to 'module.iam' to build IAM policies.
# - 'artifacts_bucket_arn': Exported to parent 'main.tf' -> passed to 'module.iam'.
# - 'lake_bucket_id': Exported to parent 'main.tf' -> passed to 'module.vpc' and root outputs.
# - 'artifacts_bucket_id': Exported to parent 'main.tf' -> passed to root outputs.
# ==============================================================================

# Data Lake Bucket Name (e.g., "urbanpulse-lake-dev")
output "lake_bucket_id" {
  description = "The name of the S3 data lake bucket"
  value       = aws_s3_bucket.lake.id
}

# Data Lake Bucket ARN (e.g., "arn:aws:s3:::urbanpulse-lake-dev")
output "lake_bucket_arn" {
  description = "The ARN of the S3 data lake bucket (used in IAM permission policies)"
  value       = aws_s3_bucket.lake.arn
}

# Artifacts Bucket Name (e.g., "urbanpulse-artifacts-dev")
output "artifacts_bucket_id" {
  description = "The name of the S3 artifacts bucket"
  value       = aws_s3_bucket.artifacts.id
}

# Artifacts Bucket ARN (e.g., "arn:aws:s3:::urbanpulse-artifacts-dev")
output "artifacts_bucket_arn" {
  description = "The ARN of the S3 artifacts bucket (used in IAM permission policies)"
  value       = aws_s3_bucket.artifacts.arn
}