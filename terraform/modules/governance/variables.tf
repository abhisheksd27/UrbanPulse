# ==============================================================================
# File: terraform/modules/governance/variables.tf
# Role: Governance, Security & Cost Controls Module Variables
# ==============================================================================

variable "project_name" {
  description = "Project name identifier"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "monthly_budget_usd" {
  description = "Monthly AWS cost budget limit in USD"
  type        = string
  default     = "50"
}

variable "alert_email" {
  description = "Email address for budget and data quality anomaly notifications (optional)"
  type        = string
  default     = ""
}

variable "lake_bucket_arn" {
  description = "ARN of the S3 Data Lake bucket to register in AWS Lake Formation"
  type        = string
}

variable "glue_role_arn" {
  description = "IAM Role ARN for AWS Glue PySpark jobs"
  type        = string
}
