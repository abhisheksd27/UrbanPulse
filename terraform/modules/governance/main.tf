# ==============================================================================
# File: terraform/modules/governance/main.tf
# Role: Enterprise Governance, Security & Cost Control Infrastructure
#
# WHAT THIS MODULE DOES:
# 1. AWS Cost Budget & Alerts: Enforces strict monthly spending threshold ($50)
#    with automated SNS notifications for Actual (80%) and Forecasted (100%) spend.
# 2. AWS KMS Encryption: Provisions Customer Managed Key (CMK) with annual rotation
#    for envelope encryption of data at rest across S3, Redshift, and Kafka.
# 3. SNS Alerts Topic: Central notification bus for infrastructure, cost, and
#    Great Expectations data quality failures.
# 4. AWS Lake Formation Resource Registration: Enrolls the S3 data lake bucket into
#    AWS Lake Formation for granular column/row-level security policies.
# ==============================================================================

# ─────────────────────────────────────────────────────────────
# 1. Customer Managed KMS Key (CMK)
# ─────────────────────────────────────────────────────────────
resource "aws_kms_key" "lake_key" {
  description             = "KMS Customer Managed Key for ${var.project_name} (${var.environment}) data lake encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  tags = {
    Name        = "${var.project_name}-${var.environment}-kms"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_kms_alias" "lake_key_alias" {
  name          = "alias/${var.project_name}-${var.environment}-lake-key"
  target_key_id = aws_kms_key.lake_key.key_id
}

# ─────────────────────────────────────────────────────────────
# 2. SNS Topic for Platform Alerts & Quality Failures
# ─────────────────────────────────────────────────────────────
resource "aws_sns_topic" "alerts" {
  name = "${var.project_name}-${var.environment}-platform-alerts"

  tags = {
    Name        = "${var.project_name}-${var.environment}-platform-alerts"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_sns_topic_subscription" "email_sub" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ─────────────────────────────────────────────────────────────
# 3. AWS Cost Budget ($50 / Month Limit)
# ─────────────────────────────────────────────────────────────
resource "aws_budgets_budget" "cost_control" {
  name              = "${var.project_name}-${var.environment}-monthly-budget"
  budget_type       = "COST"
  limit_amount      = var.monthly_budget_usd
  limit_unit        = "USD"
  time_period_start = "2024-01-01_00:00"
  time_unit         = "MONTHLY"

  # Alert 1: Warning when ACTUAL spending exceeds 80% of monthly budget
  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 80
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_sns_topic_arns = [aws_sns_topic.alerts.arn]
  }

  # Alert 2: Critical when FORECASTED spending will exceed 100% of budget
  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 100
    threshold_type            = "PERCENTAGE"
    notification_type         = "FORECASTED"
    subscriber_sns_topic_arns = [aws_sns_topic.alerts.arn]
  }
}

# ─────────────────────────────────────────────────────────────
# 4. AWS Lake Formation S3 Resource Registration
# ─────────────────────────────────────────────────────────────
resource "aws_lakeformation_resource" "data_lake_bucket" {
  arn = var.lake_bucket_arn
}
