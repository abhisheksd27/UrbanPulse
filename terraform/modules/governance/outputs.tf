# ==============================================================================
# File: terraform/modules/governance/outputs.tf
# Role: Governance, Security & Cost Controls Module Outputs
# ==============================================================================

output "alerts_topic_arn" {
  description = "SNS Topic ARN for platform alerts and cost anomalies"
  value       = aws_sns_topic.alerts.arn
}

output "kms_key_arn" {
  description = "KMS Customer Managed Key ARN for data lake envelope encryption"
  value       = aws_kms_key.lake_key.arn
}

output "kms_key_id" {
  description = "KMS Customer Managed Key ID"
  value       = aws_kms_key.lake_key.key_id
}

output "budget_id" {
  description = "ID of the AWS Cost Budget resource"
  value       = aws_budgets_budget.cost_control.id
}
