# ==============================================================================
# File: terraform/modules/iam/outputs.tf
# Role: Output Interface for IAM Security Module
#
# WHAT THIS FILE DOES:
# Exposes the created IAM Role ARNs and Instance Profile name to the parent module.
#
# HOW IT HELPS:
# Enables loose coupling: other modules (Redshift, compute instances) don't need to know
# how the IAM roles were constructed, only their ARNs.
#
# HOW IT CONNECTS:
# - 'redshift_role_arn': Fed into 'module.redshift' in root 'main.tf' to authorize COPY/UNLOAD.
# - 'glue_role_arn': Exported to root 'outputs.tf' -> used in Step 8 for Glue jobs.
# - 'airflow_instance_profile_name': Exported to root 'outputs.tf' -> used in Step 4 for Airflow EC2.
# ==============================================================================

# Redshift IAM Role ARN (e.g., "arn:aws:iam::123456789012:role/urbanpulse-redshift-role-dev")
output "redshift_role_arn" {
  description = "IAM Role ARN to attach to Redshift cluster for S3 access"
  value       = aws_iam_role.redshift.arn
}

# AWS Glue IAM Role ARN (e.g., "arn:aws:iam::123456789012:role/urbanpulse-glue-role-dev")
output "glue_role_arn" {
  description = "IAM Role ARN to attach to AWS Glue PySpark ETL jobs"
  value       = aws_iam_role.glue.arn
}

# Airflow EC2 Instance Profile Name
output "airflow_instance_profile_name" {
  description = "IAM Instance Profile name to attach to EC2 running Airflow"
  value       = aws_iam_instance_profile.airflow_ec2.name
}