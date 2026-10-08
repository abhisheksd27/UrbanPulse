# ==============================================================================
# File: terraform/outputs.tf
# Role: Root Infrastructure Outputs
#
# WHAT THIS FILE DOES:
# Extracts critical resource identifiers and connection strings generated during
# 'terraform apply' and prints them to the console or makes them queryable via CLI:
#   terraform output <output_name>
#
# HOW IT HELPS:
# Eliminates manual searching in the AWS Console for bucket names, VPC IDs, and cluster endpoints.
# You can directly program downstream scripts (Python, Airflow, dbt) to pull these values.
#
# HOW IT CONNECTS TO DOWNSTREAM STEPS:
# - 'lake_bucket': Used by Step 3 Python ingestion scripts & Step 8 PySpark jobs to write data.
# - 'artifacts_bucket': Used by Step 4 Airflow for remote task logs and Step 7 dbt documentation hosting.
# - 'redshift_endpoint' & 'redshift_db': Fed directly into Step 7 dbt profiles.yml to connect dbt to Redshift.
# - 'vpc_id': Used in Step 9 when deploying Kafka/MSK brokers into the same VPC.
# - 'glue_role_arn': Used in Step 8 when configuring AWS Glue ETL jobs to transform silver data.
# - 'airflow_instance_profile': Used in Step 4 when launching the Airflow EC2 instance.
# ==============================================================================

# Data Lake Bucket Name (e.g., "urbanpulse-lake-dev")
output "lake_bucket" {
  description = "The S3 data lake bucket name where raw, staging, and curated taxi data is stored"
  value       = module.s3.lake_bucket_id
}

# Artifacts Bucket Name (e.g., "urbanpulse-artifacts-dev")
output "artifacts_bucket" {
  description = "The S3 bucket for Airflow logs, dbt artifacts, and Spark JAR binaries"
  value       = module.s3.artifacts_bucket_id
}

# Redshift Cluster Endpoint Address + Port (e.g., "urbanpulse-cluster-dev.xxx.us-east-1.redshift.amazonaws.com:5439")
output "redshift_endpoint" {
  description = "Connection endpoint string for the Redshift data warehouse cluster"
  value       = module.redshift.cluster_endpoint
}

# Redshift Database Name (e.g., "nyc_taxi_dw")
output "redshift_db" {
  description = "The default analytical database name in Redshift"
  value       = module.redshift.database_name
}

# VPC Identifier
output "vpc_id" {
  description = "The ID of the VPC housing our entire data platform network"
  value       = module.vpc.vpc_id
}

# IAM Role ARN for AWS Glue / Spark jobs
output "glue_role_arn" {
  description = "IAM Role ARN to attach to AWS Glue PySpark jobs (Step 8)"
  value       = module.iam.glue_role_arn
}

# IAM Instance Profile Name for Airflow EC2
output "airflow_instance_profile" {
  description = "EC2 Instance Profile to attach to Airflow host instance (Step 4)"
  value       = module.iam.airflow_instance_profile_name
}