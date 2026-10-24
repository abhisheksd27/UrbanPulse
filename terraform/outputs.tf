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

# ─────────────────────────────────────────────────────────────
# MSK Kafka Outputs (Step 8 — Streaming)
# ─────────────────────────────────────────────────────────────

# Kafka bootstrap brokers (TLS) — paste into .env as KAFKA_BOOTSTRAP_SERVERS
output "kafka_bootstrap_brokers_tls" {
  description = "TLS Kafka broker endpoints — set as KAFKA_BOOTSTRAP_SERVERS in .env for Python and Spark"
  value       = module.msk.bootstrap_brokers_tls
}

# MSK Cluster ARN
output "msk_cluster_arn" {
  description = "Amazon Resource Name of the MSK Kafka cluster"
  value       = module.msk.cluster_arn
}

# ─────────────────────────────────────────────────────────────
# EMR Spark Outputs (Step 3 — Batch Processing)
# ─────────────────────────────────────────────────────────────

# EMR Cluster ID — paste into Airflow connection or DAG conf
output "emr_cluster_id" {
  description = "EMR cluster ID — used by Airflow EmrAddStepsOperator to submit Spark jobs"
  value       = module.emr.cluster_id
}

# EMR Master DNS — for SSH / Spark History Server access
output "emr_master_dns" {
  description = "DNS of the EMR master node — access Spark UI at http://<master_dns>:18080"
  value       = module.emr.master_public_dns
}

# ─────────────────────────────────────────────────────────────
# Airflow Outputs (Step 4 — Orchestration)
# ─────────────────────────────────────────────────────────────

# Airflow UI URL — open in browser after apply
output "airflow_url" {
  description = "Airflow webserver URL — open in browser, login admin/admin"
  value       = module.airflow.airflow_url
}

# SSH connection command
output "airflow_ssh_command" {
  description = "SSH command to connect to Airflow host"
  value       = module.airflow.ssh_command
}

# ─────────────────────────────────────────────────────────────
# Secrets Manager Outputs
# ─────────────────────────────────────────────────────────────

# Redshift secret ARN — use in boto3: secretsmanager.get_secret_value(SecretId=...)
output "redshift_secret_arn" {
  description = "Secrets Manager ARN for Redshift credentials — use in Python ingestion scripts"
  value       = module.secrets.redshift_secret_arn
}

# DataSF token SSM path — update value manually in AWS Console
output "datasf_token_ssm_path" {
  description = "SSM Parameter path for DataSF token — update in AWS Console: Systems Manager → Parameter Store"
  value       = module.secrets.datasf_token_ssm_path
}