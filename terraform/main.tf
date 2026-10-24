# ==============================================================================
# File: terraform/main.tf
# Role: Root Orchestration Module
#
# WHAT THIS FILE DOES:
# Calls and coordinates all four foundational infrastructure modules:
# 1. module "s3"       -> Creates Data Lake & Artifacts S3 buckets + lifecycle rules
# 2. module "iam"      -> Creates Least-Privilege IAM Roles (Redshift, Glue, Airflow)
# 3. module "vpc"      -> Creates Network Isolation (Public/Private Subnets + S3 Endpoint)
# 4. module "redshift" -> Creates Data Warehouse Cluster (Single/Multi-node)
#
# HOW IT HELPS:
# - Modular Design: Instead of 500 lines of spaghetti code in one file, infrastructure
#   is segmented into domain modules that can be developed and audited independently.
# - Explicit Dependency Graph: Terraform automatically infers creation order from outputs passed
#   as inputs:
#     Step A: S3 is created (outputs bucket ARNs & IDs).
#     Step B: IAM is created (needs S3 bucket ARNs to write least-privilege policies).
#     Step C: VPC is created (needs S3 bucket ID to configure S3 Gateway Endpoint).
#     Step D: Redshift is created (needs private subnets + SG from VPC, and Role ARN from IAM).
#
# HOW IT CONNECTS:
# - Serves as the central switchboard passing outputs from upstream modules
#   into downstream modules:
#     module.s3.lake_bucket_arn      ──> module.iam
#     module.s3.artifacts_bucket_arn ──> module.iam
#     module.s3.lake_bucket_id       ──> module.vpc
#     module.vpc.private_subnet_ids  ──> module.redshift
#     module.vpc.redshift_sg_id      ──> module.redshift
#     module.iam.redshift_role_arn   ──> module.redshift
# ==============================================================================

# ─────────────────────────────────────────────────────────────
# 1. Module: S3 (Data Lake + Artifacts Bucket)
# ─────────────────────────────────────────────────────────────
# Creates the S3 storage foundation for our lake layers (raw, staging, curated)
# and pipeline artifacts (Airflow DAG logs, dbt docs, Spark JAR files).
module "s3" {
  source       = "./modules/s3"
  project_name = var.project_name
  environment  = var.environment
}

# ─────────────────────────────────────────────────────────────
# 2. Module: IAM (Roles, Policies & Instance Profiles)
# ─────────────────────────────────────────────────────────────
# Defines service roles granting AWS services (Redshift, Glue, EC2 Airflow)
# secure access to read/write the S3 data lake without static access keys.
module "iam" {
  source               = "./modules/iam"
  project_name         = var.project_name
  environment          = var.environment
  lake_bucket_arn      = module.s3.lake_bucket_arn      # Connected from S3 module output
  artifacts_bucket_arn = module.s3.artifacts_bucket_arn # Connected from S3 module output
}

# ─────────────────────────────────────────────────────────────
# 3. Module: VPC (Network Isolation & Gateways)
# ─────────────────────────────────────────────────────────────
# Establishes our private cloud network (10.0.0.0/16).
# Redshift is quarantined in private subnets, while an S3 VPC Endpoint
# enables free, high-speed data transfer between Redshift and S3.
module "vpc" {
  source         = "./modules/vpc"
  project_name   = var.project_name
  environment    = var.environment
  vpc_cidr       = "10.0.0.0/16"
  lake_bucket_id = module.s3.lake_bucket_id # Connected to configure VPC endpoint routes
}

# ─────────────────────────────────────────────────────────────
# 4. Module: Redshift (Data Warehouse Cluster)
# ─────────────────────────────────────────────────────────────
# Provisions the columnar MPP data warehouse where analytics engineering (dbt)
# and data marts will reside in Steps 5-7.
module "redshift" {
  source             = "./modules/redshift"
  project_name       = var.project_name
  environment        = var.environment
  subnet_ids         = module.vpc.private_subnet_ids # Connected from VPC module
  security_group_ids = [module.vpc.redshift_sg_id]   # Connected from VPC module
  redshift_role_arn  = module.iam.redshift_role_arn  # Connected from IAM module
  master_password    = var.redshift_master_password
  node_type          = "dc2.large"
  number_of_nodes    = 1
}

# ─────────────────────────────────────────────────────────────
# 5. Module: MSK (Managed Kafka Cluster for Streaming)
# ─────────────────────────────────────────────────────────────
# Provisions a 2-broker Kafka MSK cluster for the real-time streaming pipeline.
# Python event_producer.py publishes taxi events, Spark Structured Streaming consumes them.
#
# Dependency chain:
#   module.vpc  → provides vpc_id, vpc_cidr, private_subnet_ids
#   module.iam  → provides airflow_role_name (MSK describe policy attached there)
#   module.s3   → provides artifacts_bucket_id (for MSK broker log delivery)
module "msk" {
  source = "./modules/msk"

  project_name        = var.project_name
  environment         = var.environment
  vpc_id              = module.vpc.vpc_id             # VPC → MSK
  vpc_cidr            = module.vpc.vpc_cidr           # VPC → MSK security group ingress
  private_subnet_ids  = module.vpc.private_subnet_ids # VPC → MSK broker placement
  artifacts_bucket_id = module.s3.artifacts_bucket_id # S3 → MSK log delivery
  airflow_role_name   = module.iam.airflow_role_name  # IAM → MSK describe policy attachment
}

# ─────────────────────────────────────────────────────────────
# 6. Module: EMR (Spark Cluster for Batch Processing)
# ─────────────────────────────────────────────────────────────
# Provisions an Amazon EMR cluster running Apache Spark 3.4 for the
# bronze→silver and silver→gold PySpark batch transformation jobs.
# Airflow DAGs submit steps to this cluster via EmrAddStepsOperator.
#
# Dependency chain:
#   module.vpc → provides vpc_id, vpc_cidr, private_subnet_ids
#   module.s3  → provides lake/artifacts bucket ARNs for Spark job S3 access
module "emr" {
  source = "./modules/emr"

  project_name         = var.project_name
  environment          = var.environment
  vpc_id               = module.vpc.vpc_id
  vpc_cidr             = module.vpc.vpc_cidr
  private_subnet_ids   = module.vpc.private_subnet_ids
  lake_bucket_arn      = module.s3.lake_bucket_arn
  artifacts_bucket_arn = module.s3.artifacts_bucket_arn
  artifacts_bucket_id  = module.s3.artifacts_bucket_id
}

# ─────────────────────────────────────────────────────────────
# 7. Module: Airflow (EC2 Orchestration Host)
# ─────────────────────────────────────────────────────────────
# Provisions an EC2 instance running Apache Airflow 2.8 via Docker Compose.
# Airflow is the control plane — it schedules and monitors every pipeline DAG.
#
# Dependency chain:
#   module.vpc → provides public_subnet_ids (Airflow goes in public subnet for UI access)
#                and airflow_sg_id (port 8080 security group)
#   module.iam → provides instance_profile_name (grants Airflow S3/EMR/MSK access)
#   module.s3  → provides artifacts_bucket_id (DAG sync + Airflow log remote storage)
module "airflow" {
  source = "./modules/airflow"

  project_name          = var.project_name
  environment           = var.environment
  vpc_id                = module.vpc.vpc_id
  public_subnet_ids     = module.vpc.public_subnet_ids
  airflow_sg_id         = module.vpc.airflow_sg_id
  instance_profile_name = module.iam.airflow_instance_profile_name
  artifacts_bucket_id   = module.s3.artifacts_bucket_id
  ssh_public_key        = var.ssh_public_key
}

# ─────────────────────────────────────────────────────────────
# 8. Module: Secrets (Secrets Manager + SSM Parameter Store)
# ─────────────────────────────────────────────────────────────
# Stores all sensitive config values — Redshift credentials, Airflow Fernet key,
# API tokens — in AWS-managed secret storage.
# No secrets in code, Git, or Terraform state.
#
# Dependency chain:
#   module.redshift → provides cluster_endpoint (stored in Redshift secret bundle)
module "secrets" {
  source = "./modules/secrets"

  project_name      = var.project_name
  environment       = var.environment
  redshift_host     = module.redshift.cluster_endpoint
  redshift_password = var.redshift_master_password
}