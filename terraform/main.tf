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