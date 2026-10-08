# ==============================================================================
# File: terraform/modules/redshift/main.tf
# Role: Redshift Data Warehouse Implementation
#
# WHAT THIS FILE DOES:
# 1. Creates a Redshift Subnet Group binding the cluster to our VPC private subnets.
# 2. Creates a Custom Parameter Group:
#    - Enables user activity audit logging for compliance.
#    - Enforces SSL for all client connections.
#    - Enables concurrency scaling for sudden query load spikes.
# 3. Provisions the Redshift Cluster:
#    - Columnar Massively Parallel Processing (MPP) architecture.
#    - Single-node 'dc2.large' in dev for low cost.
#    - 'publicly_accessible = false' ensures zero public internet exposure.
#    - Attaches the Redshift IAM role for direct S3 lake access.
#    - Enables automated snapshots (1-day retention in dev) and encryption at rest.
#
# HOW IT HELPS:
# - Analytical Query Performance: Columnar storage + zone maps allow scanning only needed columns
#   and disk blocks, making aggregations across millions of taxi records 10-50x faster than row databases.
# - Security: Quarantined in private subnets, encrypted at rest, SSL enforced, and protected by SG.
#
# HOW IT CONNECTS:
# - Connects to VPC Module: Deployed into 'var.subnet_ids' and secured by 'var.security_group_ids'.
# - Connects to IAM Module: Attaches 'var.redshift_role_arn' so Redshift can execute S3 COPY commands.
# - Connects to Step 5 (Warehouse): In Step 5, we execute DDL to create our 4 CEDW schemas
#   (landing, staging, core, marts) on this cluster.
# - Connects to Step 7 (dbt): dbt connects to this cluster to build models and marts.
# ==============================================================================

# ─────────────────────────────────────────────────────────────
# 1. Redshift Subnet Group
# ─────────────────────────────────────────────────────────────
# Redshift requires a subnet group spanning across at least 2 Availability Zones.
# It places the cluster compute slices within the private subnets provided by the VPC module.
resource "aws_redshift_subnet_group" "main" {
  name       = "${var.project_name}-subnet-group-${var.environment}"
  subnet_ids = var.subnet_ids # Private subnets from VPC module

  tags = { Name = "${var.project_name}-redshift-subnet-group" }
}

# ─────────────────────────────────────────────────────────────
# 2. Redshift Parameter Group
# ─────────────────────────────────────────────────────────────
# Defines database engine configuration parameters applied across all nodes in the cluster.
resource "aws_redshift_parameter_group" "main" {
  name   = "${var.project_name}-params-${var.environment}"
  family = "redshift-1.0"

  # Audit logging: captures all SQL queries executed against the cluster for compliance (Step 10)
  parameter {
    name  = "enable_user_activity_logging"
    value = "true"
  }

  # Security: forces all client drivers (dbt, psql, Metabase) to connect using TLS/SSL encryption
  parameter {
    name  = "require_ssl"
    value = "true"
  }

  # Performance: automatically adds bursting clusters if concurrent queries queue up
  parameter {
    name  = "max_concurrency_scaling_clusters"
    value = "1"
  }
}

# ─────────────────────────────────────────────────────────────
# 3. Redshift Cluster
# ─────────────────────────────────────────────────────────────
resource "aws_redshift_cluster" "main" {
  cluster_identifier = "${var.project_name}-cluster-${var.environment}"
  database_name      = var.database_name
  master_username    = var.master_username
  master_password    = var.master_password
  node_type          = var.node_type
  cluster_type       = var.number_of_nodes == 1 ? "single-node" : "multi-node"
  number_of_nodes    = var.number_of_nodes

  # Networking & Security
  cluster_subnet_group_name = aws_redshift_subnet_group.main.name
  vpc_security_group_ids    = var.security_group_ids
  publicly_accessible       = false # CRITICAL: Completely unreachable from the public internet!

  # Engine Configuration
  cluster_parameter_group_name = aws_redshift_parameter_group.main.name
  iam_roles                    = [var.redshift_role_arn] # Role attached for S3 COPY/UNLOAD commands

  # Automated Backups: keep 1 day of daily backups in dev (max 35 in production)
  automated_snapshot_retention_period = 1
  skip_final_snapshot                 = true # Avoids blocking teardown when running terraform destroy

  # Hardware-level encryption of all disk blocks using AWS-managed KMS keys
  encrypted = true

  tags = { Name = "${var.project_name}-redshift-${var.environment}" }
}