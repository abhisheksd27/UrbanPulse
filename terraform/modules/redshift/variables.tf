# ==============================================================================
# File: terraform/modules/redshift/variables.tf
# Role: Input Interface for Redshift Data Warehouse Module
#
# WHAT THIS FILE DOES:
# Declares the required inputs to configure, size, network, and secure the
# Redshift data warehouse cluster.
#
# HOW IT HELPS:
# Enables tuning node types (e.g. 'dc2.large' for dev vs 'ra3.xlplus' for prod) and cluster sizing
# (single-node vs multi-node) cleanly via variables without touching module internals.
#
# HOW IT CONNECTS:
# - 'project_name', 'environment', 'master_password': Fed from root 'variables.tf'.
# - 'subnet_ids', 'security_group_ids': Fed from 'module.vpc' outputs.
# - 'redshift_role_arn': Fed from 'module.iam' output.
# ==============================================================================

variable "project_name" {
  description = "Project name prefix for Redshift resource naming"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "subnet_ids" {
  description = "List of private subnet IDs for Redshift subnet group (requires >= 2 AZs)"
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security group IDs restricting access to Redshift port 5439"
  type        = list(string)
}

variable "redshift_role_arn" {
  description = "IAM Role ARN attached to Redshift cluster to permit S3 COPY/UNLOAD"
  type        = string
}

variable "master_password" {
  description = "Redshift master superuser password (sensitive)"
  type        = string
  sensitive   = true
}

variable "node_type" {
  description = "Redshift compute node hardware type (dc2.large = Dense Compute 2-core 15GB RAM)"
  type        = string
  default     = "dc2.large"
}

variable "number_of_nodes" {
  description = "Number of compute nodes in the cluster (1 = single-node mode)"
  type        = number
  default     = 1
}

variable "master_username" {
  description = "Master database superuser username"
  type        = string
  default     = "admin"
}

variable "database_name" {
  description = "Initial analytical database name"
  type        = string
  default     = "nyc_taxi_dw"
}
