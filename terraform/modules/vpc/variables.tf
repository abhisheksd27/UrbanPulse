# ==============================================================================
# File: terraform/modules/vpc/variables.tf
# Role: Input Interface for VPC Networking Module
#
# WHAT THIS FILE DOES:
# Declares the network configuration variables required to construct the VPC,
# subnets, route tables, and S3 Gateway Endpoint.
#
# HOW IT HELPS:
# Enables customizing IP address space (CIDR block) per environment
# (e.g., dev on 10.0.0.0/16, prod on 10.1.0.0/16) to prevent overlapping IP ranges in corporate networks.
#
# HOW IT CONNECTS:
# - 'project_name', 'environment', 'vpc_cidr': Passed from root 'variables.tf' via root 'main.tf'.
# - 'lake_bucket_id': Passed from 'module.s3.lake_bucket_id' to configure endpoint routing.
# ==============================================================================

variable "project_name" {
  description = "Project name prefix for network resource naming"
  type        = string
}

variable "environment" {
  description = "Deployment environment suffix (dev, staging, prod)"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR IP range (e.g., 10.0.0.0/16 provides 65,536 private IP addresses)"
  type        = string
  default     = "10.0.0.0/16"
}

variable "lake_bucket_id" {
  description = "S3 data lake bucket ID used for VPC endpoint resource association"
  type        = string
}