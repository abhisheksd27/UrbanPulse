# ==============================================================================
# File: terraform/providers.tf
# Role: Cloud Provider & State Backend Configuration
# 
# WHAT THIS FILE DOES:
# 1. Configures the Terraform engine requirements (minimum version & provider plugins).
# 2. Configures the Remote Backend in S3 + DynamoDB (stores state securely in the cloud).
# 3. Configures the AWS Provider plugin (sets region and global tags).
#
# HOW IT HELPS:
# - Version Pinning (~> 5.0): Prevents automatic upgrades from introducing breaking changes.
# - Remote State (S3 + DynamoDB): Prevents state loss if your laptop dies, and stops
#   concurrent team members from corrupting state via DynamoDB distributed locking.
# - Default Tags: Automatically attaches billing/metadata tags to EVERY resource,
#   allowing exact cost tracking in AWS Cost Explorer filtered by Project = "urbanpulse".
#
# HOW IT CONNECTS:
# - Reads 'var.aws_region', 'var.project_name', and 'var.environment' from 'variables.tf'.
# - Provides the authenticated AWS API connection used by ALL child modules (s3, iam, vpc, redshift).
# ==============================================================================

terraform {
  # Require Terraform CLI version 1.6 or higher
  required_version = ">= 1.6"

  # Declare required provider plugins to download from the Terraform Registry
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0" # Allows 5.x updates, blocks breaking 6.0 releases
    }
  }

  # ─────────────────────────────────────────────────────────────
  # Remote Backend: State File Storage & State Locking
  # ─────────────────────────────────────────────────────────────
  # Stores the infrastructure 'memory' (.tfstate) in an encrypted S3 bucket
  # rather than on local disk. DynamoDB acts as a mutex lock during applies.
  backend "s3" {
    bucket         = "nyc-taxi-tf-state-826674370922"
    key            = "terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-lock-table" # Mutex lock table
    encrypt        = true                   # AES256 encryption at rest
  }
}

# ─────────────────────────────────────────────────────────────
# AWS Provider Configuration
# ─────────────────────────────────────────────────────────────
# Translates your declarative Terraform code into real AWS REST API requests.
provider "aws" {
  region = var.aws_region # Target deployment region (e.g. us-east-1)

  # Universal tags applied automatically to every resource created by this project
  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
      Owner       = "data-engineering"
    }
  }
}