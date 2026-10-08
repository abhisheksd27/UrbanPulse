# ==============================================================================
# File: terraform/variables.tf
# Role: Root Input Variables & Validation Contracts
#
# WHAT THIS FILE DOES:
# Declares the global parameters accepted by the UrbanPulse infrastructure.
# It functions just like function arguments in Python:
# def build_platform(aws_region="us-east-1", environment="dev", project_name="urbanpulse", ...):
#
# HOW IT HELPS:
# 1. Parameterization: Lets us deploy to 'dev', 'staging', or 'prod' without altering code.
# 2. Validation: 'validation' blocks reject invalid inputs before any AWS resources are created.
# 3. Security: 'sensitive = true' redacts passwords from CLI outputs and logs.
#
# HOW IT CONNECTS:
# - Supplies values to 'providers.tf' (for region and default tags).
# - Passed directly to child modules in 'main.tf':
#   * s3 module: gets 'project_name', 'environment'
#   * iam module: gets 'project_name', 'environment'
#   * vpc module: gets 'project_name', 'environment'
#   * redshift module: gets 'project_name', 'environment', 'redshift_master_password'
# ==============================================================================

# 1. AWS Region where all infrastructure will be provisioned
variable "aws_region" {
  description = "The AWS region to deploy resources in"
  type        = string
  default     = "us-east-1"
}

# 2. Deployment stage: enforces strict adherence to known stages
variable "environment" {
  description = "The environment for the deployment (e.g., dev, staging, prod)"
  type        = string
  default     = "dev"

  # Safety check: blocks accidental typos like "devel" or "production"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

# 3. Project Name: used as the prefix for naming resources across AWS.
# CRITICAL: Keep strictly lowercase because S3 bucket names reject uppercase letters.
variable "project_name" {
  description = "The name of the project (lowercase for S3 compatibility)"
  type        = string
  default     = "urbanpulse"
}

# 4. Master password for the Redshift data warehouse cluster.
# 'sensitive = true' ensures Terraform never displays this secret in plan/apply terminal output.
variable "redshift_master_password" {
  description = "The master password for the Redshift cluster"
  type        = string
  sensitive   = true
}

# 5. Developer IP address in CIDR format (e.g., "198.51.100.24/32")
# Used to restrict ingress security group access to your specific machine.
variable "my_ip_cidr" {
  description = "The CIDR block for your current IP address to allow access to resources"
  type        = string
  default     = "0.0.0.0/0"
}