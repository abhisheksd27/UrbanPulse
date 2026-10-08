# ==============================================================================
# File: terraform/modules/s3/variables.tf
# Role: Input Interface for S3 Storage Module
#
# WHAT THIS FILE DOES:
# Declares the required inputs that the parent module (terraform/main.tf)
# must provide when invoking the S3 storage module.
#
# HOW IT HELPS:
# Isolates S3 configuration so this module can be reused across different projects
# or environments without hardcoding naming prefixes.
#
# HOW IT CONNECTS:
# - Receives values from root variables (var.project_name, var.environment) in root main.tf.
# - Used in 'modules/s3/main.tf' to generate deterministic, unique bucket names.
# ==============================================================================

variable "project_name" {
  description = "Project name prefix for bucket naming (e.g., 'urbanpulse')"
  type        = string
}

variable "environment" {
  description = "Deployment environment suffix (e.g., 'dev', 'prod')"
  type        = string
}