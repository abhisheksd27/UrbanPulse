variable "aws_region" {
  description = "The AWS region to deploy resources in"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "The environment for the deployment (e.g., dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "project_name" {
  description = "The name of the project"
  type        = string
  default     = "urbanPulse"
}

variable "redshift_master_password" {
  description = "The master password for the Redshift cluster"
  type        = string
  sensitive   = true
}

variable "my_ip_cidr"{
    description = "The CIDR block for your current IP address to allow access to resources"
    type        = string
    
}