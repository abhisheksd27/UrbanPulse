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


#Acts as Function Arguments: Defines inputs so your infrastructure code isn't hardcoded.
#Enables Multi-Environment Deployments: Lets you reuse the exact same code for dev, staging, and prod just by changing variable values.
#Enforces Type Safety: Restricts inputs to valid types (string, number, list, bool) to catch mistakes before deploying.
#Sets Safe Defaults: Provides fallback values so the code works out-of-the-box if no value is explicitly passed.
#Adds Validation Guardrails: Rejects invalid inputs before running (e.g., throws an error if environment is not dev, staging, or prod).
#Protects Sensitive Data: Marks passwords/keys with sensitive = true so they are never printed in clear text in terminal logs or CLI outputs.