# ==============================================================================
# File: terraform/modules/airflow/variables.tf
# ==============================================================================

variable "project_name" {
  description = "Project name prefix"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev | staging | prod)"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID where the Airflow EC2 instance is launched"
  type        = string
}

variable "public_subnet_ids" {
  description = "List of public subnet IDs — Airflow is placed in public_subnet_ids[0] for UI access"
  type        = list(string)
}

variable "airflow_sg_id" {
  description = "Security group ID from VPC module — allows port 8080 inbound"
  type        = string
}

variable "instance_profile_name" {
  description = "IAM instance profile name from IAM module — grants Airflow S3/EMR/MSK permissions"
  type        = string
}

variable "artifacts_bucket_id" {
  description = "Artifacts S3 bucket name — Airflow reads DAGs and writes logs here"
  type        = string
}

variable "ssh_public_key" {
  description = "SSH public key content (e.g. contents of ~/.ssh/urbanpulse.pub) for EC2 access"
  type        = string
  sensitive   = false
  default     = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIREPLACE_WITH_YOUR_PUBLIC_KEY urbanpulse"
}

variable "instance_type" {
  description = "EC2 instance type for the Airflow host. t3.large (2 vCPU, 8 GiB) for dev, t3.xlarge for prod"
  type        = string
  default     = "t3.large"
}

variable "airflow_version" {
  description = "Apache Airflow Docker image version tag"
  type        = string
  default     = "2.8.0"
}
