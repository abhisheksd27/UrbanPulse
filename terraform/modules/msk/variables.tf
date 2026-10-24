# ==============================================================================
# File: terraform/modules/msk/variables.tf
# Role: Input Variables for the MSK Kafka Module
# ==============================================================================

variable "project_name" {
  description = "Project name prefix for all resource names"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev | staging | prod)"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID — MSK security group is attached to this VPC"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR block — used to restrict Kafka port ingress to VPC-internal traffic"
  type        = string
}

variable "private_subnet_ids" {
  description = "List of 2 private subnet IDs (one per AZ) — each broker lands in one subnet"
  type        = list(string)
}

variable "artifacts_bucket_id" {
  description = "Artifacts S3 bucket name — MSK broker logs are streamed here"
  type        = string
}

variable "airflow_role_name" {
  description = "IAM Role name of the Airflow EC2 instance — MSK describe policy is attached here"
  type        = string
}

variable "kafka_version" {
  description = "Apache Kafka version to run on MSK brokers"
  type        = string
  default     = "3.5.1"
}

variable "number_of_broker_nodes" {
  description = "Number of Kafka broker nodes — must equal the number of AZs used (min 2 for HA)"
  type        = number
  default     = 2

  validation {
    condition     = var.number_of_broker_nodes >= 2
    error_message = "MSK requires at least 2 broker nodes for high availability across AZs."
  }
}

variable "broker_instance_type" {
  description = "EC2 instance type for each Kafka broker. Use kafka.t3.small for dev, kafka.m5.large for prod"
  type        = string
  default     = "kafka.t3.small"
}

variable "broker_volume_size_gb" {
  description = "EBS storage per broker in GiB — size for log retention within the configured period"
  type        = number
  default     = 20
}
