# ==============================================================================
# File: terraform/modules/emr/variables.tf
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
  description = "VPC ID where EMR cluster is launched"
  type        = string
}

variable "vpc_cidr" {
  description = "VPC CIDR block — used to restrict SSH to VPC-internal traffic"
  type        = string
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs — EMR master+core nodes are placed in private_subnet_ids[0]"
  type        = list(string)
}

variable "lake_bucket_arn" {
  description = "ARN of the S3 data lake bucket — grants Spark jobs read/write access"
  type        = string
}

variable "artifacts_bucket_arn" {
  description = "ARN of the S3 artifacts bucket — used for EMR logs and bootstrap scripts"
  type        = string
}

variable "artifacts_bucket_id" {
  description = "Name of the S3 artifacts bucket — used in log_uri path"
  type        = string
}

variable "emr_release_label" {
  description = "EMR release label — determines Hadoop, Spark, and YARN versions. Use emr-6.15.0 for Spark 3.4"
  type        = string
  default     = "emr-6.15.0"
}

variable "master_instance_type" {
  description = "EC2 instance type for the EMR master node (Spark driver). m5.xlarge for dev/prod."
  type        = string
  default     = "m5.xlarge"
}

variable "core_instance_type" {
  description = "EC2 instance type for EMR core/worker nodes (Spark executors)"
  type        = string
  default     = "m5.xlarge"
}

variable "core_instance_count" {
  description = "Number of core (executor) nodes. More nodes = more Spark parallelism."
  type        = number
  default     = 2
}
