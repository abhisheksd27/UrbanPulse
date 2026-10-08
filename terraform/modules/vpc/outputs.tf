# ==============================================================================
# File: terraform/modules/vpc/outputs.tf
# Role: Output Interface for VPC Networking Module
#
# WHAT THIS FILE DOES:
# Exposes subnet IDs, VPC ID, and security group IDs to the root module.
#
# HOW IT HELPS:
# Lets downstream modules (Redshift, Kafka) attach to the exact private subnets and security
# groups created here without tight coupling.
#
# HOW IT CONNECTS:
# - 'private_subnet_ids': Passed to 'module.redshift.subnet_ids' to create the Redshift subnet group.
# - 'redshift_sg_id': Passed to 'module.redshift.security_group_ids' to guard port 5439.
# - 'public_subnet_ids', 'airflow_sg_id': Used in Step 4 when provisioning the Airflow host.
# - 'vpc_id': Passed to root outputs and used in Step 9 for MSK Kafka broker configuration.
# ==============================================================================

# VPC ID (e.g., "vpc-0abc12345678")
output "vpc_id" {
  description = "The ID of the VPC"
  value       = aws_vpc.main.id
}

# VPC CIDR Block (e.g., "10.0.0.0/16")
output "vpc_cidr" {
  description = "The CIDR block allocated to the VPC"
  value       = aws_vpc.main.cidr_block
}

# Private Subnet IDs ([subnet-xxx, subnet-yyy])
output "private_subnet_ids" {
  description = "List of private subnet IDs where Redshift and Kafka clusters are deployed"
  value       = [aws_subnet.private_a.id, aws_subnet.private_b.id]
}

# Public Subnet IDs ([subnet-aaa, subnet-bbb])
output "public_subnet_ids" {
  description = "List of public subnet IDs where Airflow and internet-facing bastions reside"
  value       = [aws_subnet.public_a.id, aws_subnet.public_b.id]
}

# Redshift Security Group ID
output "redshift_sg_id" {
  description = "ID of the security group protecting the Redshift cluster"
  value       = aws_security_group.redshift.id
}

# Airflow Security Group ID
output "airflow_sg_id" {
  description = "ID of the security group protecting the Airflow webserver"
  value       = aws_security_group.airflow.id
}