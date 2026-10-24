# ==============================================================================
# File: terraform/modules/airflow/main.tf
# Role: Apache Airflow EC2 Host Instance
#
# WHAT THIS FILE DOES:
# Provisions an EC2 instance running Apache Airflow via Docker Compose.
# Uses the IAM instance profile from the IAM module for AWS service access.
#
# HOW IT CONNECTS:
# - Step 4 (Airflow): The DAGs in airflow/dags/ are deployed to this instance
# - Step 1 (IAM): Attaches the Airflow instance profile for S3/EMR/Redshift access
# - Step 1 (VPC): Placed in a public subnet with restricted inbound (port 8080)
#
# TODO: Implement in Step 4 (Orchestration)
# ==============================================================================

# PLACEHOLDER — Implementation in Step 4
# Resources to add:
#   aws_instance          — EC2 running Amazon Linux 2 with Docker
#   aws_security_group    — Allow port 8080 inbound, all outbound
#   aws_eip               — Elastic IP for stable Airflow UI URL
