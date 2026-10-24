# ==============================================================================
# File: terraform/modules/emr/main.tf
# Role: Amazon EMR (Elastic MapReduce) Cluster for PySpark
#
# WHAT THIS FILE DOES:
# Provisions an EMR cluster to run the PySpark batch processing jobs.
# EMR handles Spark cluster management, YARN, and HDFS configuration.
#
# HOW IT CONNECTS:
# - Step 3 (PySpark): Airflow submits Spark steps to this cluster via EmrAddStepsOperator
# - Step 1 (IAM): Attaches the Glue/EMR service role for S3 access
# - Step 1 (VPC): Placed in private subnets for security
#
# TODO: Implement in Step 3 (PySpark Processing)
# ==============================================================================

# PLACEHOLDER — Implementation in Step 3
# Resources to add:
#   aws_emr_cluster          — The Spark cluster (master + core nodes)
#   aws_security_group       — EMR master and core node security groups
#   aws_emr_instance_fleet   — Spot instance fleet for cost optimisation
