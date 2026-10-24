# ==============================================================================
# File: terraform/modules/msk/main.tf
# Role: Amazon MSK (Managed Streaming for Kafka) Cluster
#
# WHAT THIS FILE DOES:
# Provisions a managed Kafka cluster for the UrbanPulse real-time streaming pipeline.
# MSK handles broker management, patching, and replication automatically.
#
# HOW IT CONNECTS:
# - Step 8 (Streaming): Python event_producer.py writes to the MSK broker endpoints
# - Step 8 (Streaming): Spark Structured Streaming reads from MSK broker endpoints
# - VPC Module: MSK brokers are placed in the same private subnets as Redshift
#
# TODO: Implement in Step 8 (Streaming)
# ==============================================================================

# PLACEHOLDER — Implementation in Step 8
# Resources to add:
#   aws_msk_cluster          — The Kafka cluster
#   aws_msk_configuration    — Custom Kafka broker configuration
#   aws_security_group       — MSK-specific security group
