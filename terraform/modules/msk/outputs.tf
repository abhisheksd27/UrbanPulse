# ==============================================================================
# File: terraform/modules/msk/outputs.tf
# Role: Output Interface for the MSK Kafka Module
#
# HOW IT CONNECTS:
#   → root outputs.tf:          msk_bootstrap_brokers_tls printed after tf-apply
#   → Step 2 (Streaming):       Python event_producer reads KAFKA_BOOTSTRAP_SERVERS from .env
#   → Step 3 (Spark Streaming): Spark job config uses bootstrap brokers to connect to Kafka
#   → Step 4 (Airflow):         DAGs retrieve these to pass to Spark job parameters
# ==============================================================================

# TLS-encrypted broker endpoints (comma-separated) — use for all production connections
# Example: "b-1.urbanpulse.xxxx.kafka.us-east-1.amazonaws.com:9094,b-2...."
output "bootstrap_brokers_tls" {
  description = "TLS-encrypted Kafka broker connection string for producers and consumers"
  value       = aws_msk_cluster.main.bootstrap_brokers_tls
  sensitive   = false # Not secret — just hostnames and ports
}

# Plaintext broker endpoints — only available if TLS_PLAINTEXT mode is set (dev only)
output "bootstrap_brokers" {
  description = "Plaintext Kafka broker connection string (dev only — empty in prod)"
  value       = aws_msk_cluster.main.bootstrap_brokers
  sensitive   = false
}

# MSK Cluster ARN — used by IAM policies and CloudWatch alarms
output "cluster_arn" {
  description = "Amazon Resource Name of the MSK Kafka cluster"
  value       = aws_msk_cluster.main.arn
}

# MSK Security Group ID — needed if other services need to allow MSK egress
output "msk_security_group_id" {
  description = "Security group ID of the MSK brokers"
  value       = aws_security_group.msk.id
}

# Zookeeper connect string — needed for some Kafka admin tools and older clients
output "zookeeper_connect_string" {
  description = "ZooKeeper connection string for Kafka admin operations"
  value       = aws_msk_cluster.main.zookeeper_connect_string
}
