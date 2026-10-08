# ==============================================================================
# File: terraform/modules/redshift/outputs.tf
# Role: Output Interface for Redshift Data Warehouse Module
#
# WHAT THIS FILE DOES:
# Exposes connection parameters (endpoint host, port, database name) generated
# after the cluster is successfully created.
#
# HOW IT HELPS:
# Eliminates guesswork: gives you the exact JDBC/ODBC connection endpoint URL
# needed by SQL clients and analytics engineering tools.
#
# HOW IT CONNECTS:
# - 'cluster_endpoint': Passed to root 'outputs.tf' -> used in Step 7 to configure dbt's profiles.yml.
# - 'database_name': Passed to root 'outputs.tf' -> database target for Step 5 DDL scripts.
# ==============================================================================

# Cluster Connection Endpoint (e.g., "urbanpulse-cluster-dev.xxx.us-east-1.redshift.amazonaws.com:5439")
output "cluster_endpoint" {
  description = "Connection endpoint string (host:port) for the Redshift cluster"
  value       = aws_redshift_cluster.main.endpoint
}

# Cluster Identifier (e.g., "urbanpulse-cluster-dev")
output "cluster_identifier" {
  description = "Unique identifier of the Redshift cluster (used for pause/resume commands)"
  value       = aws_redshift_cluster.main.cluster_identifier
}

# Database Name (e.g., "nyc_taxi_dw")
output "database_name" {
  description = "Name of the default analytical database inside the Redshift cluster"
  value       = aws_redshift_cluster.main.database_name
}

# Redshift Port (default 5439)
output "port" {
  description = "Port number on which the Redshift cluster listens for incoming connections"
  value       = aws_redshift_cluster.main.port
}