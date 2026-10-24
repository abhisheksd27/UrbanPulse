# ==============================================================================
# File: terraform/modules/airflow/outputs.tf
# Role: Output Interface for the Airflow EC2 Module
#
# HOW IT CONNECTS:
#   → root outputs.tf: airflow_url printed after tf-apply for quick access
#   → Step 4 (Airflow): SSH into this host to deploy DAG files manually (if not using CI/CD)
# ==============================================================================

# Public Airflow URL — open in browser after tf-apply
output "airflow_url" {
  description = "Public URL of the Airflow webserver — open in browser after apply"
  value       = "http://${aws_eip.airflow.public_ip}:8080"
}

# Elastic IP — the stable public IP address of the Airflow host
output "public_ip" {
  description = "Elastic IP address of the Airflow EC2 host"
  value       = aws_eip.airflow.public_ip
}

# EC2 Instance ID — used for SSM Session Manager connections (SSH without opening port 22)
output "instance_id" {
  description = "EC2 instance ID of the Airflow host (use for SSM Session Manager)"
  value       = aws_instance.airflow.id
}

# SSH connection command — copy-paste to connect
output "ssh_command" {
  description = "SSH command to connect to the Airflow host"
  value       = "ssh -i ~/.ssh/urbanpulse ec2-user@${aws_eip.airflow.public_ip}"
}

# CloudWatch log group where Airflow container logs are streamed
output "log_group" {
  description = "CloudWatch log group for Airflow container logs"
  value       = aws_cloudwatch_log_group.airflow.name
}
