# ==============================================================================
# File: terraform/modules/emr/outputs.tf
# Role: Output Interface for the EMR Spark Module
#
# HOW IT CONNECTS:
#   → root outputs.tf: emr_cluster_id printed after tf-apply for use in Airflow DAGs
#   → Step 4 (Airflow): EmrAddStepsOperator needs the cluster ID to submit Spark steps
# ==============================================================================

# EMR Cluster ID — the primary identifier used by Airflow to submit and monitor Spark steps
# Example: "j-2AXXXXXXGAPLF"
output "cluster_id" {
  description = "EMR cluster ID — used by Airflow's EmrAddStepsOperator to submit Spark jobs"
  value       = aws_emr_cluster.main.id
}

# EMR Master node DNS name — used for SSH tunnelling to Spark UI or Ganglia dashboard
output "master_public_dns" {
  description = "Public DNS of EMR master node (accessible via SSH from VPC)"
  value       = aws_emr_cluster.main.master_public_dns
}

# EMR EC2 Instance Profile ARN — for reference/auditing
output "ec2_instance_profile_arn" {
  description = "IAM Instance Profile ARN attached to EMR EC2 nodes"
  value       = aws_iam_instance_profile.emr_ec2.arn
}

# EMR Service Role ARN — for reference/auditing
output "service_role_arn" {
  description = "IAM Service Role ARN used by the EMR cluster control plane"
  value       = aws_iam_role.emr_service.arn
}

# Master Security Group ID — used if Airflow or bastion needs to allow inbound from EMR
output "master_security_group_id" {
  description = "Security group ID of the EMR master node"
  value       = aws_security_group.emr_master.id
}
