# ==============================================================================
# File: terraform/modules/airflow/main.tf
# Role: Apache Airflow Host on EC2
#
# WHAT THIS FILE DOES:
# Provisions an EC2 instance running Apache Airflow via Docker Compose.
# Airflow is the orchestration hub — it schedules and monitors every pipeline step.
#
# Resources created:
#   1. data.aws_ami                — Finds the latest Amazon Linux 2023 AMI automatically
#   2. aws_key_pair                — Uploads an SSH public key for secure access
#   3. aws_eip                     — Elastic IP for a stable, unchanging Airflow URL
#   4. aws_instance                — The EC2 host running Airflow via Docker Compose
#   5. aws_eip_association         — Binds the Elastic IP to the EC2 instance
#   6. aws_cloudwatch_log_group    — CloudWatch log group for Airflow container logs
#
# HOW IT CONNECTS:
#   ← VPC module:  var.public_subnet_ids[0] — placed in public subnet for UI access
#   ← VPC module:  var.airflow_sg_id — security group allowing port 8080
#   ← IAM module:  var.instance_profile_name — grants Airflow S3/EMR/MSK/SSM permissions
#   → Airflow UI:  http://<elastic_ip>:8080 (admin/admin on first boot)
#   → Step 4:      DAG files are deployed to this instance via CI/CD (GitHub Actions)
# ==============================================================================

data "aws_region" "current" {}

# ─────────────────────────────────────────────────────────────
# 1. AMI Lookup — Latest Amazon Linux 2023
# Amazon Linux 2023 ships with Docker and SSM agent pre-installed.
# This data source always finds the latest AMI so we don't hardcode IDs.
# ─────────────────────────────────────────────────────────────
data "aws_ami" "amazon_linux_2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# ─────────────────────────────────────────────────────────────
# 2. SSH Key Pair
# Upload your local public key so you can SSH into the Airflow host for debugging.
# Generate with: ssh-keygen -t ed25519 -f ~/.ssh/urbanpulse
# ─────────────────────────────────────────────────────────────
resource "aws_key_pair" "airflow" {
  key_name   = "${var.project_name}-airflow-key-${var.environment}"
  public_key = var.ssh_public_key

  tags = { Name = "${var.project_name}-airflow-key-${var.environment}" }
}

# ─────────────────────────────────────────────────────────────
# 3. Elastic IP (Stable URL for Airflow UI)
# Without an EIP, the instance gets a new public IP on every restart.
# EIP guarantees the Airflow URL (http://<EIP>:8080) never changes.
# ─────────────────────────────────────────────────────────────
resource "aws_eip" "airflow" {
  domain = "vpc"
  tags   = { Name = "${var.project_name}-airflow-eip-${var.environment}" }
}

# ─────────────────────────────────────────────────────────────
# 4. EC2 Instance — Airflow Host
# Runs Airflow 2.x via Docker Compose with:
#   - Webserver (port 8080)
#   - Scheduler (continuously evaluating DAG trigger conditions)
#   - PostgreSQL metadata DB (container)
#   - LocalExecutor (tasks run directly on this instance)
#
# User data script runs on first boot:
#   1. Installs Docker + Docker Compose
#   2. Writes the .env file with AWS environment values
#   3. Clones the DAG files from the S3 artifacts bucket
#   4. Starts Airflow via docker-compose up -d
# ─────────────────────────────────────────────────────────────
resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.instance_type # t3.large: 2 vCPU 8 GiB — sufficient for LocalExecutor
  subnet_id              = var.public_subnet_ids[0]
  vpc_security_group_ids = [var.airflow_sg_id]
  iam_instance_profile   = var.instance_profile_name
  key_name               = aws_key_pair.airflow.key_name

  # Root volume: 30 GiB is enough for Docker images + Airflow logs
  root_block_device {
    volume_type           = "gp3"
    volume_size           = 30
    delete_on_termination = true
    encrypted             = true
  }

  # ── Bootstrap Script ───────────────────────────────────────
  # This shell script runs once on the first boot of the instance.
  # It installs Docker, pulls images, and starts Airflow.
  user_data = base64encode(templatefile("${path.module}/scripts/bootstrap_airflow.sh", {
    project_name        = var.project_name
    environment         = var.environment
    aws_region          = data.aws_region.current.name
    s3_artifacts_bucket = var.artifacts_bucket_id
    airflow_version     = var.airflow_version
    fernet_key_param    = "/${var.project_name}/${var.environment}/airflow/fernet_key"
    db_password_param   = "/${var.project_name}/${var.environment}/airflow/db_password"
  }))

  tags = {
    Name        = "${var.project_name}-airflow-${var.environment}"
    Role        = "airflow-orchestrator"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ─────────────────────────────────────────────────────────────
# 5. Elastic IP Association
# Binds the stable EIP to the EC2 instance.
# ─────────────────────────────────────────────────────────────
resource "aws_eip_association" "airflow" {
  instance_id   = aws_instance.airflow.id
  allocation_id = aws_eip.airflow.id
}

# ─────────────────────────────────────────────────────────────
# 6. CloudWatch Log Group for Airflow Container Logs
# Docker containers running Airflow write logs to CloudWatch via the awslogs driver.
# Log group persists even if the instance is replaced or rebuilt.
# ─────────────────────────────────────────────────────────────
resource "aws_cloudwatch_log_group" "airflow" {
  name              = "/urbanpulse/airflow/${var.environment}"
  retention_in_days = 30 # Keep 30 days of Airflow task logs

  tags = { Name = "${var.project_name}-airflow-logs-${var.environment}" }
}
