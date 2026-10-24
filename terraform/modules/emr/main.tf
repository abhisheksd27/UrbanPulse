# ==============================================================================
# File: terraform/modules/emr/main.tf
# Role: Amazon EMR Cluster for PySpark Batch Processing
#
# WHAT THIS FILE DOES:
# Provisions an Amazon EMR (Elastic MapReduce) cluster for running the UrbanPulse
# PySpark batch jobs that transform raw SF taxi data (Bronze → Silver → Gold).
#
# Resources created:
#   1. aws_security_group (master)   — Firewall for the EMR master node
#   2. aws_security_group (core)     — Firewall for EMR core/worker nodes
#   3. aws_iam_role (emr_service)    — EMR service role (cluster management)
#   4. aws_iam_instance_profile      — Profile for EC2 nodes in the cluster
#   5. aws_emr_cluster               — The actual Spark cluster
#
# ARCHITECTURE DECISION — EMR vs Glue:
#   EMR gives full control over Spark version, instance types, cluster lifecycle,
#   and custom JARs. Glue is serverless but less flexible for complex jobs.
#   We use EMR for batch, and optionally Glue Data Catalog for metadata.
#
# HOW IT CONNECTS:
#   ← VPC module:  var.private_subnet_ids[0] — master+core nodes in private subnet
#   ← IAM module:  var.glue_role_arn, var.lake_bucket_arn — S3 access
#   → Airflow:     Step 4 DAGs use EmrAddStepsOperator to submit Spark jobs
#   → S3:          Jobs read from s3://lake/raw/ and write to s3://lake/staging/ + s3://lake/curated/
# ==============================================================================

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# ─────────────────────────────────────────────────────────────
# 1. EMR Master Node Security Group
# The master node is the Spark driver — it coordinates jobs.
# Only allow SSH from VPC CIDR (not public internet).
# ─────────────────────────────────────────────────────────────
resource "aws_security_group" "emr_master" {
  name        = "${var.project_name}-emr-master-sg-${var.environment}"
  description = "EMR master node — allows Spark UI (18080) and SSH (22) from VPC only"
  vpc_id      = var.vpc_id

  # SSH for debugging — accessible only from within VPC (via Airflow or bastion)
  ingress {
    description = "SSH from VPC"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  # Spark History Server UI — view completed job logs and DAG visualisations
  ingress {
    description = "Spark History Server UI"
    from_port   = 18080
    to_port     = 18080
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  # YARN ResourceManager UI — monitor running/pending Spark applications
  ingress {
    description = "YARN ResourceManager UI"
    from_port   = 8088
    to_port     = 8088
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    description = "Allow all outbound (S3, MSK, CloudWatch)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-emr-master-sg-${var.environment}" }
}

# ─────────────────────────────────────────────────────────────
# 2. EMR Core Node Security Group
# Core nodes are Spark executors — they do the actual data transformation work.
# Must communicate with the master node via ephemeral ports.
# ─────────────────────────────────────────────────────────────
resource "aws_security_group" "emr_core" {
  name        = "${var.project_name}-emr-core-sg-${var.environment}"
  description = "EMR core/worker nodes — internal cluster communication only"
  vpc_id      = var.vpc_id

  # SSH from VPC for debugging
  ingress {
    description = "SSH from VPC"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  # All traffic from master node (Spark driver → executor RPC)
  ingress {
    description = "All traffic from master security group"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true # Allows core nodes to talk to each other (shuffle traffic)
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-emr-core-sg-${var.environment}" }
}

# ─────────────────────────────────────────────────────────────
# 3. EMR Service Role
# The cluster-level role used by EMR control plane to provision
# EC2 instances, attach storage, and interact with AWS services.
# ─────────────────────────────────────────────────────────────
resource "aws_iam_role" "emr_service" {
  name = "${var.project_name}-emr-service-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# AWS-managed policy for EMR service role — covers EC2, CloudWatch, S3 bootstrap, etc.
resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

# ─────────────────────────────────────────────────────────────
# 4. EMR EC2 Instance Profile
# The node-level role assumed by every EC2 instance in the cluster.
# This is what the PySpark code uses when calling boto3 or s3a:// paths.
# ─────────────────────────────────────────────────────────────
resource "aws_iam_role" "emr_ec2" {
  name = "${var.project_name}-emr-ec2-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# EMR EC2 default policy — includes HDFS, YARN, Spark coordination permissions
resource "aws_iam_role_policy_attachment" "emr_ec2_default" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

# Custom S3 data lake policy — allows Spark jobs to read raw and write staging/curated
resource "aws_iam_policy" "emr_s3" {
  name        = "${var.project_name}-emr-s3-policy-${var.environment}"
  description = "Allows EMR Spark jobs to read/write the UrbanPulse S3 data lake"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:GetBucketLocation", "s3:ListBucket"
        ]
        Resource = [
          var.lake_bucket_arn,
          "${var.lake_bucket_arn}/*",
          var.artifacts_bucket_arn,
          "${var.artifacts_bucket_arn}/*"
        ]
      },
      {
        # Spark writes application logs to CloudWatch — essential for debugging failed jobs
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/urbanpulse/emr/*"
      },
      {
        # Allow EMR to read bootstrap action scripts from the artifacts bucket
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${var.artifacts_bucket_arn}/emr-bootstrap/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_s3" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = aws_iam_policy.emr_s3.arn
}

# IAM Instance Profile wraps the EC2 role so it can be attached to instances
resource "aws_iam_instance_profile" "emr_ec2" {
  name = "${var.project_name}-emr-ec2-profile-${var.environment}"
  role = aws_iam_role.emr_ec2.name
}

# ─────────────────────────────────────────────────────────────
# 5. EMR Cluster
# Runs Apache Spark for the bronze→silver and silver→gold batch jobs.
#
# Dev config:  1 master m5.xlarge + 2 core m5.xlarge on-demand
#              (~$0.192/hr per node — destroy after use)
# Prod config: 1 master m5.xlarge + 4 core m5.2xlarge (spot instances)
#              (~60-70% cheaper via spot with similar performance)
# ─────────────────────────────────────────────────────────────
resource "aws_emr_cluster" "main" {
  name          = "${var.project_name}-spark-${var.environment}"
  release_label = var.emr_release_label # e.g. "emr-6.15.0" includes Spark 3.4.1
  applications  = ["Spark", "Hadoop", "Ganglia"]

  # ── Networking ─────────────────────────────────────────────
  # Master and core nodes placed in the first private subnet
  # (single-AZ in dev; use instance_fleets for multi-AZ in prod)
  ec2_attributes {
    subnet_id                         = var.private_subnet_ids[0]
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_core.id
    instance_profile                  = aws_iam_instance_profile.emr_ec2.arn
  }

  service_role = aws_iam_role.emr_service.arn

  # ── Master Node ─────────────────────────────────────────────
  # Runs: Spark driver, YARN ResourceManager, HDFS NameNode
  master_instance_group {
    instance_type  = var.master_instance_type # m5.xlarge: 4 vCPU, 16 GiB — enough for driver
    instance_count = 1
    name           = "Master"
  }

  # ── Core Nodes (Spark Executors) ────────────────────────────
  # Run: YARN NodeManager, Spark executors, HDFS DataNode
  # More core nodes = more parallelism = faster Parquet processing
  core_instance_group {
    instance_type  = var.core_instance_type  # m5.xlarge: 4 vCPU, 16 GiB per executor
    instance_count = var.core_instance_count # 2 in dev, 4 in prod
    name           = "Core"

    # In dev: on-demand (reliable but costs more)
    # In prod: switch to spot via bid_price to save 60-70%
    # bid_price = "0.15"  # Uncomment for spot in prod
  }

  # ── Spark Configuration ─────────────────────────────────────
  # These settings are merged with EMR defaults to tune Spark for
  # our Parquet-heavy, S3-native workload.
  configurations_json = jsonencode([
    {
      # Spark core tuning
      Classification = "spark"
      Properties = {
        maximizeResourceAllocation = "true" # Use all available RAM and vCPUs per node
      }
    },
    {
      # Spark defaults per-application
      Classification = "spark-defaults"
      Properties = {
        # S3A magic committer: uses S3 atomic renames for safe Parquet writes
        "spark.hadoop.fs.s3a.committer.magic.enabled"               = "true"
        "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a" = "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"
        "spark.sql.sources.commitProtocolClass"                     = "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol"
        "spark.sql.parquet.output.committer.class"                  = "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter"

        # Optimise S3 reads: use vectorized reader for columnar Parquet files
        "spark.sql.parquet.filterPushdown"   = "true"
        "spark.sql.parquet.mergeSchema"      = "false" # Don't merge schemas — ours are fixed
        "spark.hadoop.fs.s3a.fast.upload"    = "true"  # Use multi-part upload for large files
        "spark.hadoop.fs.s3a.multipart.size" = "128M"

        # Serializer: Kryo is ~10x faster than Java default for shuffle operations
        "spark.serializer" = "org.apache.spark.serializer.KryoSerializer"

        # Adaptive Query Execution — automatically coalesces shuffle partitions
        "spark.sql.adaptive.enabled"                    = "true"
        "spark.sql.adaptive.coalescePartitions.enabled" = "true"
      }
    },
    {
      # YARN memory allocation: give more heap to containers for large DataFrames
      Classification = "yarn-site"
      Properties = {
        "yarn.nodemanager.vmem-check-enabled" = "false" # Avoid false OOM kills
        "yarn.nodemanager.pmem-check-enabled" = "false"
      }
    }
  ])

  # ── Logging ─────────────────────────────────────────────────
  # All YARN, Spark, and bootstrap logs streamed to S3 artifacts bucket.
  # Access via AWS Console: EMR → Cluster → Steps → View logs
  log_uri = "s3://${var.artifacts_bucket_id}/emr-logs/${var.environment}/"

  # ── Auto-Termination ────────────────────────────────────────
  # In dev: cluster auto-terminates after all Spark steps complete.
  # This prevents paying for an idle cluster overnight.
  # Set to false in prod where a long-running cluster is more efficient.
  keep_job_flow_alive_when_no_steps = var.environment == "prod" ? true : false

  # ── Tags ────────────────────────────────────────────────────
  tags = {
    Name        = "${var.project_name}-emr-${var.environment}"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}
