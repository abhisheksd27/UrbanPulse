# ==============================================================================
# File: terraform/modules/msk/main.tf
# Role: Amazon MSK (Managed Streaming for Apache Kafka) Cluster
#
# WHAT THIS FILE DOES:
# Provisions a fully managed, highly available Apache Kafka cluster for the
# UrbanPulse real-time streaming pipeline (Step 8).
#
# Resources created:
#   1. aws_security_group      — Firewall: allows Kafka broker ports 9092/9094
#   2. aws_msk_configuration   — Custom Kafka broker config (topic defaults, retention)
#   3. aws_msk_cluster         — 2-broker Kafka cluster in private subnets
#   4. aws_iam_policy          — Allows Airflow EC2 to describe MSK and get broker endpoints
#   5. aws_iam_role_policy_attachment — Attaches MSK policy to Airflow role
#
# HOW IT CONNECTS TO OTHER MODULES:
#   ← VPC module:      Uses var.private_subnet_ids (2 AZs) and var.vpc_id
#   ← IAM module:      Uses var.airflow_role_name to attach MSK describe policy
#   → root outputs.tf: Exports msk_bootstrap_brokers_tls for Python and Spark to connect
#
# HOW IT CONNECTS TO PIPELINE STEPS:
#   Step 2 (Streaming):  Python event_producer.py connects to bootstrap_brokers_tls
#   Step 3 (Streaming):  Spark Structured Streaming reads from bootstrap_brokers_tls
#   Step 4 (Airflow):    Airflow monitors MSK consumer lag via CloudWatch metrics
# ==============================================================================

# ─────────────────────────────────────────────────────────────
# 1. MSK Security Group
# Grants access on Kafka's plaintext (9092) and TLS-encrypted (9094) ports
# from within the VPC only — no public internet access.
# ─────────────────────────────────────────────────────────────
resource "aws_security_group" "msk" {
  name        = "${var.project_name}-msk-sg-${var.environment}"
  description = "Security group for MSK Kafka brokers — VPC-internal access only"
  vpc_id      = var.vpc_id

  # Kafka plaintext port — used internally for low-latency intra-VPC producers/consumers
  ingress {
    description = "Kafka plaintext (port 9092) from VPC"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  # Kafka TLS port — used by all external clients (Python producer, Spark consumer)
  ingress {
    description = "Kafka TLS (port 9094) from VPC"
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  # Zookeeper port — MSK brokers coordinate via ZK internally (within broker SG)
  ingress {
    description = "ZooKeeper port — MSK broker internal communication"
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-msk-sg-${var.environment}" }
}

# ─────────────────────────────────────────────────────────────
# 2. MSK Broker Configuration
# Custom server.properties settings applied to every Kafka broker.
# These tune data retention, replication, and log compaction for
# our taxi + weather streaming topics.
# ─────────────────────────────────────────────────────────────
resource "aws_msk_configuration" "main" {
  name           = "${var.project_name}-msk-config-${var.environment}"
  kafka_versions = [var.kafka_version]
  description    = "UrbanPulse Kafka broker configuration"

  server_properties = <<-EOF
    # ── Topic Defaults ─────────────────────────────────────────
    # New topics (created by producers) default to 3 partitions.
    # More partitions = more parallelism when consuming with Spark.
    num.partitions=3

    # Replicate every message across all brokers (resilience).
    # With 2 brokers, replication.factor=2 means no data loss on single broker failure.
    default.replication.factor=2

    # Minimum replicas that must be in-sync before a PRODUCE is acknowledged.
    # With min.insync.replicas=2 and acks=all, no message can be lost.
    min.insync.replicas=2

    # ── Data Retention ─────────────────────────────────────────
    # Keep messages for 7 days (604800000 ms) — long enough for batch replay if needed.
    log.retention.ms=604800000

    # Cap each partition log at 1 GiB. Old segments are deleted when this is exceeded.
    log.retention.bytes=1073741824

    # Each log segment file is capped at 512 MiB.
    log.segment.bytes=536870912

    # ── Performance ────────────────────────────────────────────
    # Allow leaders to report messages fetched from replicas.
    # Required by consumer groups that need guaranteed ordering.
    replica.fetch.max.bytes=1048576

    # Auto-create topics when a producer publishes to a non-existent topic.
    # Convenient for dev; set to false in production.
    auto.create.topics.enable=true
  EOF
}

# ─────────────────────────────────────────────────────────────
# 3. MSK Kafka Cluster
# A 2-broker cluster (one per AZ) for high availability.
# Dev uses kafka.t3.small (cheapest) — upgrade to kafka.m5.large for prod.
# ─────────────────────────────────────────────────────────────
resource "aws_msk_cluster" "main" {
  cluster_name           = "${var.project_name}-msk-${var.environment}"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.number_of_broker_nodes # 2 for multi-AZ HA

  broker_node_group_info {
    instance_type = var.broker_instance_type # kafka.t3.small for dev

    # Place one broker in each private subnet (one per AZ) for fault tolerance
    client_subnets  = var.private_subnet_ids
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        # Each broker's local disk for segment storage
        # 20 GiB is sufficient for 7-day retention of our SF taxi volume
        volume_size = var.broker_volume_size_gb
      }
    }
  }

  # ── Encryption ─────────────────────────────────────────────
  # Enforce TLS-only between clients and brokers (no plaintext in production).
  # In dev we allow plaintext for local Docker testing convenience.
  encryption_info {
    encryption_in_transit {
      client_broker = var.environment == "prod" ? "TLS" : "TLS_PLAINTEXT"
      in_cluster    = true # Always encrypt broker-to-broker traffic
    }
  }

  # ── Monitoring ─────────────────────────────────────────────
  # PER_BROKER level exposes per-topic/partition metrics to CloudWatch.
  # These power our consumer lag alerting (Step 10 — Governance).
  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }

  broker_logs {
    cloudwatch_logs {
      enabled   = true
      log_group = "/urbanpulse/msk/${var.environment}"
    }
    s3 {
      enabled = true
      bucket  = var.artifacts_bucket_id
      prefix  = "msk-logs/${var.environment}/"
    }
  }

  # ── Custom Configuration ────────────────────────────────────
  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  tags = { Name = "${var.project_name}-msk-${var.environment}" }
}

# ─────────────────────────────────────────────────────────────
# 4. IAM Policy — Allow Airflow to discover MSK broker endpoints
# Airflow DAGs need to read the broker connection string at runtime
# to pass it to the Spark Structured Streaming job config.
# ─────────────────────────────────────────────────────────────
resource "aws_iam_policy" "airflow_msk" {
  name        = "${var.project_name}-airflow-msk-policy-${var.environment}"
  description = "Allows Airflow EC2 to describe MSK cluster and retrieve broker endpoints"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["kafka:DescribeCluster", "kafka:GetBootstrapBrokers", "kafka:ListClusters"]
      Resource = [aws_msk_cluster.main.arn]
    }]
  })
}

# Attach the MSK describe policy to the Airflow EC2 IAM role (created in IAM module)
resource "aws_iam_role_policy_attachment" "airflow_msk" {
  role       = var.airflow_role_name
  policy_arn = aws_iam_policy.airflow_msk.arn
}
