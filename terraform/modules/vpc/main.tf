# ==============================================================================
# File: terraform/modules/vpc/main.tf
# Role: VPC Networking, Subnet Isolation & S3 VPC Gateway Endpoint
#
# WHAT THIS FILE DOES:
# 1. Provisions an isolated VPC network (10.0.0.0/16).
# 2. Creates 4 subnets across 2 Availability Zones:
#    - 2 Public Subnets (10.0.1.0/24, 10.0.2.0/24): for Airflow webserver / Bastion.
#    - 2 Private Subnets (10.0.10.0/24, 10.0.11.0/24): for Redshift & Kafka (Step 9).
# 3. Creates an Internet Gateway (IGW) for public subnets.
# 4. Creates a NAT Gateway + Elastic IP for private outbound connectivity.
# 5. Configures Route Tables separating public vs private outbound traffic.
# 6. Provisions Security Groups for Redshift (port 5439) and Airflow (port 8080).
# 7. Configures an S3 VPC Gateway Endpoint (FREE): routes traffic from private subnets
#    directly to S3 without passing through the NAT Gateway or public internet!
#
# HOW IT HELPS:
# - Defense-in-Depth: Redshift has no public IP and cannot be reached from the public internet.
# - High Performance & Cost Reduction: S3 VPC Gateway Endpoint transfers gigabytes of Parquet
#   data between S3 and Redshift over AWS internal fiber with zero NAT data-processing fees ($0.045/GB).
# - High Availability: Spans 2 Availability Zones, fulfilling the Redshift subnet group requirement.
#
# HOW IT CONNECTS:
# - Connects to Redshift Module: Outputs 'private_subnet_ids' and 'redshift_sg_id' to 'module.redshift'
#   in root 'main.tf'.
# - Connects to Step 4 (Airflow): Outputs 'public_subnet_ids' and 'airflow_sg_id' for Airflow EC2 hosting.
# - Connects to Step 9 (Kafka/MSK): MSK brokers will be launched inside 'private_subnet_ids'.
# ==============================================================================

# Dynamic query to discover active Availability Zones in the current region
data "aws_availability_zones" "available" {
  state = "available"
}

# Dynamic query for the current region name
data "aws_region" "current" {}

# ─────────────────────────────────────────────────────────────
# 1. VPC Container
# ─────────────────────────────────────────────────────────────
resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true # Required for Redshift cluster DNS endpoint resolution
  enable_dns_support   = true

  tags = { Name = "${var.project_name}-vpc-${var.environment}" }
}

# ─────────────────────────────────────────────────────────────
# 2. Subnets (Multi-AZ Architecture)
# ─────────────────────────────────────────────────────────────

# Public Subnet in AZ A (e.g. us-east-1a)
resource "aws_subnet" "public_a" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true # Instances launched here receive public IPv4 addresses

  tags = { Name = "${var.project_name}-public-a-${var.environment}" }
}

# Public Subnet in AZ B (e.g. us-east-1b)
resource "aws_subnet" "public_b" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = data.aws_availability_zones.available.names[1]
  map_public_ip_on_launch = true

  tags = { Name = "${var.project_name}-public-b-${var.environment}" }
}

# Private Subnet in AZ A (Redshift compute node slice placement)
resource "aws_subnet" "private_a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.10.0/24"
  availability_zone = data.aws_availability_zones.available.names[0]

  tags = { Name = "${var.project_name}-private-a-${var.environment}" }
}

# Private Subnet in AZ B (Redshift multi-node / Kafka replica placement)
resource "aws_subnet" "private_b" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.11.0/24"
  availability_zone = data.aws_availability_zones.available.names[1]

  tags = { Name = "${var.project_name}-private-b-${var.environment}" }
}

# ─────────────────────────────────────────────────────────────
# 3. Internet Gateway (Public Subnet Inbound & Outbound)
# ─────────────────────────────────────────────────────────────
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.project_name}-igw-${var.environment}" }
}

# ─────────────────────────────────────────────────────────────
# 4. NAT Gateway & Elastic IP (Private Subnet Outbound Only)
# ─────────────────────────────────────────────────────────────
# Allows private resources (like Redshift downloading updates) outbound access
# without permitting incoming connections from the outside internet.
resource "aws_eip" "nat" {
  domain = "vpc"
  tags   = { Name = "${var.project_name}-nat-eip-${var.environment}" }
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public_a.id # NAT gateway resides in a public subnet

  tags = { Name = "${var.project_name}-nat-${var.environment}" }

  depends_on = [aws_internet_gateway.main]
}

# ─────────────────────────────────────────────────────────────
# 5. Route Tables & Associations
# ─────────────────────────────────────────────────────────────

# Public Route Table: routes 0.0.0.0/0 to the Internet Gateway
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = { Name = "${var.project_name}-public-rt-${var.environment}" }
}

# Private Route Table: routes 0.0.0.0/0 to the NAT Gateway
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main.id
  }

  tags = { Name = "${var.project_name}-private-rt-${var.environment}" }
}

# Bind Public Subnets to Public Route Table
resource "aws_route_table_association" "public_a" {
  subnet_id      = aws_subnet.public_a.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "public_b" {
  subnet_id      = aws_subnet.public_b.id
  route_table_id = aws_route_table.public.id
}

# Bind Private Subnets to Private Route Table
resource "aws_route_table_association" "private_a" {
  subnet_id      = aws_subnet.private_a.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "private_b" {
  subnet_id      = aws_subnet.private_b.id
  route_table_id = aws_route_table.private.id
}

# ─────────────────────────────────────────────────────────────
# 6. Security Groups (Virtual Stateful Firewalls)
# ─────────────────────────────────────────────────────────────

# Redshift Security Group: permits inbound SQL traffic on port 5439 strictly from inside the VPC
resource "aws_security_group" "redshift" {
  name        = "${var.project_name}-redshift-sg-${var.environment}"
  description = "Allow Redshift port 5439 access from within the VPC"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Redshift SQL port from VPC CIDR only"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr] # Airflow, Spark, and bastion inside VPC can connect
  }

  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-redshift-sg-${var.environment}" }
}

# Airflow Security Group: permits HTTP access to the Airflow Webserver UI (port 8080)
resource "aws_security_group" "airflow" {
  name        = "${var.project_name}-airflow-sg-${var.environment}"
  description = "Allow Airflow UI access on port 8080"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Airflow webserver UI"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Restrict to your IP (var.my_ip_cidr) in production
  }

  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-airflow-sg-${var.environment}" }
}

# ─────────────────────────────────────────────────────────────
# 7. S3 VPC Gateway Endpoint (Architecture Gold Standard)
# ─────────────────────────────────────────────────────────────
# Establishes a direct private route inside the AWS network connecting the VPC to S3.
# WHY THIS MATTERS:
# 1. Cost: Gateway endpoints are 100% FREE (unlike Interface endpoints).
# 2. Speed: Traffic doesn't traverse the NAT Gateway, eliminating NAT data transfer costs.
# 3. Security: All Redshift COPY queries fetch S3 Parquet files over private AWS lines.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.s3"
  vpc_endpoint_type = "Gateway"

  # Injects the S3 route directly into both public and private route tables
  route_table_ids = [aws_route_table.private.id, aws_route_table.public.id]

  tags = { Name = "${var.project_name}-s3-endpoint-${var.environment}" }
}