# ─────────────────────────────────────────────────────────────
# Terraform backend: store state in S3, lock in DynamoDB
# ─────────────────────────────────────────────────────────────


terraform {
    required_version = ">= 1.6"

    required_providers {
        aws = {
            source  = "hashicorp/aws"
            version = "~> 5.0"
        }
    }

    backend "s3" {
        bucket         = "nyc-taxi-tf-state-826674370922"
        key            = "terraform.tfstate"
        region         = "us-east-1"
        dynamodb_table = "terraform-lock-table"
        encrypt        = true
    }
}



# ─────────────────────────────────────────────────────────────
# AWS Provider: region + default tags applied to every resource
# ─────────────────────────────────────────────────────────────

provider "aws" {
    region = "us-east-1"

    default_tags {
        tags = {
            "Owner"       = "urbanPulse"
            Environment = "dev"
            ManagedBy   = "terraform"
            Owner       = "data-engineering"
        }
    }
}


