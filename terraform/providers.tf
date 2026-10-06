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


#Picks the Cloud Translator: Tells Terraform which cloud provider plugin to download (AWS, GCP, Azure) to translate your code into real API calls.
#Pins Provider Versions: Locks the AWS plugin version (e.g., ~> 5.0) so automatic updates don't break existing code.
#Sets the Target Region: Specifies where AWS resources will be physically created (e.g., us-east-1).
#Applies Global Tags: Automatically attaches cost/ownership tags (Project = "UrbanPulse", Environment = "dev") to every resource created.
#Configures the Remote Backend: Tells Terraform to save its state file (.tfstate) in S3 and use DynamoDB for mutex locks instead of saving locally on your laptop.