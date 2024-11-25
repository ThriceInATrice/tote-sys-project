terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket         = "state-bucket-nov-11"
    dynamodb_table = "terraform-state-lock-dynamo"
    key            = "data-engineering-project-state/terraform.tfstate"
    region         = "eu-west-2"
  }
}
provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName  = "Data Engineering Project"
      DeployedFrom = "Terraform"
      Repository   = "tote-sys-project"
      Authors      = "Joanna Link/Iqra Farooq/Louis Smith/Rory Smith/Grace Arnup/Maria McNulty"
      Environment  = "dev"
    }
  }
}
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
