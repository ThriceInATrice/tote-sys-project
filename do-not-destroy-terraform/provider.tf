terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName  = "Data Engineering Project"
      Description = "Outside of main terraform infrastructure"
      DeployedFrom = "Terraform"
      Repository   = "tote-sys-project"
      Authors      = "Joanna Link/Iqra Farooq/Louis Smith/Rory Smith/Grace Arnup/Maria McNulty"
      Environment  = "dev"
    }
  }
}
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
