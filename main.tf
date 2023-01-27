terraform {
  required_version = ">= 0.12"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
  backend "s3" {
    bucket = "tarn-datalake-tf-state-433046906551"
    key    = "aws-buckets/project-mod1/desafio/terraform.tfstate"
    region = "us-east-1"
  }
}

# Configure the AWS Provider
provider "aws" {
  region = var.location

  default_tags {
    tags = {
      owner      = "${local.common_tags.owner}"
      managed_by = "${local.common_tags.managed_by}"
      project    = "${local.common_tags.project}"
    }
  }
}