terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "ap-south-1"
}

module "s3" {
  source  = "../../modules/s3"
  env     = var.env
  project = var.project
}

variable "env" {
  default = "dev"
}

variable "project" {
  default = "fintech"
}
module "iam" {
  source  = "../../modules/iam"
  env     = var.env
  project = var.project
}
