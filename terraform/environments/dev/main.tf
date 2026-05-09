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

module "iam" {
  source  = "../../modules/iam"
  env     = var.env
  project = var.project
}

module "kms" {
  source      = "../../modules/kms"
  env         = var.env
  project     = var.project
  alert_email = var.alert_email
}

module "sns" {
  source      = "../../modules/sns"
  env         = var.env
  project     = var.project
  alert_email = var.alert_email
}

module "vpc" {
  source   = "../../modules/vpc"
  env      = var.env
  project  = var.project
  vpc_cidr = "10.0.0.0/16"
}

module "glue" {
  source        = "../../modules/glue"
  env           = var.env
  project       = var.project
  glue_role_arn = "arn:aws:iam::512584596315:role/fintech-glue-role-dev"
  bronze_bucket = "fintech-bronze-dev"
}
module "athena" {
  source         = "../../modules/athena"
  env            = var.env
  project        = var.project
  results_bucket = "fintech-athena-results-dev"
}

module "lake_formation" {
  source        = "../../modules/lake_formation"
  env           = var.env
  project       = var.project
  account_id    = "512584596315"
  glue_role_arn = "arn:aws:iam::512584596315:role/fintech-glue-role-dev"
}


variable "env" {
  default = "dev"
}

variable "project" {
  default = "fintech"
}

variable "alert_email" {
  default = "babudataarch@gmail.com"
}
