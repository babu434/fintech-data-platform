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

variable "env" {
  default = "dev"
}

variable "project" {
  default = "fintech"
}

variable "alert_email" {
  default = "babudataarch@gmail.com"
}
