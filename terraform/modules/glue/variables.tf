variable "env" {
  description = "Environment name: dev, qa, prod"
  type        = string
}

variable "project" {
  description = "Project name"
  type        = string
  default     = "fintech"
}

variable "glue_role_arn" {
  description = "IAM role ARN for Glue"
  type        = string
}

variable "bronze_bucket" {
  description = "Bronze S3 bucket name"
  type        = string
}
