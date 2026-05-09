variable "env" {
  description = "Environment name"
  type        = string
}

variable "project" {
  description = "Project name"
  type        = string
  default     = "fintech"
}

variable "account_id" {
  description = "AWS Account ID"
  type        = string
}

variable "glue_role_arn" {
  description = "Glue ETL role ARN"
  type        = string
}
