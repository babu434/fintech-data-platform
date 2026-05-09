variable "env" {
  description = "Environment name"
  type        = string
}

variable "project" {
  description = "Project name"
  type        = string
  default     = "fintech"
}

variable "results_bucket" {
  description = "S3 bucket for Athena query results"
  type        = string
}
