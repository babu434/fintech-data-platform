variable "env" {
  description = "Environment name: dev, qa, prod"
  type        = string
}

variable "project" {
  description = "Project name"
  type        = string
  default     = "fintech"
}

variable "alert_email" {
  description = "Email address for pipeline alerts"
  type        = string
}
