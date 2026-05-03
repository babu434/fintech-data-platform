variable "env" {
  description = "Environment name: dev, qa, prod"
  type        = string
}

variable "project" {
  description = "Project name"
  type        = string
  default     = "fintech"
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.0.0.0/16"
}
