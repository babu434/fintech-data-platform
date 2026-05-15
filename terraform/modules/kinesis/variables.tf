variable "env" {
  description = "Environment name"
  type        = string
}

variable "project" {
  description = "Project name"
  type        = string
  default     = "fintech"
}

variable "shard_count" {
  description = "Number of Kinesis shards"
  type        = number
  default     = 1
}
