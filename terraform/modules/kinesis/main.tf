resource "aws_kinesis_stream" "transactions" {
  name             = "${var.project}-transactions-${var.env}"
  shard_count      = var.shard_count
  retention_period = 24

  shard_level_metrics = [
    "IncomingBytes",
    "OutgoingBytes",
    "IncomingRecords",
    "OutgoingRecords",
    "IteratorAgeMilliseconds"
  ]

  tags = {
    Environment = var.env
    Project     = var.project
  }
}

resource "aws_dynamodb_table" "fraud_decisions" {
  name           = "${var.project}-fraud-decisions-${var.env}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "transaction_id"

  attribute {
    name = "transaction_id"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = {
    Environment = var.env
    Project     = var.project
  }
}
