resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project}-bronze-${var.env}"
}

resource "aws_s3_bucket" "silver" {
  bucket = "${var.project}-silver-${var.env}"
}

resource "aws_s3_bucket" "gold" {
  bucket = "${var.project}-gold-${var.env}"
}

resource "aws_s3_bucket" "dlq" {
  bucket = "${var.project}-dlq-${var.env}"
}

resource "aws_s3_bucket_versioning" "bronze_versioning" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze_lifecycle" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    id     = "move-to-cheaper-storage"
    status = "Enabled"

    filter {
      prefix = ""
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}
