resource "aws_kms_key" "pii_key" {
  description             = "${var.project}-pii-encryption-${var.env}"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = {
    Name        = "${var.project}-pii-key-${var.env}"
    Environment = var.env
    Project     = var.project
    Purpose     = "PII encryption for GDPR compliance"
  }
}

resource "aws_kms_alias" "pii_key_alias" {
  name          = "alias/${var.project}-pii-${var.env}"
  target_key_id = aws_kms_key.pii_key.key_id
}
