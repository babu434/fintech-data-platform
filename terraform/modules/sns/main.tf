resource "aws_sns_topic" "alerts" {
  name = "${var.project}-alerts-${var.env}"

  tags = {
    Name        = "${var.project}-alerts-${var.env}"
    Environment = var.env
    Project     = var.project
  }
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_sns_topic" "gdpr_notifications" {
  name = "${var.project}-gdpr-${var.env}"

  tags = {
    Name        = "${var.project}-gdpr-${var.env}"
    Environment = var.env
    Project     = var.project
  }
}

resource "aws_sns_topic_subscription" "gdpr_email" {
  topic_arn = aws_sns_topic.gdpr_notifications.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
