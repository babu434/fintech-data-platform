resource "aws_athena_workgroup" "analysts" {
  name        = "${var.project}-analysts-${var.env}"
  description = "Workgroup for business analysts"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.results_bucket}/analysts/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    bytes_scanned_cutoff_per_query = 1073741824
  }

  tags = {
    Environment = var.env
    Project     = var.project
    Team        = "analysts"
  }
}

resource "aws_athena_workgroup" "data_engineering" {
  name        = "${var.project}-data-engineering-${var.env}"
  description = "Workgroup for data engineering team"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.results_bucket}/data-engineering/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = {
    Environment = var.env
    Project     = var.project
    Team        = "data-engineering"
  }
}

resource "aws_athena_workgroup" "compliance" {
  name        = "${var.project}-compliance-${var.env}"
  description = "Workgroup for compliance team"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.results_bucket}/compliance/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = {
    Environment = var.env
    Project     = var.project
    Team        = "compliance"
  }
}

resource "aws_athena_named_query" "daily_fraud_summary" {
  name      = "daily-fraud-summary"
  workgroup = aws_athena_workgroup.analysts.name
  database  = "${var.project}_bronze_${var.env}"

  query = <<-EOT
    SELECT
      transaction_date,
      COUNT(transaction_id) as total_transactions,
      SUM(amount) as total_amount,
      SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count,
      ROUND(AVG(amount), 2) as avg_amount,
      ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1.0 ELSE 0 END) /
            COUNT(transaction_id) * 100, 2) as fraud_rate_pct
    FROM transactions
    GROUP BY transaction_date
    ORDER BY transaction_date DESC
  EOT
}

resource "aws_athena_named_query" "top_merchants" {
  name      = "top-merchants-by-volume"
  workgroup = aws_athena_workgroup.analysts.name
  database  = "${var.project}_bronze_${var.env}"

  query = <<-EOT
    SELECT
      merchant,
      category,
      COUNT(transaction_id) as transaction_count,
      ROUND(SUM(amount), 2) as total_amount,
      ROUND(AVG(amount), 2) as avg_amount,
      SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count
    FROM transactions
    GROUP BY merchant, category
    ORDER BY total_amount DESC
    LIMIT 10
  EOT
}

resource "aws_athena_named_query" "high_risk_customers" {
  name      = "high-risk-customers"
  workgroup = aws_athena_workgroup.analysts.name
  database  = "${var.project}_bronze_${var.env}"

  query = <<-EOT
    SELECT
      customer_id,
      total_transactions,
      total_spend,
      fraud_txn_count,
      fraud_rate,
      risk_tier,
      kyc_status,
      total_tickets
    FROM customer_360
    WHERE risk_tier = 'HIGH'
    OR fraud_rate > 10
    ORDER BY fraud_rate DESC
    LIMIT 100
  EOT
}
