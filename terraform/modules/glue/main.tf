resource "aws_glue_catalog_database" "bronze" {
  name        = "${var.project}_bronze_${var.env}"
  description = "Bronze layer raw data catalog"
}

resource "aws_glue_catalog_database" "silver" {
  name        = "${var.project}_silver_${var.env}"
  description = "Silver layer cleaned data catalog"
}

resource "aws_glue_catalog_database" "gold" {
  name        = "${var.project}_gold_${var.env}"
  description = "Gold layer business data catalog"
}

resource "aws_glue_crawler" "transactions" {
  name          = "${var.project}-transactions-crawler-${var.env}"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.bronze.name
  description   = "Crawls transaction data in Bronze S3"

  s3_target {
    path = "s3://${var.bronze_bucket}/transactions/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_NEW_FOLDERS_ONLY"
  }

  schedule = "cron(0 1 * * ? *)"

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
      Tables = {
        AddOrUpdateBehavior = "MergeNewColumns"
      }
    }
  })

  tags = {
    Environment = var.env
    Project     = var.project
  }
}

resource "aws_glue_crawler" "customers" {
  name          = "${var.project}-customers-crawler-${var.env}"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.bronze.name
  description   = "Crawls customer data in Bronze S3"

  s3_target {
    path = "s3://${var.bronze_bucket}/customers/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "LOG"
  }

  schedule = "cron(0 1 * * ? *)"

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Tables = {
        AddOrUpdateBehavior = "MergeNewColumns"
      }
    }
  })

  tags = {
    Environment = var.env
    Project     = var.project
  }
}

resource "aws_glue_crawler" "kyc" {
  name          = "${var.project}-kyc-crawler-${var.env}"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.bronze.name
  description   = "Crawls KYC data in Bronze S3"

  s3_target {
    path = "s3://${var.bronze_bucket}/kyc/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "LOG"
  }

  schedule = "cron(0 1 * * ? *)"

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Tables = {
        AddOrUpdateBehavior = "MergeNewColumns"
      }
    }
  })

  tags = {
    Environment = var.env
    Project     = var.project
  }
}

resource "aws_glue_crawler" "support_tickets" {
  name          = "${var.project}-tickets-crawler-${var.env}"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.bronze.name
  description   = "Crawls support ticket data in Bronze S3"

  s3_target {
    path = "s3://${var.bronze_bucket}/support_tickets/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "LOG"
  }

  schedule = "cron(0 1 * * ? *)"

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Tables = {
        AddOrUpdateBehavior = "MergeNewColumns"
      }
    }
  })

  tags = {
    Environment = var.env
    Project     = var.project
  }
}
resource "aws_glue_job" "bronze_to_silver" {
  name         = "${var.project}-bronze-to-silver-${var.env}"
  role_arn     = var.glue_role_arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${var.bronze_bucket}/scripts/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"            = "python"
    "--enable-metrics"          = "true"
    "--enable-job-insights"     = "true"
    "--enable-glue-datacatalog" = "true"
    "--datalake-formats"        = "delta"
    "--TempDir"                 = "s3://${var.bronze_bucket}/temp/"
    "--conf"                    = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  number_of_workers = 5
  worker_type       = "G.1X"
  timeout           = 60

  tags = {
    Environment = var.env
    Project     = var.project
  }
}
resource "aws_glue_job" "silver_to_gold" {
  name         = "${var.project}-silver-to-gold-${var.env}"
  role_arn     = var.glue_role_arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${var.bronze_bucket}/scripts/silver_to_gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"            = "python"
    "--enable-metrics"          = "true"
    "--enable-job-insights"     = "true"
    "--enable-glue-datacatalog" = "true"
    "--datalake-formats"        = "delta"
    "--TempDir"                 = "s3://${var.bronze_bucket}/temp/"
    "--conf"                    = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  number_of_workers = 5
  worker_type       = "G.1X"
  timeout           = 60

  tags = {
    Environment = var.env
    Project     = var.project
  }
}
resource "aws_glue_job" "gdpr_batch_deletion" {
  name         = "${var.project}-gdpr-batch-deletion-${var.env}"
  role_arn     = var.glue_role_arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${var.bronze_bucket}/scripts/gdpr_batch_deletion.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"              = "python"
    "--enable-metrics"            = "true"
    "--enable-job-insights"       = "true"
    "--datalake-formats"          = "delta"
    "--TempDir"                   = "s3://${var.bronze_bucket}/temp/"
    "--conf"                      = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    "--additional-python-modules" = "boto3"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  number_of_workers = 5
  worker_type       = "G.1X"
  timeout           = 60

  tags = {
    Environment = var.env
    Project     = var.project
  }
}
