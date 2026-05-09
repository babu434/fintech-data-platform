resource "aws_lakeformation_resource" "bronze_bucket" {
  arn = "arn:aws:s3:::${var.project}-bronze-${var.env}"
}

resource "aws_lakeformation_resource" "silver_bucket" {
  arn = "arn:aws:s3:::${var.project}-silver-${var.env}"
}

resource "aws_lakeformation_resource" "gold_bucket" {
  arn = "arn:aws:s3:::${var.project}-gold-${var.env}"
}

resource "aws_lakeformation_permissions" "glue_role_bronze" {
  principal   = var.glue_role_arn
  permissions = ["ALL"]

  database {
    name = "${var.project}_bronze_${var.env}"
  }
}

resource "aws_lakeformation_permissions" "glue_role_silver" {
  principal   = var.glue_role_arn
  permissions = ["ALL"]

  database {
    name = "${var.project}_silver_${var.env}"
  }
}

resource "aws_lakeformation_permissions" "glue_role_gold" {
  principal   = var.glue_role_arn
  permissions = ["ALL"]

  database {
    name = "${var.project}_gold_${var.env}"
  }
}
