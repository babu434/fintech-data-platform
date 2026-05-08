import sys
import boto3
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def get_spark():
    return SparkSession.builder\
        .appName("fintech-silver-to-gold")\
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta"
                ".catalog.DeltaCatalog")\
        .getOrCreate()

def read_silver(spark, table):
    path = f"s3://fintech-silver-dev/{table}/"
    print(f"Reading Silver: {path}")
    df = spark.read.format("delta").load(path)
    print(f"Rows: {df.count()}")
    return df

def write_gold(df, table, partition_col=None):
    path = f"s3://fintech-gold-dev/{table}/"
    print(f"Writing Gold: {path}")

    writer = df.write\
        .format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", "true")

    if partition_col:
        writer = writer.partitionBy(partition_col)

    writer.save(path)
    print(f"Gold write complete: {df.count()} rows")

def build_transaction_summary(txn_df):
    print("\nBuilding transaction summary...")

    summary = txn_df.groupBy("customer_id").agg(
        F.count("transaction_id")
         .alias("total_transactions"),
        F.sum("amount")
         .alias("total_spend"),
        F.avg("amount")
         .alias("avg_transaction"),
        F.max("amount")
         .alias("max_transaction"),
        F.min("amount")
         .alias("min_transaction"),
        F.min("transaction_date")
         .alias("first_transaction_date"),
        F.max("transaction_date")
         .alias("last_transaction_date"),
        F.countDistinct("transaction_date")
         .alias("active_days"),
        F.sum(F.when(F.col("is_fraud")==1, 1)
              .otherwise(0))
         .alias("fraud_txn_count"),
        F.sum(F.when(F.col("is_fraud")==0, 1)
              .otherwise(0))
         .alias("normal_txn_count"),
        F.countDistinct("merchant")
         .alias("unique_merchants"),
        F.countDistinct("category")
         .alias("unique_categories"),
        F.countDistinct("device_id")
         .alias("unique_devices"),
        F.max("location_lat")
         .alias("last_location_lat"),
        F.max("location_lng")
         .alias("last_location_lng")
    )

    summary = summary.withColumn(
        "fraud_rate",
        F.round(
            F.col("fraud_txn_count") /
            F.col("total_transactions") * 100,
            2
        )
    ).withColumn(
        "txn_per_active_day",
        F.round(
            F.col("total_transactions") /
            F.col("active_days"),
            2
        )
    )

    print(f"Transaction summary rows: {summary.count()}")
    return summary

def build_ticket_summary(tickets_df):
    print("\nBuilding ticket summary...")

    summary = tickets_df.groupBy("customer_id").agg(
        F.count("ticket_id")
         .alias("total_tickets"),
        F.sum(F.when(F.col("status")=="OPEN", 1)
              .otherwise(0))
         .alias("open_tickets"),
        F.sum(F.when(F.col("status")=="CLOSED", 1)
              .otherwise(0))
         .alias("closed_tickets"),
        F.sum(F.when(F.col("status")=="PENDING", 1)
              .otherwise(0))
         .alias("pending_tickets"),
        F.sum(F.when(
            F.col("category")=="fraud_report", 1)
            .otherwise(0))
         .alias("fraud_reports"),
        F.sum(F.when(
            F.col("category")=="transaction_dispute", 1)
            .otherwise(0))
         .alias("dispute_count")
    )

    print(f"Ticket summary rows: {summary.count()}")
    return summary

def build_customer_360(spark):
    print("\n" + "="*50)
    print("Building Customer 360")
    print("="*50)

    txn_df      = read_silver(spark, "transactions")
    cust_df     = read_silver(spark, "customers")
    kyc_df      = read_silver(spark, "kyc")
    tickets_df  = read_silver(spark, "support_tickets")

    txn_summary     = build_transaction_summary(txn_df)
    ticket_summary  = build_ticket_summary(tickets_df)

    print("\nJoining all tables...")
    customer_360 = cust_df\
        .join(kyc_df.select(
            "customer_id",
            "kyc_status",
            "risk_tier",
            "pan_verified",
            "aadhar_verified",
            "kyc_date",
            "kyc_expiry"
        ), on="customer_id", how="left")\
        .join(txn_summary,
              on="customer_id", how="left")\
        .join(ticket_summary,
              on="customer_id", how="left")

    customer_360 = customer_360\
        .withColumn(
            "updated_at",
            F.current_timestamp())\
        .withColumn(
            "batch_date",
            F.current_date())\
        .withColumn(
            "is_current",
            F.lit(True))\
        .withColumn(
            "valid_from",
            F.current_date())\
        .withColumn(
            "valid_to",
            F.lit("9999-12-31").cast("date"))

    customer_360 = customer_360\
        .fillna({
            "total_transactions": 0,
            "total_spend": 0.0,
            "fraud_txn_count": 0,
            "total_tickets": 0,
            "open_tickets": 0,
            "fraud_rate": 0.0
        })

    print(f"\nCustomer 360 rows: {customer_360.count()}")
    print(f"Columns: {len(customer_360.columns)}")

    write_gold(customer_360, "customer_360")
    return customer_360

def build_daily_aggregates(spark, date_str):
    print("\n" + "="*50)
    print(f"Building daily aggregates for {date_str}")
    print("="*50)

    txn_df = read_silver(spark, "transactions")

    daily = txn_df\
        .filter(F.col("transaction_date") == date_str)\
        .groupBy(
            "transaction_date",
            "category",
            "merchant"
        ).agg(
            F.count("transaction_id")
             .alias("transaction_count"),
            F.sum("amount")
             .alias("total_amount"),
            F.avg("amount")
             .alias("avg_amount"),
            F.sum(F.when(F.col("is_fraud")==1, 1)
                  .otherwise(0))
             .alias("fraud_count"),
            F.sum(F.when(F.col("is_fraud")==1,
                         F.col("amount"))
                  .otherwise(0))
             .alias("fraud_amount")
        )\
        .withColumn(
            "fraud_rate",
            F.round(
                F.col("fraud_count") /
                F.col("transaction_count") * 100,
                2
            )
        )\
        .withColumn(
            "processed_at",
            F.current_timestamp()
        )

    print(f"Daily aggregates rows: {daily.count()}")
    write_gold(
        daily,
        "daily_aggregates",
        partition_col="transaction_date"
    )

def build_regulatory_report(spark, date_str):
    print("\n" + "="*50)
    print(f"Building regulatory report for {date_str}")
    print("="*50)

    txn_df = read_silver(spark, "transactions")

    daily_txn = txn_df\
        .filter(F.col("transaction_date") == date_str)

    report = daily_txn.agg(
        F.count("transaction_id")
         .alias("total_transactions"),
        F.sum("amount")
         .alias("total_amount"),
        F.sum(F.when(F.col("is_fraud")==1, 1)
              .otherwise(0))
         .alias("flagged_transactions"),
        F.sum(F.when(F.col("is_fraud")==1,
                     F.col("amount"))
              .otherwise(0))
         .alias("fraud_amount"),
        F.countDistinct("customer_id")
         .alias("unique_customers"),
        F.countDistinct("merchant")
         .alias("unique_merchants"),
        F.avg("amount")
         .alias("avg_transaction_amount"),
        F.max("amount")
         .alias("max_transaction_amount")
    )\
    .withColumn(
        "report_date",
        F.lit(date_str)
    )\
    .withColumn(
        "fraud_rate_pct",
        F.round(
            F.col("flagged_transactions") /
            F.col("total_transactions") * 100,
            2
        )
    )\
    .withColumn(
        "generated_at",
        F.current_timestamp()
    )\
    .withColumn(
        "report_status",
        F.lit("COMPLETE")
    )

    print(f"Regulatory report generated!")
    write_gold(
        report,
        "regulatory_reports",
        partition_col="report_date"
    )

if __name__ == "__main__":
    print("Starting Silver to Gold ETL...")
    print(f"Time: {datetime.now()}")

    spark = get_spark()

    try:
        from awsglue.utils import getResolvedOptions
        args = getResolvedOptions(sys.argv, ['date'])
        date_str = args['date']
        print(f"Using provided date: {date_str}")
    except Exception:
        yesterday = datetime.now() - timedelta(days=1)
        date_str = yesterday.strftime("%Y-%m-%d")
        print(f"Using yesterday: {date_str}")

    build_customer_360(spark)
    build_daily_aggregates(spark, date_str)
    build_regulatory_report(spark, date_str)

    print("\nSilver to Gold ETL Complete!")
    print(f"Time: {datetime.now()}")

    spark.stop()
