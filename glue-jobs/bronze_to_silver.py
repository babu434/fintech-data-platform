import sys
import hashlib
import boto3
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
    IntegerType, TimestampType
)

def get_spark():
    return SparkSession.builder\
        .appName("fintech-bronze-to-silver")\
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .getOrCreate()

def mask_pii(df):
    print("Masking PII columns...")
    df = df.withColumn(
        "email_masked",
        F.sha2(F.col("email"), 256)
    ).drop("email")

    df = df.withColumn(
        "phone_masked",
        F.sha2(F.col("phone"), 256)
    ).drop("phone")

    df = df.withColumn(
        "name_masked",
        F.sha2(F.col("name"), 256)
    ).drop("name")

    df = df.withColumn(
        "address_masked",
        F.sha2(F.col("address"), 256)
    ).drop("address")

    return df

def validate_transactions(df):
    print("Validating transactions...")
    good = df.filter(
        F.col("transaction_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        F.col("amount").isNotNull() &
        (F.col("amount") > 0) &
        F.col("timestamp").isNotNull()
    )

    bad = df.subtract(good)
    print(f"Good records: {good.count()}")
    print(f"Bad records:  {bad.count()}")

    return good, bad

def validate_customers(df):
    print("Validating customers...")
    good = df.filter(
        F.col("customer_id").isNotNull() &
        F.col("signup_date").isNotNull()
    )

    bad = df.subtract(good)
    print(f"Good records: {good.count()}")
    print(f"Bad records:  {bad.count()}")

    return good, bad

def deduplicate(df, key_col):
    print(f"Deduplicating on {key_col}...")
    before = df.count()
    df = df.dropDuplicates([key_col])
    after = df.count()
    print(f"Removed {before - after} duplicates")
    return df

def write_to_silver(df, path, partition_col=None):
    print(f"Writing to Silver: {path}")
    writer = df.write\
        .format("delta")\
        .mode("append")\
        .option("mergeSchema", "true")

    if partition_col:
        writer = writer.partitionBy(partition_col)

    writer.save(path)
    print(f"Silver write complete!")

def write_to_dlq(df, source, reason):
    if df.count() == 0:
        print(f"No bad records for {source}")
        return

    print(f"Writing {df.count()} bad records to DLQ...")
    df.withColumn("source", F.lit(source))\
      .withColumn("reason", F.lit(reason))\
      .withColumn("dlq_timestamp",
                  F.current_timestamp())\
      .write\
      .mode("append")\
      .parquet(f"s3://fintech-dlq-dev/{source}/")

    print(f"DLQ write complete!")

def process_transactions(spark, date_str):
    print(f"\n{'='*50}")
    print(f"Processing transactions for {date_str}")
    print(f"{'='*50}")

    year  = date_str[:4]
    month = date_str[5:7]
    day   = date_str[8:10]

    bronze_path = (
        f"s3://fintech-bronze-dev/transactions/"
        f"year={year}/month={month}/day={day}/"
    )

    print(f"Reading from: {bronze_path}")
    df = spark.read.parquet(bronze_path)
    print(f"Bronze rows: {df.count()}")

    df = deduplicate(df, "transaction_id")
    good, bad = validate_transactions(df)
    write_to_dlq(bad, "transactions", "validation_failed")

    good = good\
        .withColumn("processed_date",
                    F.current_date())\
        .withColumn("silver_timestamp",
                    F.current_timestamp())

    write_to_silver(
        good,
        "s3://fintech-silver-dev/transactions/",
        partition_col="transaction_date"
    )

def process_customers(spark):
    print(f"\n{'='*50}")
    print("Processing customers")
    print(f"{'='*50}")

    df = spark.read.parquet(
        "s3://fintech-bronze-dev/customers/customers.parquet"
    )
    print(f"Bronze rows: {df.count()}")

    df = deduplicate(df, "customer_id")
    good, bad = validate_customers(df)
    write_to_dlq(bad, "customers", "validation_failed")

    good = mask_pii(good)
    good = good\
        .withColumn("processed_date",
                    F.current_date())\
        .withColumn("silver_timestamp",
                    F.current_timestamp())

    write_to_silver(
        good,
        "s3://fintech-silver-dev/customers/"
    )

def process_kyc(spark):
    print(f"\n{'='*50}")
    print("Processing KYC")
    print(f"{'='*50}")

    df = spark.read.parquet(
        "s3://fintech-bronze-dev/kyc/kyc.parquet"
    )
    print(f"Bronze rows: {df.count()}")

    df = deduplicate(df, "customer_id")

    good = df.filter(
        F.col("customer_id").isNotNull() &
        F.col("kyc_status").isNotNull()
    )
    bad = df.subtract(good)
    write_to_dlq(bad, "kyc", "validation_failed")

    good = good\
        .withColumn("processed_date",
                    F.current_date())\
        .withColumn("silver_timestamp",
                    F.current_timestamp())

    write_to_silver(
        good,
        "s3://fintech-silver-dev/kyc/"
    )

def process_support_tickets(spark):
    print(f"\n{'='*50}")
    print("Processing support tickets")
    print(f"{'='*50}")

    df = spark.read.parquet(
        "s3://fintech-bronze-dev/"
        "support_tickets/support_tickets.parquet"
    )
    print(f"Bronze rows: {df.count()}")

    df = deduplicate(df, "ticket_id")

    good = df.filter(
        F.col("ticket_id").isNotNull() &
        F.col("customer_id").isNotNull()
    )
    bad = df.subtract(good)
    write_to_dlq(bad, "tickets", "validation_failed")

    good = good\
        .withColumn("processed_date",
                    F.current_date())\
        .withColumn("silver_timestamp",
                    F.current_timestamp())

    write_to_silver(
        good,
        "s3://fintech-silver-dev/support_tickets/"
    )

if __name__ == "__main__":
    print("Starting Bronze to Silver ETL...")
    print(f"Time: {datetime.now()}")

    spark = get_spark()

    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    process_transactions(spark, date_str)
    process_customers(spark)
    process_kyc(spark)
    process_support_tickets(spark)

    print("\nBronze to Silver ETL Complete!")
    print(f"Time: {datetime.now()}")

    spark.stop()
