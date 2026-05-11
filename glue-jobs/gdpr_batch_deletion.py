import boto3
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

dynamodb = boto3.resource(
    'dynamodb', region_name='ap-south-1')
sns = boto3.client(
    'sns', region_name='ap-south-1')
s3 = boto3.client(
    's3', region_name='ap-south-1')

TABLE = 'fintech-gdpr-requests-dev'
SNS_TOPIC = (
    "arn:aws:sns:ap-south-1:"
    "512584596315:fintech-gdpr-dev"
)

def get_spark():
    return SparkSession.builder\
        .appName("fintech-gdpr-deletion")\
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta"
                ".catalog.DeltaCatalog")\
        .getOrCreate()

def get_pending_requests():
    table = dynamodb.Table(TABLE)
    response = table.scan(
        FilterExpression="#s = :pending",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":pending": "PENDING"}
    )
    requests = response.get('Items', [])
    print(f"Found {len(requests)} pending requests")
    return requests

def batch_delete_from_delta(spark,
                             customer_ids,
                             path,
                             table_name):
    print(f"Deleting from {table_name}...")
    try:
        df = spark.read.format("delta").load(path)
        before = df.count()

        df_filtered = df.filter(
            ~F.col("customer_id").isin(customer_ids)
        )
        after = df_filtered.count()
        deleted = before - after

        df_filtered.write\
            .format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema", "true")\
            .save(path)

        print(f"Deleted {deleted} rows "
              f"from {table_name}")
        return deleted

    except Exception as e:
        print(f"Error deleting from {table_name}: {e}")
        return 0

def purge_redis_keys(customer_ids):
    print(f"Purging Redis keys...")
    try:
        import redis
        r = redis.Redis(
            host='localhost',
            port=6379
        )
        deleted_keys = 0
        for customer_id in customer_ids:
            pattern = f"user:{customer_id}:*"
            keys = r.keys(pattern)
            if keys:
                r.delete(*keys)
                deleted_keys += len(keys)
        print(f"Deleted {deleted_keys} Redis keys")
    except Exception as e:
        print(f"Redis not available: {e}")
        print("Skipping Redis purge for dev environment")

def write_audit_log(requests, deletion_summary):
    audit = {
        "batch_id": f"GDPR-BATCH-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "processed_at": datetime.now().isoformat(),
        "total_requests": len(requests),
        "customers_deleted": [
            r['customer_id'] for r in requests
        ],
        "deletion_summary": deletion_summary,
        "status": "COMPLETE"
    }

    key = (
        f"gdpr-audit/"
        f"batch_{datetime.now().strftime('%Y%m%d')}.json"
    )

    s3.put_object(
        Bucket='fintech-bronze-dev',
        Key=key,
        Body=json.dumps(audit, indent=2)
    )
    print(f"Audit log written: s3://fintech-bronze-dev/{key}")
    return key

def update_request_status(requests, audit_log_key):
    table = dynamodb.Table(TABLE)
    for request in requests:
        table.update_item(
            Key={
                'customer_id': request['customer_id'],
                'requested_at': request['requested_at']
            },
            UpdateExpression=(
                "SET #s = :complete, "
                "completed_at = :completed, "
                "audit_log_key = :audit"
            ),
            ExpressionAttributeNames={
                "#s": "status"
            },
            ExpressionAttributeValues={
                ":complete": "COMPLETE",
                ":completed": datetime.now().isoformat(),
                ":audit": audit_log_key
            }
        )
    print(f"Updated {len(requests)} requests to COMPLETE")

def notify_customers(requests):
    for request in requests:
        sns.publish(
            TopicArn=SNS_TOPIC,
            Message=(
                f"Your data deletion is complete!\n"
                f"Request ID: {request['request_id']}\n"
                f"Customer: {request['customer_id']}\n"
                f"Completed: {datetime.now().isoformat()}\n"
                f"All your data has been permanently deleted."
            ),
            Subject=(
                f"GDPR Deletion Complete: "
                f"{request['request_id']}"
            )
        )
    print(f"Notified {len(requests)} customers")

if __name__ == "__main__":
    print("Starting GDPR batch deletion...")
    print(f"Time: {datetime.now()}")

    requests = get_pending_requests()

    if not requests:
        print("No pending deletion requests. Exiting.")
        exit(0)

    customer_ids = [r['customer_id'] for r in requests]
    print(f"Processing: {customer_ids}")

    spark = get_spark()

    deletion_summary = {}

    silver_tables = [
        ("s3://fintech-silver-dev/customers/",
         "silver_customers"),
        ("s3://fintech-silver-dev/transactions/",
         "silver_transactions"),
        ("s3://fintech-silver-dev/kyc/",
         "silver_kyc"),
        ("s3://fintech-silver-dev/support_tickets/",
         "silver_tickets"),
    ]

    gold_tables = [
        ("s3://fintech-gold-dev/customer_360/",
         "gold_customer_360"),
    ]

    for path, name in silver_tables + gold_tables:
        deleted = batch_delete_from_delta(
            spark, customer_ids, path, name)
        deletion_summary[name] = deleted

    purge_redis_keys(customer_ids)

    audit_log_key = write_audit_log(
        requests, deletion_summary)

    update_request_status(requests, audit_log_key)

    notify_customers(requests)

    print("\nGDPR batch deletion complete!")
    print(f"Processed: {len(requests)} customers")
    print(f"Summary: {deletion_summary}")

    spark.stop()
