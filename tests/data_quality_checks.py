import boto3
import pandas as pd
import io
import json
import sys
from datetime import datetime, timedelta

s3 = boto3.client('s3', region_name='ap-south-1')
sns = boto3.client('sns', region_name='ap-south-1')

SNS_TOPIC = (
    "arn:aws:sns:ap-south-1:"
    "512584596315:fintech-alerts-dev"
)

BRONZE_BUCKET = "fintech-bronze-dev"
SILVER_BUCKET = "fintech-silver-dev"

def read_parquet_files(bucket, prefix):
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )

    if 'Contents' not in response:
        return None

    parquet_files = [
        obj['Key'] for obj in response['Contents']
        if obj['Key'].endswith('.parquet')
        and 'delta_log' not in obj['Key']
    ]

    if not parquet_files:
        return None

    dfs = []
    for key in parquet_files:
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(
            io.BytesIO(obj['Body'].read()))
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)

def send_alert(subject, message):
    sns.publish(
        TopicArn=SNS_TOPIC,
        Message=message,
        Subject=subject
    )
    print(f"Alert sent: {subject}")

class QualityCheck:
    def __init__(self, name):
        self.name = name
        self.passed = 0
        self.failed = 0
        self.results = []

    def check(self, condition, description,
              actual=None, expected=None):
        if condition:
            self.passed += 1
            status = "PASS ✅"
        else:
            self.failed += 1
            status = "FAIL ❌"

        result = {
            "check": description,
            "status": status,
            "actual": actual,
            "expected": expected
        }
        self.results.append(result)
        print(f"  {status} {description}")
        if actual is not None:
            print(f"         actual={actual} "
                  f"expected={expected}")

    def summary(self):
        total = self.passed + self.failed
        print(f"\n{self.name} Summary:")
        print(f"  Passed: {self.passed}/{total}")
        print(f"  Failed: {self.failed}/{total}")
        return self.failed == 0

def check_bronze_transactions(date_str):
    print(f"\n{'='*50}")
    print(f"Checking Bronze Transactions: {date_str}")
    print(f"{'='*50}")

    year  = date_str[:4]
    month = date_str[5:7]
    day   = date_str[8:10]

    prefix = (
        f"transactions/year={year}/"
        f"month={month}/day={day}/"
    )

    df = read_parquet_files(BRONZE_BUCKET, prefix)
    qc = QualityCheck("Bronze Transactions")

    qc.check(
        df is not None,
        "Data exists for date",
        actual=date_str
    )

    if df is None:
        qc.summary()
        return False

    required_columns = [
        'transaction_id', 'customer_id',
        'amount', 'merchant', 'category',
        'timestamp', 'is_fraud', 'currency'
    ]

    for col in required_columns:
        qc.check(
            col in df.columns,
            f"Column exists: {col}"
        )

    qc.check(
        len(df) >= 1000,
        "Minimum row count",
        actual=len(df),
        expected=">=1000"
    )

    qc.check(
        df['transaction_id'].isnull().sum() == 0,
        "No nulls on transaction_id",
        actual=df['transaction_id'].isnull().sum(),
        expected=0
    )

    qc.check(
        df['customer_id'].isnull().sum() == 0,
        "No nulls on customer_id",
        actual=df['customer_id'].isnull().sum(),
        expected=0
    )

    qc.check(
        df['amount'].isnull().sum() == 0,
        "No nulls on amount",
        actual=df['amount'].isnull().sum(),
        expected=0
    )

    qc.check(
        (df['amount'] > 0).all(),
        "Amount always positive",
        actual=f"{(df['amount'] <= 0).sum()} negative",
        expected="0 negative"
    )

    qc.check(
        df['transaction_id'].nunique() == len(df),
        "No duplicate transaction_ids",
        actual=len(df) - df['transaction_id'].nunique(),
        expected="0 duplicates"
    )

    fraud_rate = df['is_fraud'].mean() * 100
    qc.check(
        0 < fraud_rate < 20,
        "Fraud rate within expected range",
        actual=f"{fraud_rate:.2f}%",
        expected="0-20%"
    )

    return qc.summary()

def check_silver_transactions(date_str):
    print(f"\n{'='*50}")
    print(f"Checking Silver Transactions: {date_str}")
    print(f"{'='*50}")

    prefix = (
        f"transactions/"
        f"transaction_date={date_str}/"
    )

    df = read_parquet_files(SILVER_BUCKET, prefix)
    qc = QualityCheck("Silver Transactions")

    qc.check(
        df is not None,
        "Silver data exists for date",
        actual=date_str
    )

    if df is None:
        qc.summary()
        return False

    qc.check(
        len(df) >= 1000,
        "Minimum row count in Silver",
        actual=len(df),
        expected=">=1000"
    )

    pii_columns = ['email', 'phone', 'name', 'address']
    for col in pii_columns:
        qc.check(
            col not in df.columns,
            f"PII column removed: {col}"
        )

    qc.check(
        df['transaction_id'].isnull().sum() == 0,
        "No nulls on transaction_id in Silver",
        actual=df['transaction_id'].isnull().sum(),
        expected=0
    )

    qc.check(
        'processed_date' in df.columns,
        "processed_date column exists"
    )

    qc.check(
        'silver_timestamp' in df.columns,
        "silver_timestamp column exists"
    )

    return qc.summary()

def check_silver_customers():
    print(f"\n{'='*50}")
    print("Checking Silver Customers")
    print(f"{'='*50}")

    df = read_parquet_files(
        SILVER_BUCKET, "customers/")
    qc = QualityCheck("Silver Customers")

    qc.check(
        df is not None,
        "Customer data exists in Silver"
    )

    if df is None:
        qc.summary()
        return False

    qc.check(
        len(df) >= 100,
        "Minimum customer count",
        actual=len(df),
        expected=">=100"
    )

    pii_columns = ['email', 'phone', 'name', 'address']
    for col in pii_columns:
        qc.check(
            col not in df.columns,
            f"PII column removed: {col}"
        )

    masked_columns = [
        'email_masked', 'phone_masked', 'name_masked'
    ]
    for col in masked_columns:
        qc.check(
            col in df.columns,
            f"Masked column exists: {col}"
        )

    qc.check(
        df['customer_id'].nunique() == len(df),
        "No duplicate customer_ids",
        actual=len(df) - df['customer_id'].nunique(),
        expected="0 duplicates"
    )

    return qc.summary()

def check_gold_customer_360():
    print(f"\n{'='*50}")
    print("Checking Gold Customer 360")
    print(f"{'='*50}")

    df = read_parquet_files(
        "fintech-gold-dev", "customer_360/")
    qc = QualityCheck("Gold Customer 360")

    qc.check(
        df is not None,
        "Customer 360 exists in Gold"
    )

    if df is None:
        qc.summary()
        return False

    qc.check(
        len(df) >= 100,
        "Minimum customer count in Gold",
        actual=len(df),
        expected=">=100"
    )

    required_columns = [
        'customer_id', 'kyc_status',
        'risk_tier', 'total_transactions',
        'total_spend', 'fraud_rate',
        'total_tickets', 'updated_at'
    ]
    for col in required_columns:
        qc.check(
            col in df.columns,
            f"Required column exists: {col}"
        )

    pii_columns = ['email', 'phone', 'name']
    for col in pii_columns:
        qc.check(
            col not in df.columns,
            f"No PII in Gold: {col}"
        )

    qc.check(
        df['customer_id'].nunique() == len(df),
        "No duplicate customers in Gold",
        actual=len(df) - df['customer_id'].nunique(),
        expected="0 duplicates"
    )

    qc.check(
        (df['fraud_rate'] >= 0).all() and
        (df['fraud_rate'] <= 100).all(),
        "Fraud rate between 0-100%",
        actual=f"min={df['fraud_rate'].min():.1f} "
               f"max={df['fraud_rate'].max():.1f}",
        expected="0-100"
    )

    return qc.summary()

def run_all_checks(date_str):
    print("\n" + "="*50)
    print("FINTECH DATA QUALITY REPORT")
    print(f"Date: {date_str}")
    print(f"Time: {datetime.now()}")
    print("="*50)

    results = {
        "bronze_transactions": check_bronze_transactions(date_str),
        "silver_transactions": check_silver_transactions(date_str),
        "silver_customers": check_silver_customers(),
        "gold_customer_360": check_gold_customer_360()
    }

    print("\n" + "="*50)
    print("OVERALL RESULTS")
    print("="*50)

    all_passed = True
    for check_name, passed in results.items():
        status = "PASS ✅" if passed else "FAIL ❌"
        print(f"  {status} {check_name}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\n✅ ALL QUALITY CHECKS PASSED!")
        print("Pipeline can proceed safely.")
    else:
        print("\n❌ QUALITY CHECKS FAILED!")
        print("Pipeline should NOT proceed!")

        failed_checks = [
            k for k, v in results.items()
            if not v
        ]
        message = (
            f"DATA QUALITY FAILURE\n"
            f"Date: {date_str}\n"
            f"Failed checks: {failed_checks}\n"
            f"Action: Investigate before"
            f" running ETL!"
        )
        send_alert(
            "DATA QUALITY ALERT",
            message
        )

    return all_passed

if __name__ == "__main__":
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    if len(sys.argv) > 1:
        date_str = sys.argv[1]

    print(f"Running checks for: {date_str}")
    success = run_all_checks(date_str)
    sys.exit(0 if success else 1)
