from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue\
    import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler\
    import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.sns\
    import SnsPublishOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

SNS_TOPIC_ARN = (
    "arn:aws:sns:ap-south-1:"
    "512584596315:fintech-alerts-dev"
)

GLUE_CONN_ID = "aws_default"
REGION = "ap-south-1"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

def check_data_quality(**context):
    import boto3
    import pandas as pd
    import io

    execution_date = context['ds']
    print(f"Running quality checks for {execution_date}")

    s3 = boto3.client('s3', region_name=REGION)

    response = s3.list_objects_v2(
        Bucket='fintech-silver-dev',
        Prefix=f'transactions/transaction_date={execution_date}/'
    )

    if 'Contents' not in response:
        raise ValueError(
            f"No Silver data found for {execution_date}!"
            " Pipeline may have failed!"
        )

    parquet_files = [
        obj['Key'] for obj in response['Contents']
        if obj['Key'].endswith('.parquet')
        and 'delta_log' not in obj['Key']
    ]

    if not parquet_files:
        raise ValueError(
            f"No Parquet files in Silver for {execution_date}!"
        )

    total_rows = 0
    for key in parquet_files:
        obj = s3.get_object(
            Bucket='fintech-silver-dev', Key=key)
        df = pd.read_parquet(
            io.BytesIO(obj['Body'].read()))
        total_rows += len(df)

    print(f"Total Silver rows: {total_rows}")

    if total_rows < 1000:
        raise ValueError(
            f"Too few rows in Silver: {total_rows}!"
            " Expected at least 1000!"
        )

    null_check = df['transaction_id'].isnull().sum()
    if null_check > 0:
        raise ValueError(
            f"Null transaction_ids found: {null_check}!"
        )

    pii_check = 'email' in df.columns
    if pii_check:
        raise ValueError(
            "PII column 'email' found in Silver!"
            " Masking failed!"
        )

    print(f"All quality checks passed! ✅")
    print(f"Rows: {total_rows}")
    print(f"No nulls on transaction_id ✅")
    print(f"No PII in Silver ✅")

    return total_rows

def notify_failure(context):
    import boto3
    sns = boto3.client('sns', region_name=REGION)

    task_id = context['task_instance'].task_id
    dag_id  = context['dag'].dag_id
    exec_dt = context['ds']

    message = (
        f"PIPELINE FAILURE ALERT\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Date: {exec_dt}\n"
        f"Time: {datetime.now()}\n"
        f"Action: Check Airflow logs immediately!"
    )

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=message,
        Subject=f"ALERT: {dag_id} failed on {task_id}"
    )
    print(f"Failure alert sent to SNS!")

with DAG(
    dag_id="fintech_daily_pipeline",
    default_args=default_args,
    description="Daily Bronze→Silver→Gold pipeline",
    schedule_interval="0 1 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["fintech", "etl", "daily"],
    sla_miss_callback=None,
) as dag:

    crawl_bronze = GlueCrawlerOperator(
        task_id="crawl_bronze",
        config={"Name": "fintech-transactions-crawler-dev"},
        aws_conn_id=GLUE_CONN_ID,
        region_name=REGION,
        on_failure_callback=notify_failure
    )

    bronze_to_silver = GlueJobOperator(
        task_id="bronze_to_silver",
        job_name="fintech-bronze-to-silver-dev",
        script_args={
            "--date": "{{ ds }}"
        },
        aws_conn_id=GLUE_CONN_ID,
        region_name=REGION,
        on_failure_callback=notify_failure
    )

    data_quality = PythonOperator(
        task_id="data_quality_check",
        python_callable=check_data_quality,
        provide_context=True,
        on_failure_callback=notify_failure
    )

    silver_to_gold = GlueJobOperator(
        task_id="silver_to_gold",
        job_name="fintech-silver-to-gold-dev",
        script_args={
            "--date": "{{ ds }}"
        },
        aws_conn_id=GLUE_CONN_ID,
        region_name=REGION,
        on_failure_callback=notify_failure
    )

    notify_success = SnsPublishOperator(
        task_id="notify_success",
        target_arn=SNS_TOPIC_ARN,
        message=(
            "✅ Fintech daily pipeline SUCCEEDED!\n"
            "Date: {{ ds }}\n"
            "Time: {{ ts }}\n"
            "Gold tables updated successfully!"
        ),
        subject="SUCCESS: Fintech Daily Pipeline",
        aws_conn_id=GLUE_CONN_ID,
        region_name=REGION
    )

    crawl_bronze >> bronze_to_silver >> \
    data_quality >> silver_to_gold >> \
    notify_success
