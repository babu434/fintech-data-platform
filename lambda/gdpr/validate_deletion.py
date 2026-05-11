import boto3
import json
from datetime import datetime

dynamodb = boto3.resource(
    'dynamodb', region_name='ap-south-1')
sns = boto3.client(
    'sns', region_name='ap-south-1')

SNS_TOPIC = (
    "arn:aws:sns:ap-south-1:"
    "512584596315:fintech-gdpr-dev"
)

def lambda_handler(event, context):
    print(f"Validating deletion request: {event}")

    customer_id = event.get('customer_id')

    if not customer_id:
        raise ValueError(
            "customer_id is required!")

    if not customer_id.startswith('CUST'):
        raise ValueError(
            f"Invalid customer_id format: {customer_id}"
        )

    request_id = (
        f"GDPR-{customer_id}-"
        f"{datetime.now().strftime('%Y%m%d%H%M%S')}"
    )

    print(f"Deletion request validated!")
    print(f"Customer: {customer_id}")
    print(f"Request ID: {request_id}")
    print(f"Timestamp: {datetime.now().isoformat()}")

    return {
        'customer_id': customer_id,
        'request_id': request_id,
        'status': 'VALIDATED',
        'timestamp': datetime.now().isoformat()
    }
