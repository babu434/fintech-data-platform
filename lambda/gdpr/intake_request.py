import boto3
import json
from datetime import datetime, timedelta

dynamodb = boto3.resource(
    'dynamodb', region_name='ap-south-1')
sns = boto3.client(
    'sns', region_name='ap-south-1')

TABLE = 'fintech-gdpr-requests-dev'
SNS_TOPIC = (
    "arn:aws:sns:ap-south-1:"
    "512584596315:fintech-gdpr-dev"
)

def lambda_handler(event, context):
    customer_id = event.get('customer_id')

    if not customer_id:
        return {
            'statusCode': 400,
            'body': 'customer_id required!'
        }

    requested_at = datetime.now().isoformat()
    sla_deadline = (
        datetime.now() + timedelta(hours=24)
    ).isoformat()
    request_id = (
        f"GDPR-{customer_id}-"
        f"{datetime.now().strftime('%Y%m%d%H%M%S')}"
    )

    table = dynamodb.Table(TABLE)
    table.put_item(Item={
        'customer_id':  customer_id,
        'requested_at': requested_at,
        'status':       'PENDING',
        'request_id':   request_id,
        'sla_deadline': sla_deadline,
        'completed_at': None
    })

    sns.publish(
        TopicArn=SNS_TOPIC,
        Message=(
            f"GDPR deletion request received!\n"
            f"Customer: {customer_id}\n"
            f"Request ID: {request_id}\n"
            f"SLA deadline: {sla_deadline}\n"
            f"Will complete within 24 hours."
        ),
        Subject=f"GDPR Request Received: {request_id}"
    )

    print(f"Request stored: {request_id}")

    return {
        'statusCode': 200,
        'body': json.dumps({
            'request_id': request_id,
            'status': 'PENDING',
            'message': 'Deletion request received!'
                       ' Will complete within 24 hours.',
            'sla_deadline': sla_deadline
        })
    }
