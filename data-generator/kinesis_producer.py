import boto3
import json
import uuid
import random
import time
from datetime import datetime
from faker import Faker

fake = Faker('en_IN')
kinesis = boto3.client(
    'kinesis', region_name='ap-south-1')

STREAM_NAME = 'fintech-transactions-dev'

MERCHANTS = [
    'Amazon', 'Flipkart', 'Swiggy',
    'Zomato', 'Uber', 'Paytm'
]

CATEGORIES = [
    'ecommerce', 'food_delivery',
    'transport', 'banking'
]

def generate_transaction(is_fraud=False):
    customer_id = f'CUST{random.randint(0,499):04d}'

    if is_fraud:
        amount = round(random.uniform(50000, 150000), 2)
        hour = random.randint(0, 4)
        device_id = str(uuid.uuid4())
        lat = round(random.uniform(8.0, 37.0), 4)
        lng = round(random.uniform(68.0, 97.0), 4)
    else:
        amount = round(random.uniform(100, 5000), 2)
        hour = random.randint(9, 22)
        device_id = f'device-{customer_id}'
        lat = round(random.uniform(12.0, 19.0), 4)
        lng = round(random.uniform(72.0, 80.0), 4)

    return {
        'transaction_id': str(uuid.uuid4()),
        'customer_id': customer_id,
        'amount': amount,
        'merchant': random.choice(MERCHANTS),
        'category': random.choice(CATEGORIES),
        'timestamp': datetime.now().replace(
            hour=hour).isoformat(),
        'device_id': device_id,
        'location_lat': lat,
        'location_lng': lng,
        'is_fraud': int(is_fraud),
        'currency': 'INR'
    }

def send_to_kinesis(transaction):
    response = kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(transaction),
        PartitionKey=transaction['customer_id']
    )
    return response['ShardId']

def run_producer(
        num_transactions=100,
        fraud_rate=0.05):

    print(f"Starting Kinesis producer...")
    print(f"Stream: {STREAM_NAME}")
    print(f"Transactions: {num_transactions}")
    print(f"Fraud rate: {fraud_rate*100}%")
    print("="*50)

    sent = 0
    fraud_sent = 0
    errors = 0

    for i in range(num_transactions):
        is_fraud = random.random() < fraud_rate

        transaction = generate_transaction(
            is_fraud=is_fraud)

        try:
            shard_id = send_to_kinesis(transaction)
            sent += 1
            if is_fraud:
                fraud_sent += 1

            if i % 10 == 0:
                print(
                    f"Sent {sent} transactions "
                    f"({fraud_sent} fraud) "
                    f"→ {shard_id}"
                )

            time.sleep(0.1)

        except Exception as e:
            errors += 1
            print(f"Error: {e}")

    print("="*50)
    print(f"Complete!")
    print(f"Total sent:  {sent}")
    print(f"Fraud sent:  {fraud_sent}")
    print(f"Errors:      {errors}")

if __name__ == '__main__':
    run_producer(
        num_transactions=100,
        fraud_rate=0.05
    )
