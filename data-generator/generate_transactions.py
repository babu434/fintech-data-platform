import uuid
import random
import json
import boto3
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('en_IN')
s3 = boto3.client('s3', region_name='ap-south-1')

BUCKET = 'fintech-bronze-dev'
NUM_CUSTOMERS = 500
NUM_TRANSACTIONS = 10000
FRAUD_RATE = 0.05

MERCHANTS = [
    'Amazon', 'Flipkart', 'Swiggy', 'Zomato',
    'Uber', 'Ola', 'BigBasket', 'Myntra',
    'HDFC Bank', 'ICICI Bank', 'Paytm', 'PhonePe'
]

CATEGORIES = [
    'ecommerce', 'food_delivery', 'transport',
    'groceries', 'fashion', 'banking',
    'entertainment', 'utilities'
]

MERCHANT_CATEGORY = {
    'Amazon': 'ecommerce',
    'Flipkart': 'ecommerce',
    'Swiggy': 'food_delivery',
    'Zomato': 'food_delivery',
    'Uber': 'transport',
    'Ola': 'transport',
    'BigBasket': 'groceries',
    'Myntra': 'fashion',
    'HDFC Bank': 'banking',
    'ICICI Bank': 'banking',
    'Paytm': 'banking',
    'PhonePe': 'banking'
}

def generate_customers():
    customers = []
    for i in range(NUM_CUSTOMERS):
        customers.append({
            'customer_id': f'CUST{str(i).zfill(4)}',
            'avg_spend': round(random.uniform(100, 5000), 2),
            'home_lat': round(random.uniform(8.0, 37.0), 4),
            'home_lng': round(random.uniform(68.0, 97.0), 4),
            'device_id': str(uuid.uuid4())
        })
    return customers

def generate_transaction(customer, txn_date, is_fraud=False):
    merchant = random.choice(MERCHANTS)

    if is_fraud:
        amount = round(customer['avg_spend'] *
                      random.uniform(5, 15), 2)
        lat = round(random.uniform(8.0, 37.0), 4)
        lng = round(random.uniform(68.0, 97.0), 4)
        device_id = str(uuid.uuid4())
    else:
        amount = round(customer['avg_spend'] *
                      random.uniform(0.1, 2.0), 2)
        lat = round(customer['home_lat'] +
                   random.uniform(-0.5, 0.5), 4)
        lng = round(customer['home_lng'] +
                   random.uniform(-0.5, 0.5), 4)
        device_id = customer['device_id']

    return {
        'transaction_id': str(uuid.uuid4()),
        'customer_id': customer['customer_id'],
        'amount': amount,
        'merchant': merchant,
        'category': MERCHANT_CATEGORY[merchant],
        'timestamp': txn_date.strftime('%Y-%m-%d %H:%M:%S'),
        'transaction_date': txn_date.strftime('%Y-%m-%d'),
        'device_id': device_id,
        'location_lat': lat,
        'location_lng': lng,
        'is_fraud': int(is_fraud),
        'currency': 'INR'
    }

def generate_daily_transactions(txn_date):
    customers = generate_customers()
    transactions = []

    num_fraud = int(NUM_TRANSACTIONS * FRAUD_RATE)
    num_normal = NUM_TRANSACTIONS - num_fraud

    for _ in range(num_normal):
        customer = random.choice(customers)
        hour = random.randint(8, 23)
        minute = random.randint(0, 59)
        txn_time = txn_date.replace(
            hour=hour, minute=minute)
        transactions.append(
            generate_transaction(customer, txn_time,
                               is_fraud=False))

    for _ in range(num_fraud):
        customer = random.choice(customers)
        hour = random.randint(0, 5)
        minute = random.randint(0, 59)
        txn_time = txn_date.replace(
            hour=hour, minute=minute)
        transactions.append(
            generate_transaction(customer, txn_time,
                               is_fraud=True))

    random.shuffle(transactions)
    return transactions

def upload_to_s3(transactions, txn_date):
    df = pd.DataFrame(transactions)

    year = txn_date.strftime('%Y')
    month = txn_date.strftime('%m')
    day = txn_date.strftime('%d')

    key = (f'transactions/year={year}/'
           f'month={month}/day={day}/'
           f'transactions_{year}{month}{day}.parquet')

    local_file = f'/tmp/transactions_{year}{month}{day}.parquet'
    df.to_parquet(local_file, index=False)

    s3.upload_file(local_file, BUCKET, key)
    print(f'Uploaded {len(transactions)} transactions to '
          f's3://{BUCKET}/{key}')

    return key

if __name__ == '__main__':
    print('Generating transactions for last 7 days...')

    for days_ago in range(7, 0, -1):
        txn_date = datetime.now() - timedelta(days=days_ago)
        print(f'Generating {txn_date.strftime("%Y-%m-%d")}...')

        transactions = generate_daily_transactions(txn_date)
        upload_to_s3(transactions, txn_date)

    print('Done! 7 days of transactions uploaded to Bronze S3')
