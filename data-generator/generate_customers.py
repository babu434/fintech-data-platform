import uuid
import random
import boto3
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('en_IN')
s3 = boto3.client('s3', region_name='ap-south-1')

BUCKET = 'fintech-bronze-dev'
NUM_CUSTOMERS = 500

RISK_TIERS = ['LOW', 'MEDIUM', 'HIGH']
KYC_STATUSES = ['VERIFIED', 'PENDING', 'REJECTED']
RISK_WEIGHTS = [0.6, 0.3, 0.1]
KYC_WEIGHTS = [0.7, 0.2, 0.1]

TICKET_CATEGORIES = [
    'transaction_dispute',
    'account_blocked',
    'payment_failed',
    'refund_request',
    'fraud_report'
]

def generate_customers():
    customers = []
    for i in range(NUM_CUSTOMERS):
        signup_date = fake.date_between(
            start_date='-3y',
            end_date='today'
        )
        customers.append({
            'customer_id':  f'CUST{str(i).zfill(4)}',
            'name':         fake.name(),
            'email':        fake.email(),
            'phone':        fake.phone_number(),
            'address':      fake.address().replace('\n', ' '),
            'city':         fake.city(),
            'state':        fake.state(),
            'pincode':      fake.postcode(),
            'signup_date':  str(signup_date),
            'avg_spend':    round(random.uniform(100, 5000), 2),
            'home_lat':     round(random.uniform(8.0, 37.0), 4),
            'home_lng':     round(random.uniform(68.0, 97.0), 4),
            'device_id':    str(uuid.uuid4()),
            'is_active':    random.choice([1, 1, 1, 0])
        })
    return customers

def generate_kyc(customers):
    kyc_records = []
    for customer in customers:
        kyc_records.append({
            'customer_id':    customer['customer_id'],
            'kyc_status':     random.choices(
                                KYC_STATUSES,
                                weights=KYC_WEIGHTS)[0],
            'risk_tier':      random.choices(
                                RISK_TIERS,
                                weights=RISK_WEIGHTS)[0],
            'pan_verified':   random.choice([True, True, False]),
            'aadhar_verified': random.choice([True, True, False]),
            'kyc_date':       str(fake.date_between(
                                start_date='-2y',
                                end_date='today')),
            'kyc_expiry':     str(fake.date_between(
                                start_date='today',
                                end_date='+3y'))
        })
    return kyc_records

def generate_support_tickets(customers):
    tickets = []
    for customer in customers:
        num_tickets = random.choices(
            [0, 1, 2, 3, 4, 5],
            weights=[0.4, 0.3, 0.15, 0.1, 0.04, 0.01]
        )[0]

        for _ in range(num_tickets):
            created_date = fake.date_between(
                start_date='-1y',
                end_date='today'
            )
            tickets.append({
                'ticket_id':    str(uuid.uuid4()),
                'customer_id':  customer['customer_id'],
                'category':     random.choice(TICKET_CATEGORIES),
                'status':       random.choice(
                                  ['OPEN', 'CLOSED', 'PENDING']),
                'priority':     random.choice(
                                  ['LOW', 'MEDIUM', 'HIGH']),
                'created_date': str(created_date),
                'description':  fake.sentence(nb_words=10)
            })
    return tickets

def upload_to_s3(df, s3_key):
    local_file = f'/tmp/{s3_key.split("/")[-1]}'
    df.to_parquet(local_file, index=False)
    s3.upload_file(local_file, BUCKET, s3_key)
    print(f'Uploaded {len(df)} rows to '
          f's3://{BUCKET}/{s3_key}')

if __name__ == '__main__':
    print('Generating customer data...')

    customers = generate_customers()
    customers_df = pd.DataFrame(customers)
    upload_to_s3(
        customers_df,
        'customers/customers.parquet'
    )

    print('Generating KYC data...')
    kyc = generate_kyc(customers)
    kyc_df = pd.DataFrame(kyc)
    upload_to_s3(
        kyc_df,
        'kyc/kyc.parquet'
    )

    print('Generating support tickets...')
    tickets = generate_support_tickets(customers)
    tickets_df = pd.DataFrame(tickets)
    upload_to_s3(
        tickets_df,
        'support_tickets/support_tickets.parquet'
    )

    print('Done! All customer data uploaded to Bronze S3')
    print(f'Customers: {len(customers)}')
    print(f'KYC records: {len(kyc)}')
    print(f'Support tickets: {len(tickets)}')
