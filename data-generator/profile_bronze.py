import boto3
import pandas as pd
import io
from datetime import datetime

s3 = boto3.client('s3', region_name='ap-south-1')
BUCKET = 'fintech-bronze-dev'

def read_parquet_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj['Body'].read()))

def profile_dataframe(df, name):
    print(f'\n{"="*50}')
    print(f'PROFILE: {name}')
    print(f'{"="*50}')
    print(f'Rows:     {len(df):,}')
    print(f'Columns:  {len(df.columns)}')
    print(f'\nColumn Details:')
    print(f'{"-"*50}')

    for col in df.columns:
        null_count = df[col].isnull().sum()
        null_pct = round(null_count / len(df) * 100, 1)
        unique = df[col].nunique()

        if df[col].dtype in ['float64', 'int64']:
            print(f'{col:25} | '
                  f'nulls:{null_pct:4}% | '
                  f'unique:{unique:5} | '
                  f'min:{df[col].min():10.2f} | '
                  f'max:{df[col].max():10.2f} | '
                  f'mean:{df[col].mean():10.2f}')
        else:
            sample = str(df[col].iloc[0])[:20]
            print(f'{col:25} | '
                  f'nulls:{null_pct:4}% | '
                  f'unique:{unique:5} | '
                  f'sample: {sample}')

def check_duplicates(df, name, key_col):
    dupes = df[key_col].duplicated().sum()
    print(f'\nDuplicates on {key_col}: {dupes}')
    if dupes > 0:
        print(f'WARNING: {dupes} duplicate {key_col} found!')
    else:
        print(f'OK: No duplicates found')

def check_fraud_distribution(df):
    print(f'\nFraud Distribution:')
    dist = df['is_fraud'].value_counts()
    total = len(df)
    for val, count in dist.items():
        label = 'FRAUD' if val == 1 else 'NORMAL'
        pct = round(count/total*100, 1)
        print(f'  {label}: {count:,} ({pct}%)')

def check_kyc_distribution(df):
    print(f'\nKYC Status Distribution:')
    for val, count in df['kyc_status'].value_counts().items():
        pct = round(count/len(df)*100, 1)
        print(f'  {val}: {count} ({pct}%)')

    print(f'\nRisk Tier Distribution:')
    for val, count in df['risk_tier'].value_counts().items():
        pct = round(count/len(df)*100, 1)
        print(f'  {val}: {count} ({pct}%)')

def check_ticket_distribution(df):
    print(f'\nTicket Category Distribution:')
    for val, count in df['category'].value_counts().items():
        pct = round(count/len(df)*100, 1)
        print(f'  {val}: {count} ({pct}%)')

if __name__ == '__main__':
    print('BRONZE LAYER DATA PROFILING REPORT')
    print(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f'Bucket: s3://{BUCKET}')

    print('\n\n### 1. TRANSACTIONS ###')
    txn_dfs = []
    keys = [
        'transactions/year=2026/month=04/day=26/transactions_20260426.parquet',
        'transactions/year=2026/month=04/day=27/transactions_20260427.parquet',
        'transactions/year=2026/month=04/day=28/transactions_20260428.parquet',
        'transactions/year=2026/month=04/day=29/transactions_20260429.parquet',
        'transactions/year=2026/month=04/day=30/transactions_20260430.parquet',
        'transactions/year=2026/month=05/day=01/transactions_20260501.parquet',
        'transactions/year=2026/month=05/day=02/transactions_20260502.parquet',
    ]
    for key in keys:
        txn_dfs.append(read_parquet_from_s3(key))
    txn_df = pd.concat(txn_dfs, ignore_index=True)
    profile_dataframe(txn_df, 'TRANSACTIONS')
    check_duplicates(txn_df, 'transactions', 'transaction_id')
    check_fraud_distribution(txn_df)

    print('\n\n### 2. CUSTOMERS ###')
    cust_df = read_parquet_from_s3('customers/customers.parquet')
    profile_dataframe(cust_df, 'CUSTOMERS')
    check_duplicates(cust_df, 'customers', 'customer_id')

    print('\n\n### 3. KYC ###')
    kyc_df = read_parquet_from_s3('kyc/kyc.parquet')
    profile_dataframe(kyc_df, 'KYC')
    check_duplicates(kyc_df, 'kyc', 'customer_id')
    check_kyc_distribution(kyc_df)

    print('\n\n### 4. SUPPORT TICKETS ###')
    tickets_df = read_parquet_from_s3(
        'support_tickets/support_tickets.parquet')
    profile_dataframe(tickets_df, 'SUPPORT TICKETS')
    check_duplicates(tickets_df, 'tickets', 'ticket_id')
    check_ticket_distribution(tickets_df)

    print('\n\n### BRONZE LAYER SUMMARY ###')
    print(f'{"="*50}')
    print(f'Transactions:    {len(txn_df):,} rows')
    print(f'Customers:       {len(cust_df):,} rows')
    print(f'KYC records:     {len(kyc_df):,} rows')
    print(f'Support tickets: {len(tickets_df):,} rows')
    print(f'Total rows:      {len(txn_df)+len(cust_df)+len(kyc_df)+len(tickets_df):,}')
    print(f'\nBronze layer verification COMPLETE')
    print(f'Ready for Silver ETL pipeline!')
