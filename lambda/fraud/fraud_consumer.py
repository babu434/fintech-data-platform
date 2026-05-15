import boto3
import json
import base64
from datetime import datetime

dynamodb = boto3.resource(
    'dynamodb', region_name='ap-south-1')

TABLE = 'fintech-fraud-decisions-dev'

def calculate_fraud_score(transaction):
    score = 0
    reasons = []

    amount = transaction.get('amount', 0)
    timestamp = transaction.get('timestamp', '')
    device_id = transaction.get('device_id', '')
    customer_id = transaction.get('customer_id', '')
    location_lat = transaction.get('location_lat', 0)
    location_lng = transaction.get('location_lng', 0)

    try:
        hour = int(timestamp[11:13])
    except:
        hour = 12

    known_device = device_id.startswith('device-')

    if amount > 100000:
        score += 50
        reasons.append(f'very_high_amount:{amount}')
    elif amount > 50000:
        score += 30
        reasons.append(f'high_amount:{amount}')
    elif amount > 20000:
        score += 15
        reasons.append(f'elevated_amount:{amount}')

    if hour < 5:
        score += 25
        reasons.append(f'midnight_transaction:hour_{hour}')
    elif hour < 7:
        score += 10
        reasons.append(f'early_morning:hour_{hour}')

    if not known_device:
        score += 20
        reasons.append('unknown_device')

    if location_lat > 30 or location_lat < 10:
        score += 10
        reasons.append(f'unusual_location:{location_lat}')

    if amount > 50000 and not known_device:
        score += 15
        reasons.append('high_amount_unknown_device')

    if amount > 20000 and hour < 5:
        score += 15
        reasons.append('high_amount_midnight')

    score = min(score, 100)
    return score, reasons

def make_decision(score):
    if score >= 70:
        return 'BLOCK'
    elif score >= 40:
        return 'FLAG'
    else:
        return 'ALLOW'

def process_transaction(transaction):
    score, reasons = calculate_fraud_score(
        transaction)
    decision = make_decision(score)

    result = {
        'transaction_id': transaction['transaction_id'],
        'customer_id': transaction['customer_id'],
        'amount': str(transaction['amount']),
        'merchant': transaction.get('merchant', ''),
        'fraud_score': score,
        'decision': decision,
        'reasons': json.dumps(reasons),
        'actual_fraud': transaction.get('is_fraud', 0),
        'timestamp': transaction.get('timestamp', ''),
        'processed_at': datetime.now().isoformat(),
        'model_version': 'rule-engine-v1'
    }

    table = dynamodb.Table(TABLE)
    table.put_item(Item=result)

    return result

def lambda_handler(event, context):
    print(f"Received {len(event['Records'])} records")

    results = {
        'processed': 0,
        'allowed': 0,
        'flagged': 0,
        'blocked': 0,
        'errors': 0
    }

    for record in event['Records']:
        try:
            data = base64.b64decode(
                record['kinesis']['data']
            ).decode('utf-8')

            transaction = json.loads(data)

            result = process_transaction(transaction)
            results['processed'] += 1

            decision = result['decision']
            if decision == 'ALLOW':
                results['allowed'] += 1
            elif decision == 'FLAG':
                results['flagged'] += 1
            elif decision == 'BLOCK':
                results['blocked'] += 1

            print(
                f"txn={result['transaction_id'][:8]}... "
                f"amount=₹{result['amount']} "
                f"score={result['fraud_score']} "
                f"decision={result['decision']} "
                f"actual={result['actual_fraud']}"
            )

        except Exception as e:
            results['errors'] += 1
            print(f"Error processing record: {e}")

    print(f"Results: {results}")
    return results
