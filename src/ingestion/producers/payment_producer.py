import json
import os
import time
import random
from datetime import datetime, timedelta, timezone
from faker import Faker
from confluent_kafka import Producer
import string

fake = Faker() 

MERCHANTS = [
    f"merch_{''.join(random.choices(string.ascii_lowercase + string.digits, k=10))}" 
    for i in range (20)]

PAYMENT_METHODS = ['credit_card', 'debit_card', 'apple_pay', 'paypal', 'bank_transfer']
METHOD_WEIGHTS = [0.50, 0.25, 0.15, 0.08, 0.02]
COUNTRIES = ['US', 'GB', 'DE', 'EG', 'AE', 'CA']
SOURCE_SYSTEMS = ['stripe_webhook', 'adyen_api', 'paypal_ipn', 'internal_pos']
FAILURE_CODES = ['insufficient_funds', 'expired_card', 'incorrect_cvc', 'suspected_fraud']
FAILURE_WEIGHTS = [0.50, 0.25, 0.15, 0.10] # Most failures are just lack of funds


pending_authorizations = {} # Waiting to be captured
captured_transactions = {}  # Waiting to be settled or refunded/charged back

def generate_initial_checkout():
    """Generates a highly realistic payment event using Faker.""" 
    transaction_id = fake.uuid4()
    idempotency_key = fake.uuid4()
    intent_id = f"intent_{fake.uuid4()}"
    
    origin_time = datetime.now(timezone.utc)
    
    merchant_id = random.choice(MERCHANTS)
    
    currency = fake.random_element(elements=('USD', 'EGP', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF'))
    amount = round(random.uniform(5.00, 1500.00), 8)
    user_id = fake.random_int(min=10000, max=99999)
    
    payload = {
        'transaction_id': transaction_id, 
        'idempotency_key': idempotency_key, 
        'intent_id': intent_id,
        'charge_id': None,
        'refund_id': None,
        'chargeback_id': None,
        'retry_attempt': 0,
        
        'merchant_id': merchant_id, 
        'amount': amount, 
        'currency': currency,
        'user_id': f'user_{user_id}', 
        'country': random.choice(COUNTRIES),
        
        'source_system': random.choice(SOURCE_SYSTEMS), 
        'payment_method': random.choices(PAYMENT_METHODS, weights=METHOD_WEIGHTS, k=1)[0],
        'original_transaction_timestamp': origin_time.isoformat()

    }
    
    events_to_emit = []
    created_event = payload.copy()
    created_event['event_type'] = 'payment_intent_created'
    created_event['status'] = 'pending'
    created_event['event_id'] = fake.uuid4()

    current_time = datetime.now(timezone.utc).isoformat()
    created_event['event_timestamp'] = current_time
    created_event['ingestion_timestamp'] = current_time
    events_to_emit.append(created_event)
    time.sleep(0.05) # Tiny micro-delay between webhooks
    

    decision_event = payload.copy()
    decision_event['event_id'] = fake.uuid4()
    current_time = datetime.now(timezone.utc).isoformat()
    decision_event['event_timestamp'] = current_time
    decision_event['ingestion_timestamp'] = current_time
    
    if random.random() < 0.85: # 85% chance of success
        decision_event['event_type'] = 'payment_intent_authorized'
        decision_event['status'] = 'authorized'
        pending_authorizations[transaction_id] = decision_event # Store for later capture
        
    else:
        decision_event['event_type'] = 'payment_intent_failed'
        decision_event['status'] = 'failed'
        decision_event['failure_code'] = random.choices(FAILURE_CODES, weights=FAILURE_WEIGHTS, k=1)[0]
        # Failed transactions won't have captures.
    events_to_emit.append(decision_event)
    
    return events_to_emit



def get_duplicate_events(payload, p_exact = 0.05, p_retry = 0.02):
    outbound_events = [payload]
    
    if random.random() < p_exact:
        outbound_events.append(payload.copy()) 
    
    if random.random() < p_retry:
        retry_event = payload.copy()
        retry_event['event_id'] = fake.uuid4()
        retry_event['ingestion_timestamp'] = datetime.now(timezone.utc).isoformat()
        retry_event["retry_attempt"] = int(payload.get("retry_attempt", 0)) + 1
        outbound_events.append(retry_event)
    
    
    return outbound_events         
        
    
    
    
    





def progress_transaction_to_capture(transaction_id):
    """Step 2: The funds actually move (Linked Event)."""
    base_payload = pending_authorizations.pop(transaction_id, None) # Remove from pending, move to captured
    if base_payload is None:
        return None
    # Clone base payload but update the state
    capture_payload = base_payload.copy()
    capture_payload["event_id"] = fake.uuid4() # New Kafka event ID
    capture_payload["event_type"] = "capture_succeeded"
    capture_payload["status"] = "captured"
    capture_payload["charge_id"] = f"charge_{fake.uuid4()}"
    
    # Simulate processing delay (event happened 2 seconds ago, ingested now)
    event_time = datetime.now(timezone.utc) - timedelta(seconds=random.randint(1, 5))
    capture_payload['event_timestamp'] = event_time.isoformat()
    capture_payload["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

    # Store captured payload for potential refunds/chargebacks.
    captured_transactions[transaction_id] = capture_payload
    
    
    return capture_payload

def generate_late_lifecycle_event():
    """Handles Settled, Refunded, and Chargeback (Days Later)."""
    if not captured_transactions:
        return None
        
    transaction_id = random.choice(list(captured_transactions.keys()))
    base_payload = captured_transactions[transaction_id]
    
    # Decide if this is a settlement, refund, or chargeback
    lifecycle_kind = random.choices(['settled', 'refund', 'chargeback'], weights=[0.70, 0.25, 0.05])[0]
    
    late_event_payload = base_payload.copy()
    late_event_payload['event_id'] = fake.uuid4()
    
    # The event happened days ago (Late Lifecycle Event)
    days_late = random.randint(2, 45)
    historical_time = datetime.now(timezone.utc) - timedelta(days=days_late)
    late_event_payload['event_timestamp'] = historical_time.isoformat()
    late_event_payload['ingestion_timestamp'] = datetime.now(timezone.utc).isoformat()
    if lifecycle_kind == 'settled':
        late_event_payload['event_type'] = 'settlement_recorded'
        late_event_payload['status'] = 'settled'
        # Once settled, we can remove from captured as it's fully complete
        captured_transactions.pop(transaction_id)
        
    elif lifecycle_kind == 'refund':
        late_event_payload['event_type'] = 'refund_recorded'
        late_event_payload['status'] = 'refunded'
        late_event_payload['refund_id'] = f"refund_{fake.uuid4()}"
        # Refunds can still be charged back later, so we keep it in captured for now
        
    else: # Chargeback
        late_event_payload['event_type'] = 'chargeback_received'
        late_event_payload['status'] = 'chargeback'
        late_event_payload['chargeback_id'] = f"chargeback_{fake.uuid4()}"
        # Chargebacks are final, remove from captured
        captured_transactions.pop(transaction_id)
    
    
    # ______________________________________________________________________________________________________________ 
    
    return late_event_payload

    # ______________________________________________________________________________________________________________ 

def create_kafka_producer():
    return Producer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "client.id": "financial-reconciliation-payment-producer",
        }
    )


def emit_event_batch(producer, topic_name, raw_events, p_exact=0.05, p_retry=0.02):
    """Expand raw events into delivery variants, then publish to Kafka."""
    published = 0
    for raw_event in raw_events:
        outbound_events = get_duplicate_events(raw_event, p_exact=p_exact, p_retry=p_retry)
        for event in outbound_events:
            producer.produce(
                topic=topic_name,
                key=event["transaction_id"].encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
            )
            published += 1

    producer.poll(0)
    return published


def run_kafka_stream():
    topic_name = os.getenv("PAYMENT_EVENTS_TOPIC", "raw_payment_events")
    p_exact = float(os.getenv("PAYMENT_DUPLICATE_RATE", "0.05"))
    p_retry = float(os.getenv("PAYMENT_RETRY_RATE", "0.02"))

    producer = create_kafka_producer()
    print(f"Starting Kafka payment stream on topic={topic_name}")

    try:
        while True:
            action_roll = random.random()
            raw_events = []

            if action_roll < 0.60:
                raw_events = generate_initial_checkout()
            elif action_roll < 0.95 and pending_authorizations:
                txn_to_capture = random.choice(list(pending_authorizations.keys()))
                capture_event = progress_transaction_to_capture(txn_to_capture)
                if capture_event is not None:
                    raw_events = [capture_event]
            else:
                late_event = generate_late_lifecycle_event()
                if late_event is not None:
                    raw_events = [late_event]

            if raw_events:
                published_count = emit_event_batch(
                    producer,
                    topic_name,
                    raw_events,
                    p_exact=p_exact,
                    p_retry=p_retry,
                )
                print(f"published_events={published_count}")

            time.sleep(random.uniform(0.1, 0.7))

    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        print("Producer flushed and stopped.")


if __name__ == "__main__":
    run_kafka_stream()



