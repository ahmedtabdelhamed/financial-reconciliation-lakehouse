import json
import time
import random
from datetime import datetime, timedelta, timezone
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

# --- REALISTIC TUPLES & POOLS ---
MERCHANTS = [f"merch_{fake.unique.alphanumeric(letters='abcdefghijklmnopqrstuvwxyz0123456789', length=10)}" for _ in range(15)]
PAYMENT_METHODS = ['credit_card', 'debit_card', 'apple_pay', 'paypal', 'bank_transfer']
METHOD_WEIGHTS = [0.50, 0.25, 0.15, 0.08, 0.02]
COUNTRIES = ['US', 'GB', 'DE', 'EG', 'AE', 'CA']
SOURCE_SYSTEMS = ['stripe_webhook', 'adyen_api', 'paypal_ipn', 'internal_pos']

# Memory stores for stateful events
active_transactions = {}  # {transaction_id: payload_dict} for linked events
historical_captures = []  # List of transaction_ids for delayed refunds/chargebacks
recent_events_queue = []  # Buffer to simulate duplicate network drops

conf = {'bootstrap.servers': 'localhost:9092', 'client.id': 'advanced-mock-producer'}
producer = Producer(conf)
topic_name = 'raw_payment_events'

def get_current_utc():
    return datetime.now(timezone.utc)

def emit_event(payload):
    """Handles JSON encoding, producing to Kafka, and duplicate injection."""
    json_payload = json.dumps(payload).encode('utf-8')
    producer.produce(topic=topic_name, value=json_payload)
    
    # DUPLICATE INJECTION (2% chance the network stutters and sends the exact same event twice)
    if random.random() < 0.02:
        print(f"   [NETWORK STUTTER] Emitting exact duplicate for Event ID: {payload['event_id']}")
        producer.produce(topic=topic_name, value=json_payload)

def generate_new_authorization():
    """Step 1: The user clicks 'Pay'."""
    transaction_id = fake.uuid4()
    merchant_id = random.choice(MERCHANTS)
    
    payload = {
        "event_id": fake.uuid4(),
        "transaction_id": transaction_id,
        "merchant_id": merchant_id,
        "user_id": fake.random_int(min=10000, max=99999),
        "payment_method": random.choices(PAYMENT_METHODS, weights=METHOD_WEIGHTS, k=1)[0],
        "country": random.choice(COUNTRIES),
        "amount": round(random.uniform(5.00, 1500.00), 2),
        "currency": "USD",
        "event_type": "AUTHORIZATION",
        "source_system": random.choice(SOURCE_SYSTEMS),
        "processor_reference": f"ch_{fake.alphanumeric(14)}",
        "event_timestamp": get_current_utc().isoformat(),
        "ingestion_timestamp": get_current_utc().isoformat()
    }
    
    # Store in memory so we can link a 'CAPTURE' to it later
    active_transactions[transaction_id] = payload
    return payload

def progress_transaction_to_capture(transaction_id):
    """Step 2: The funds actually move (Linked Event)."""
    base_payload = active_transactions.pop(transaction_id) # Remove from active, move to captured
    historical_captures.append(transaction_id)
    
    # Keep the list manageable to prevent memory leaks
    if len(historical_captures) > 500:
        historical_captures.pop(0)

    # Clone base payload but update the state
    capture_payload = base_payload.copy()
    capture_payload["event_id"] = fake.uuid4() # New Kafka event ID
    capture_payload["event_type"] = "CAPTURE"
    
    # Simulate processing delay (event happened 2 seconds ago, ingested now)
    event_time = get_current_utc() - timedelta(seconds=random.randint(1, 5))
    capture_payload["event_timestamp"] = event_time.isoformat()
    capture_payload["ingestion_timestamp"] = get_current_utc().isoformat()
    
    return capture_payload

def generate_delayed_update():
    """Step 3: Late arriving data (Refund or Chargeback)."""
    if not historical_captures:
        return None
        
    transaction_id = random.choice(historical_captures)
    event_type = random.choices(['REFUND', 'CHARGEBACK'], weights=[0.8, 0.2])[0]
    
    # The event happened days ago (Delayed Update)
    days_late = random.randint(2, 45)
    historical_time = get_current_utc() - timedelta(days=days_late)
    
    # Notice: Event time is old, but Ingestion time is NOW. 
    # This is exactly what watermark windows in Spark evaluate.
    payload = {
        "event_id": fake.uuid4(),
        "transaction_id": transaction_id,
        "event_type": event_type,
        "event_timestamp": historical_time.isoformat(),
        "ingestion_timestamp": get_current_utc().isoformat(),
        "metadata_note": "LATE_ARRIVING_DATA"
    }
    return payload

if __name__ == '__main__':
    print("Starting Advanced Payment Simulator...")
    try:
        while True:
            action_roll = random.random()
            
            # 60% chance: New transaction initiated
            if action_roll < 0.60:
                event = generate_new_authorization()
                emit_event(event)
                print(f"[AUTH] Trans: {event['transaction_id']}")
                
            # 35% chance: Existing transaction captured (Linked Event)
            elif action_roll < 0.95 and active_transactions:
                txn_to_capture = random.choice(list(active_transactions.keys()))
                event = progress_transaction_to_capture(txn_to_capture)
                emit_event(event)
                print(f"[CAPTURE] Trans: {event['transaction_id']}")
                
            # 5% chance: Historical update arrives late (Refund/Chargeback)
            else:
                event = generate_delayed_update()
                if event:
                    emit_event(event)
                    print(f"[{event['event_type']}] LATE ARRIVAL Trans: {event['transaction_id']}")

            producer.poll(0)
            time.sleep(random.uniform(0.1, 0.8)) # Variable load simulation
            
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print("Shutdown complete.")
        
# ________________________________________________________________________________________________



import json 
from math import e
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
    
    merchant_id = random.choice(MERCHANTS)
    
    currency = fake.random_element(elements=('USD', 'EGP', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF'))
    amount = round(random.uniform(5.00, 1500.00), 8)
    user_id = fake.random_int(min=10000, max=99999)
    
    payload = {
        'transaction_id': transaction_id, 
        'idempotency_key': idempotency_key, 
        
        
        'merchant_id': merchant_id, 
        'amount': amount, 
        'currency': currency,
        'user_id': user_id, 
        
        'country': random.choice(COUNTRIES),
        
        'source_system': random.choice(SOURCE_SYSTEMS), 
        'payment_method': random.choices(PAYMENT_METHODS, weights=METHOD_WEIGHTS, k=1)[0],
        

    }
    
    events_to_emit = []
    for initial_status in ['payment_intent_created', 'pending']:
        event_payload = payload.copy()  
        event_payload['status'] = initial_status
        event_payload['event_id'] = fake.uuid4()
        events_to_emit.append(event_payload)
        time.sleep(0.05) # Tiny micro-delay between webhooks
    
    
    decision_event = payload.copy()
    decision_event['event_id'] = fake.uuid4()
    decision_event['timestamp'] = datetime.now(timezone.utc).isoformat()
    
    if random.random() < 0.85: # 85% chance of success
        decision_event['status'] = 'authorized'
        pending_authorizations[transaction_id] = decision_event # Store for later capture
        
    else:
        decision_event['status'] = 'failed'
        decision_event['failure_code'] = random.choices(FAILURE_CODES, weights=FAILURE_WEIGHTS, k=1)[0]
        # Failed transactions won't have captures.
    events_to_emit.append(decision_event)
    
    
    return events_to_emit


def progress_transaction_to_capture(transaction_id):
    """Step 2: The funds actually move (Linked Event)."""
    base_payload = pending_authorizations.pop(transaction_id) # Remove from pending, move to captured
    captured_transactions[transaction_id] = base_payload # Store in captured for potential refunds/chargebacks
    
    # Clone base payload but update the state
    capture_payload = base_payload.copy()
    capture_payload["event_id"] = fake.uuid4() # New Kafka event ID
    capture_payload["status"] = "captured"
    
    # Simulate processing delay (event happened 2 seconds ago, ingested now)
    event_time = datetime.now(timezone.utc) - timedelta(seconds=random.randint(1, 5))
    capture_payload["timestamp"] = event_time.isoformat()
    
    return capture_payload

def generate_late_lifecycle_event():
    """Handles Settled, Refunded, and Chargeback (Days Later)."""
    if not captured_transactions:
        return None
        
    transaction_id = random.choice(list(captured_transactions.keys()))
    base_payload = captured_transactions[transaction_id]
    
    # Decide if this is a settlement, refund, or chargeback
    event_type = random.choices(['settled', 'refund', 'chargeback'], weights=[0.70, 0.25, 0.05])[0]
    
    late_event_payload = base_payload.copy()
    late_event_payload['event_id'] = fake.uuid4()
    
    # The event happened days ago (Late Lifecycle Event)
    days_late = random.randint(2, 45)
    historical_time = datetime.now(timezone.utc) - timedelta(days=days_late)
    late_event_payload['timestamp'] = historical_time.isoformat()
    
    if event_type == 'settled':
        late_event_payload['status'] = 'settled'
        # Once settled, we can remove from captured as it's fully complete
        captured_transactions.pop(transaction_id)
        
    elif event_type == 'refund':
        late_event_payload['status'] = 'refunded'
        # Refunds can still be charged back later, so we keep it in captured for now
        
    else: # Chargeback
        late_event_payload['status'] = 'chargeback'
        # Chargebacks are final, remove from captured
        captured_transactions.pop(transaction_id)
    
    return late_event_payload


