import json
import os
import time
import random
from datetime import datetime, timedelta, timezone
from faker import Faker
from psycopg2 import pool
from psycopg2.extras import Json
from dotenv import load_dotenv
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

CURRENCY_MINOR_UNITS = {
    "USD": 2,
    "EGP": 2,
    "EUR": 2,
    "GBP": 2,
    "JPY": 0,
    "AUD": 2,
    "CAD": 2,
    "CHF": 2,
}

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

def create_db_pool():
    load_dotenv()

    db_host = os.getenv("POSTGRES_HOST")
    db_port = int(os.getenv("POSTGRES_PORT", "5432"))
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    min_conn = int(os.getenv("POSTGRES_POOL_MIN_CONNECTIONS", "1"))
    max_conn = int(os.getenv("POSTGRES_POOL_MAX_CONNECTIONS", "5"))

    if not all([db_host, db_user, db_password, db_name]):
        raise ValueError(
            "Missing required Postgres env vars: "
            "POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB"
        )

    return pool.ThreadedConnectionPool(
        minconn=min_conn,
        maxconn=max_conn,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        database=db_name,
    )
    
    
    
    
def to_amount_minor(amount, currency):
    scale = CURRENCY_MINOR_UNITS.get(currency, 2)
    return int(round(amount * (10 ** scale)))


def parse_iso_ts(s):
    """Parse ISO timestamp string to Python datetime or return None."""
    if s is None:
        return None
    if isinstance(s, (int, float)):
        # numeric timestamps not expected; ignore
        return None
    try:
        # Python's fromisoformat handles most ISO formats including offsets
        return datetime.fromisoformat(s)
    except Exception:
        try:
            # Fallback: try common format without offset
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f")
        except Exception:
            return None
    
def upsert_payment_intent(cursor, event):
    amount_minor = to_amount_minor(event["amount"], event["currency"])

    cursor.execute(
        """
        INSERT INTO operational.payment_intents (
            intent_id, transaction_id, idempotency_key, user_id, merchant_id,
            amount_minor, currency, status, source_system, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (transaction_id, idempotency_key)
        DO UPDATE SET
            status = EXCLUDED.status,
            updated_at = NOW()
        """,
        (
            event.get("intent_id"),
            event["transaction_id"],
            event["idempotency_key"],
            event.get("user_id"),
            event.get("merchant_id"),
            amount_minor,
            event.get("currency"),
            event.get("status"),
            event.get("source_system"),
        ),
    )


def ensure_user_and_merchant(cursor, event):
    """Ensure referenced user and merchant exist (idempotent)."""
    user_id = event.get('user_id')
    merchant_id = event.get('merchant_id')
    if user_id:
        cursor.execute(
            """
            INSERT INTO operational.users (user_id, email)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO NOTHING
            """,
            (user_id, f"{user_id}@example.com"),
        )
    if merchant_id:
        cursor.execute(
            """
            INSERT INTO operational.merchants (merchant_id, merchant_name)
            VALUES (%s, %s)
            ON CONFLICT (merchant_id) DO NOTHING
            """,
            (merchant_id, merchant_id),
        )
    


def upsert_charge(cursor, event):
    amount_minor = to_amount_minor(event["amount"], event["currency"])

    cursor.execute(
        """
        INSERT INTO operational.charges (
            charge_id, transaction_id, idempotency_key, intent_id,
            amount_minor, status, currency, payment_method, source_system,
            created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (transaction_id, idempotency_key)
        DO UPDATE SET
            status = EXCLUDED.status,
            updated_at = NOW()
        """,
        (
            event.get("charge_id"),
            event["transaction_id"],
            event["idempotency_key"],
            event.get("intent_id"),
            amount_minor,
            event.get("status"),
            event.get("currency"),
            event.get("payment_method"),
            event.get("source_system"),
        ),
    )
    
    
def upsert_refund(cursor, event):
    amount_minor = to_amount_minor(event["amount"], event["currency"])

    cursor.execute(
        """
        INSERT INTO operational.refunds (
            refund_id, transaction_id, idempotency_key, charge_id,
            amount_minor, currency, refund_reason, is_partial, status, source_system,
            created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (transaction_id, idempotency_key)
        DO UPDATE SET
            status = EXCLUDED.status,
            updated_at = NOW()
        """,
        (
            event.get("refund_id"),
            event["transaction_id"],
            event["idempotency_key"],
            event.get("charge_id"),
            amount_minor,
            event.get("currency"),
            event.get("refund_reason"),
            event.get("is_partial", False),
            event.get("status"),
            event.get("source_system"),
        ),
    )
    
    
def upsert_chargeback(cursor, event):
    amount_minor = to_amount_minor(event["amount"], event["currency"])

    cursor.execute(
        """
        INSERT INTO operational.chargebacks (
            chargeback_id, charge_id, amount_minor, reason, filed_at, status,
            created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, NOW(), %s, NOW(), NOW())
        ON CONFLICT (chargeback_id)
        DO UPDATE SET
            status = EXCLUDED.status,
            updated_at = NOW()
        """,
        (
            event.get("chargeback_id"),
            event.get("charge_id"),
            amount_minor,
            event.get("reason"),
            event.get("status"),
        ),
    )


def normalize_entity_ids(event):
    """
    Normalize entity IDs based on event_type to satisfy one_entity constraint.
    Only the primary entity ID for the event is kept non-NULL; others are set to NULL.
    """
    event_type = event.get("event_type", "")
    normalized = {
        "intent_id": None,
        "charge_id": None,
        "refund_id": None,
        "chargeback_id": None,
    }

    if event_type.startswith("payment_intent"):
        normalized["intent_id"] = event.get("intent_id")
    elif event_type.startswith("charge") or event_type.startswith("capture"):
        normalized["charge_id"] = event.get("charge_id")
    elif event_type.startswith("refund"):
        normalized["refund_id"] = event.get("refund_id")
    elif event_type.startswith("chargeback"):
        normalized["chargeback_id"] = event.get("chargeback_id")
    elif event_type.startswith("settlement"):
        normalized["intent_id"] = event.get("intent_id")

    return normalized


def insert_outbox_event(cursor, event):
    # Convert ISO timestamp strings to Python datetimes where possible
    evt_ts = parse_iso_ts(event.get("event_timestamp"))
    ingest_ts = parse_iso_ts(event.get("ingestion_timestamp"))

    # Normalize entity IDs to satisfy one_entity constraint
    entity_ids = normalize_entity_ids(event)

    cursor.execute(
        """
        INSERT INTO operational.payment_status_history (
            transaction_id, idempotency_key, intent_id, charge_id, refund_id, chargeback_id,
            event_type, status, event_timestamp, ingestion_timestamp, source_system, payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id, idempotency_key, event_type) DO NOTHING
        """,
        (
            event["transaction_id"],
            event["idempotency_key"],
            entity_ids["intent_id"],
            entity_ids["charge_id"],
            entity_ids["refund_id"],
            entity_ids["chargeback_id"],
            event["event_type"],
            event["status"],
            evt_ts,
            ingest_ts,
            event.get("source_system"),
            Json(event),
        ),
    )
    
def write_event_to_postgres(cursor, event):
    event_type = event["event_type"]
    # Ensure referenced user/merchant exist to avoid FK violations
    ensure_user_and_merchant(cursor, event)

    if event_type in {"payment_intent_created", "payment_intent_authorized", "payment_intent_failed", "settlement_recorded"}:
        upsert_payment_intent(cursor, event)
    elif event_type == "capture_succeeded":
        upsert_charge(cursor, event)
    elif event_type == "refund_recorded":
        upsert_refund(cursor, event)
    elif event_type == "chargeback_received":
        upsert_chargeback(cursor, event)
    else:
        raise ValueError(f"Unknown event_type: {event_type}")

    insert_outbox_event(cursor, event)
    
def persist_event_batch(db_pool, raw_events, p_exact=0.05, p_retry=0.02):
    conn = db_pool.getconn()
    inserted = 0

    try:
        with conn:
            with conn.cursor() as cursor:
                for raw_event in raw_events:
                    outbound_events = get_duplicate_events(
                        raw_event,
                        p_exact=p_exact,
                        p_retry=p_retry,
                    )
                    for event in outbound_events:
                        write_event_to_postgres(cursor, event)
                        inserted += 1
        return inserted
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)
        


def run_postgres_stream():
    p_exact = float(os.getenv("PAYMENT_DUPLICATE_RATE", "0.05"))
    p_retry = float(os.getenv("PAYMENT_RETRY_RATE", "0.02"))

    db_pool = create_db_pool()
    print("Starting Postgres payment stream")

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
                inserted_count = persist_event_batch(
                    db_pool,
                    raw_events,
                    p_exact=p_exact,
                    p_retry=p_retry,
                )
                print(f"inserted_events={inserted_count}")

            time.sleep(random.uniform(0.1, 0.7))

    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        db_pool.closeall()
        print("DB pool closed.")
        
        
if __name__ == "__main__":
    run_postgres_stream()