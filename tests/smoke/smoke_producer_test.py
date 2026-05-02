import os
from dotenv import load_dotenv
load_dotenv()

from src.ingestion.producers.payment_producer import (
    create_db_pool,
    generate_initial_checkout,
    persist_event_batch,
)

import psycopg2


def run_one_shot():
    db_pool = create_db_pool()
    raw_events = generate_initial_checkout()
    print(f"Generated {len(raw_events)} raw events, transaction_id={raw_events[0]['transaction_id']}")

    inserted = persist_event_batch(db_pool, raw_events)
    print(f"persist_event_batch inserted: {inserted}")

    # Inspect outbox rows for the transaction
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT event_id, transaction_id, idempotency_key, event_type, status, ingestion_timestamp, payload->>'event_id' as payload_event_id FROM operational.payment_status_history WHERE transaction_id = %s ORDER BY ingestion_timestamp DESC LIMIT 10",
                (raw_events[0]['transaction_id'],),
            )
            rows = cur.fetchall()
            print(f"Outbox rows found: {len(rows)}")
            for r in rows:
                print(r)
    finally:
        db_pool.putconn(conn)
        db_pool.closeall()


if __name__ == '__main__':
    run_one_shot()
