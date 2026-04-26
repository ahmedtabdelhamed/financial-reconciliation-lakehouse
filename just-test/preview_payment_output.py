import json
import random

from ingestion.producers import payment_producer as pp


def pretty_print(event):
    print(json.dumps(event, indent=2))


def main():
    # Optional seeds for repeatable demo output.
    random.seed(42)
    pp.random.seed(42)
    pp.fake.seed_instance(42)

    pp.pending_authorizations.clear()
    pp.captured_transactions.clear()

    print("=== Initial Checkout Events ===")
    initial_events = pp.generate_initial_checkout()
    for idx, event in enumerate(initial_events, start=1):
        print(f"\nEvent {idx}")
        pretty_print(event)

    decision = initial_events[-1]
    txn_id = decision["transaction_id"]

    if decision["status"] == "authorized":
        print("\n=== Capture Event ===")
        capture_event = pp.progress_transaction_to_capture(txn_id)
        pretty_print(capture_event)

        print("\n=== Late Lifecycle Event ===")
        late_event = pp.generate_late_lifecycle_event()
        if late_event is None:
            print("No late event generated yet.")
        else:
            pretty_print(late_event)
    else:
        print("\nDecision status is failed, so no capture/late event for this transaction.")

    print("\n=== In-Memory Pools ===")
    print("pending_authorizations:", len(pp.pending_authorizations))
    print("captured_transactions:", len(pp.captured_transactions))


if __name__ == "__main__":
    main()
