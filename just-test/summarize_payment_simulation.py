import argparse
from collections import Counter

from ingestion.producers import payment_producer as pp


def run_simulation(total_events: int, seed: int | None) -> tuple[Counter, Counter]:
    if seed is not None:
        pp.random.seed(seed)
        pp.fake.seed_instance(seed)

    pp.pending_authorizations.clear()
    pp.captured_transactions.clear()

    status_counts: Counter[str] = Counter()
    transition_counts: Counter[str] = Counter()
    last_status_by_txn: dict[str, str] = {}

    emitted = 0
    while emitted < total_events:
        action_roll = pp.random.random()
        raw_events = []

        if action_roll < 0.60:
            raw_events = pp.generate_initial_checkout()
        elif action_roll < 0.95 and pp.pending_authorizations:
            txn_to_capture = pp.random.choice(list(pp.pending_authorizations.keys()))
            capture_event = pp.progress_transaction_to_capture(txn_to_capture)
            if capture_event is not None:
                raw_events = [capture_event]
        else:
            late_event = pp.generate_late_lifecycle_event()
            if late_event is not None:
                raw_events = [late_event]

        events_batch = []
        for event in raw_events:
            events_batch.extend(pp.get_duplicate_events(event))

        for event in events_batch:
            if emitted >= total_events:
                break

            txn = event["transaction_id"]
            current = event["status"]
            prev = last_status_by_txn.get(txn)

            status_counts[current] += 1
            if prev is None:
                transition_counts[f"START -> {current}"] += 1
            else:
                transition_counts[f"{prev} -> {current}"] += 1

            last_status_by_txn[txn] = current
            emitted += 1

    return status_counts, transition_counts


def print_counter_table(title: str, counter: Counter, top_n: int | None = None) -> None:
    items = counter.most_common(top_n)
    if not items:
        print(f"\n{title}")
        print("(no rows)")
        return

    key_width = max(len(name) for name, _ in items)
    value_width = max(len(str(value)) for _, value in items)

    print(f"\n{title}")
    print(f"{'item'.ljust(key_width)} | {'count'.rjust(value_width)}")
    print(f"{'-' * key_width}-+-{'-' * value_width}")
    for name, value in items:
        print(f"{name.ljust(key_width)} | {str(value).rjust(value_width)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run payment event simulation and print compact summary tables.")
    parser.add_argument(
        "--events",
        type=int,
        default=300,
        help="Target number of emitted events (clamped to range 100..1000).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Optional random seed for deterministic output. Use -1 for non-deterministic run.",
    )
    parser.add_argument(
        "--top-transitions",
        type=int,
        default=20,
        help="How many transition rows to print.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    requested = args.events
    total_events = max(100, min(1000, requested))
    seed = None if args.seed == -1 else args.seed

    print("Payment Simulation Summary")
    print(f"requested_events={requested}")
    print(f"actual_events={total_events}")
    print(f"seed={seed}")

    status_counts, transition_counts = run_simulation(total_events=total_events, seed=seed)

    print_counter_table("Status Counts", status_counts)
    print_counter_table("Lifecycle Transitions", transition_counts, top_n=args.top_transitions)

    print("\nIn-memory pool sizes after run")
    print(f"pending_authorizations={len(pp.pending_authorizations)}")
    print(f"captured_transactions={len(pp.captured_transactions)}")


if __name__ == "__main__":
    main()
