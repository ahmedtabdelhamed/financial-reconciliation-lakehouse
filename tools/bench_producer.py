#!/usr/bin/env python3
import time
import os
import json
from pathlib import Path

from src.ingestion.producers.payment_producer import create_db_pool, generate_initial_checkout, persist_event_batch


def run(duration_s=20, out_file="/tmp/insert_times.csv"):
    db_pool = create_db_pool()
    end = time.time() + duration_s
    total = 0
    p_exact = float(os.getenv("PAYMENT_DUPLICATE_RATE", "0.05"))
    p_retry = float(os.getenv("PAYMENT_RETRY_RATE", "0.02"))

    Path(out_file).parent.mkdir(parents=True, exist_ok=True)
    f = open(out_file, "w")

    try:
        while time.time() < end:
            events = generate_initial_checkout()
            t0 = time.time()
            inserted = persist_event_batch(db_pool, events, p_exact=p_exact, p_retry=p_retry)
            t_ms = int(t0 * 1000)
            for i in range(inserted):
                f.write(f"{t_ms}\n")
            total += inserted
        print(f"Total inserted events: {total}")
    finally:
        f.close()
        db_pool.closeall()


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--duration", "-d", type=int, default=20)
    p.add_argument("--out", "-o", default="/tmp/insert_times.csv")
    args = p.parse_args()
    run(args.duration, args.out)
