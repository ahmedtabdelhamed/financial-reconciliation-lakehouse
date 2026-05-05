#!/usr/bin/env python3
import sys
import statistics
from datetime import datetime, timezone


def parse_iso_to_ms(s: str):
    # Debezium emits ISO timestamps like 2024-01-01T00:00:00.123456Z
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def read_lines(path):
    with open(path, "r") as f:
        return [l.strip() for l in f if l.strip()]


def main(insert_file, msg_file):
    insert_times = [int(x) for x in read_lines(insert_file)]
    msg_times_iso = read_lines(msg_file)
    msg_times = [parse_iso_to_ms(x) for x in msg_times_iso]

    n = min(len(insert_times), len(msg_times))
    if n == 0:
        print("No paired messages to compare")
        return

    lags = [msg_times[i] - insert_times[i] for i in range(n)]
    print(f"Paired messages: {n}")
    print(f"min={min(lags)} ms, p50={statistics.median(lags):.1f} ms, mean={statistics.mean(lags):.1f} ms, p95={sorted(lags)[int(0.95*n)-1]} ms, max={max(lags)} ms")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: compute_lag.py <insert_times.csv> <message_ingestion_timestamps.txt>")
        sys.exit(2)
    main(sys.argv[1], sys.argv[2])
