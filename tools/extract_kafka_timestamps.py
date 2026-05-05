#!/usr/bin/env python3
import json
import sys
from pathlib import Path


def main(out_path: str):
    values = []
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        ts = obj.get("ingestion_timestamp")
        if ts:
            values.append(ts)
    Path(out_path).write_text("\n".join(values))
    print(f"captured={len(values)}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: extract_kafka_timestamps.py <out_path>")
        raise SystemExit(2)
    main(sys.argv[1])
