## Financial Reconciliation Lakehouse

Transactional outbox + CDC foundation for a financial reconciliation lakehouse.

This repository currently implements the ingestion side of the platform:

- a synthetic payment producer that generates realistic transaction lifecycles
- Postgres as the operational store and transactional outbox
- Debezium to stream outbox rows into Kafka
- a smoke test that validates the end-to-end data flow

The broader target architecture for this project is:

producer -> Postgres -> Debezium CDC -> Kafka -> Spark microbatches -> dbt models -> Delta lakehouse -> downstream reconciliation consumers

## What This Repo Does Today

- Generates payment events such as `payment_intent_created`, `payment_intent_authorized`, `capture_succeeded`, `refund_recorded`, and `chargeback_received`
- Writes operational records and outbox rows in the same Postgres transaction
- Enforces idempotency on domain rows and outbox inserts
- Publishes the outbox table through Debezium into Kafka
- Provides a smoke test for validating the flow locally

## Architecture

The current implementation uses a transactional outbox pattern:

1. The producer generates synthetic payment events.
2. The producer writes domain rows to `operational.*` tables in Postgres.
3. The producer inserts a normalized row into `operational.payment_status_history`.
4. Debezium watches the outbox publication and streams committed rows to Kafka.
5. Downstream consumers can read Kafka and build reconciliation and analytics layers from there.

The Postgres schema and publication are defined in [`docker/postgres/init.sql`](docker/postgres/init.sql).
The Debezium connector configuration lives in [`docker/connect/postgres-connector.json`](docker/connect/postgres-connector.json).

## Repository Layout

- [`src/ingestion/producers/payment_producer.py`](src/ingestion/producers/payment_producer.py) - synthetic payment event generator and Postgres writer
- [`tests/smoke/smoke_producer_test.py`](tests/smoke/smoke_producer_test.py) - one-shot smoke test for the producer and outbox
- [`docker/docker-compose.yml`](docker/docker-compose.yml) - local stack for Postgres, Kafka, and Debezium
- [`docker/postgres/init.sql`](docker/postgres/init.sql) - operational schema and publication setup
- [`docker/connect/postgres-connector.json`](docker/connect/postgres-connector.json) - Debezium connector config

## Prerequisites

- Python 3.11+
- `uv`
- Docker and Docker Compose

## Local Setup

Install Python dependencies:

```bash
uv sync
```

Start the local data stack:

```bash
docker compose -f docker/docker-compose.yml up -d postgres kafka connect
```

The default local Postgres settings are:

- host: `localhost`
- port: `5432`
- database: `operational_db`
- user: `postgres`
- password: `postgres`

## Run The Smoke Test

```bash
POSTGRES_HOST=localhost \
POSTGRES_PORT=5432 \
POSTGRES_USER=postgres \
POSTGRES_PASSWORD=postgres \
POSTGRES_DB=operational_db \
uv run python3 tests/smoke/smoke_producer_test.py
```

The test will:

- generate a small batch of payment events
- persist them into Postgres
- write outbox rows into `operational.payment_status_history`
- print the inserted outbox rows for inspection

## Consume CDC Events From Kafka

After the smoke test writes data, you can verify the CDC stream with Kafka console consumer:

```bash
docker compose -f docker/docker-compose.yml exec -T kafka \
	/kafka/bin/kafka-console-consumer.sh \
	--bootstrap-server localhost:9092 \
	--topic postgres-db.operational.payment_status_history \
	--from-beginning \
	--max-messages 5 \
	--timeout-ms 10000 \
	--property print.key=true \
	--property print.value=true
```

## Performance Notes

Local benchmark results from the current implementation:

- ingestion throughput: about 108-133 transactions/sec without the generator sleep
- CDC lag: about 7 seconds end-to-end in the local Docker stack
- outbox scan throughput: 75,000+ events/sec for simple batch reads

These are local development numbers, not production capacity numbers.

## Current Scope And Roadmap

This repository currently focuses on the ingestion and CDC foundation.

Planned next layers for the broader financial reconciliation project:

- Spark microbatch processing
- dbt transformation models
- Airflow orchestration
- Delta tables in a cloud-backed lakehouse
- Terraform for infrastructure provisioning

## Notes

- The outbox table uses a `one_entity` constraint to keep each event tied to a single primary entity.
- Multi-entity events are normalized before insert so the outbox remains consistent.
- The project is intentionally designed as a realistic financial reconciliation foundation rather than a simple demo pipeline.
