# LedgerCore

A distributed double-entry ledger and settlement engine, built with FastAPI, async SQLAlchemy, PostgreSQL, and Kafka.

## The core guarantee

Every transfer writes two permanent lines — one debit, one credit — that always net to zero. Balances are never stored directly; they're derived by summing an account's postings. The invariant that matters, always:

```
SUM(all debits) == SUM(all credits)
```

## Architecture

```
Client → FastAPI app → Postgres (accounts, transactions, postings)
                     → Outbox table (written in the same commit as the transfer)
                          → Outbox poller
                               → Kafka topic
                                    → Idempotent consumer
```

- **Transfers** run under `SERIALIZABLE` isolation with a retry loop, so two concurrent transfers touching the same account are detected and safely retried, never silently corrupted.
- **Idempotency keys** protect against duplicate submissions (e.g. a client retry after a dropped connection) — the same key always returns the original result.
- **The outbox pattern** guarantees a transfer and its downstream event are recorded atomically — either both happen or neither does.
- **The consumer** deduplicates by event id, so Kafka's at-least-once delivery can't cause double-processing.
- **A hash-chained audit log** makes every transaction's history tamper-evident — editing a past transaction breaks the chain from that point forward, and `/system/audit-verify` detects exactly where.

## Running it

```bash
docker compose up --build
```

This brings up the FastAPI app, Postgres, and Kafka together. The app is available at `http://localhost:8000`, with interactive docs at `/docs`.

## What's tested

- `pytest test_ledgercore.py -v` — covers successful transfers, idempotency-key deduplication, the system-wide invariant, and safe behavior under concurrent load
- **Chaos/load test**: 50 concurrent transfers fired at the same account under `SERIALIZABLE` isolation. Every request returned either a completed transfer or a clean `409` — zero unhandled failures, invariant held throughout. (An earlier version of this test surfaced a real bug: the retry loop's exception handler was too narrow and didn't catch Postgres's actual `SerializationError`, causing conflicts to crash as 500s instead of retrying — fixed by broadening the caught exception types.)

## Stack

FastAPI · async SQLAlchemy 2.0 · PostgreSQL · Kafka (aiokafka) · Docker · pytest

## API overview

| Route | Purpose |
|---|---|
| `POST /register`, `POST /login`, `POST /refresh` | Auth — JWT access/refresh tokens |
| `POST /transfers` | Create a transfer (idempotent, SERIALIZABLE-safe) |
| `GET /accounts/{id}/balance` | Derived balance for an account |
| `GET /accounts/{id}/transactions` | Transaction history for an account |
| `GET /system/invariant-check` | System-wide debit/credit balance check |
| `GET /system/audit-verify` | Verifies the hash-chained audit log is intact |
