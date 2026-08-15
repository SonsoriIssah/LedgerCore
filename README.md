# LedgerCore

A distributed double-entry ledger and settlement engine, built with FastAPI, async SQLAlchemy, PostgreSQL, and Kafka with a small web UI on top.

## The core guarantee

Every transfer writes two permanent lines one debit, one credit that always net to zero. Balances are never stored directly; they're derived by summing an account's postings. The invariant that matters, always:

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
- **Idempotency keys** protect against duplicate submissions (e.g. a client retry after a dropped connection) the same key always returns the original result.
- **The outbox pattern** guarantees a transfer and its downstream event are recorded atomically either both happen or neither does.
- **The consumer** deduplicates by event id, so Kafka's at-least-once delivery can't cause double-processing.
- **A hash-chained audit log** makes every transaction's history tamper-evident editing a past transaction breaks the chain from that point forward, and `/system/audit-verify` detects exactly where.
- **Kafka is optional at startup.** If it's unreachable, the app logs a warning and boots anyway HTTP and database routes keep working; only event publishing pauses.

## Web UI

A single-page frontend is served at `/` (plain HTML/CSS/JS, no build step, styled with Tailwind via CDN):

- **Login / registration**
- **Dashboard** real balance, transaction count, and recent activity for one account
- **Accounts** full transaction history with a running balance, plus a CSV statement export
- **Transfers** a confirm-before-send transfer flow
- **Activity** postings for any account, newest first
- **Admin Console** live invariant check, audit-chain verification, and a concurrency simulator that fires real concurrent transfers at the API to demonstrate the retry-on-conflict logic

Everything shown is real data from the API nothing on any page is mocked.

## Running it

```bash
docker compose up --build
```

This brings up the FastAPI app, Postgres, and Kafka together. The app (API + web UI) is available at `http://localhost:8000`, with interactive API docs at `/docs`.

## Deploying

Deploy target: Render (Docker). Required environment variables:

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | Postgres connection string (`postgresql+asyncpg://...`) |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address optional, the app runs fine without it |
| `SECRET_KEY` | JWT signing secret falls back to a dev-only default locally |
| `PORT` | Set automatically by Render; read by the Dockerfile's `CMD` |

All of these fall back to sane localhost defaults for local development.

## What's tested

- `pytest test_ledgercore.py -v` covers successful transfers, idempotency-key deduplication, the system-wide invariant, and safe behavior under concurrent load. Requires the app running on `localhost:8001` (a separate instance from the Docker Compose one on port 8000 see `CLAUDE.md` for the exact commands).
- **Chaos/load test** (`python chaos_test.py`): 50 concurrent transfers fired at the same account under `SERIALIZABLE` isolation. Every request returned either a completed transfer or a clean `409` zero unhandled failures, invariant held throughout. (An earlier version of this test surfaced a real bug: the retry loop's exception handler was too narrow and didn't catch Postgres's actual serialization error, causing conflicts to crash as 500s instead of retrying fixed by broadening the caught exception types.)

## Stack

FastAPI · async SQLAlchemy 2.0 · PostgreSQL · Kafka (aiokafka) · Docker · pytest · Tailwind (CDN, frontend only)

## API overview

| Route | Purpose |
|---|---|
| `POST /register` | Create a user |
| `POST /login` | Returns `{access_token, refresh_token}` |
| `POST /refresh` | Exchange a refresh token for a new access token |
| `POST /transfers` | Create a transfer (idempotent, SERIALIZABLE-safe) |
| `GET /accounts/{id}/balance` | Derived balance for an account |
| `GET /accounts/{id}/summary` | Balance + transaction count + last transaction time |
| `GET /accounts/{id}/postings` | Recent postings for an account, newest first |
| `GET /accounts/{id}/statement` | Full history as a downloadable CSV, with running balance |
| `GET /system/invariant-check` | System-wide debit/credit balance check |
| `GET /system/audit-verify` | Verifies the hash-chained audit log is intact |

## Known limitations

- No route currently enforces authentication `/transfers`, `/accounts/*`, etc. are all open. `get_current_user` exists in `auth.py`/`main.py` but isn't wired into those routes yet.
- Accounts aren't linked to the users who "own" them. The web UI shows one configurable account (default `#1`, changeable via the Settings dropdown), not a per-user account list, since there's no ownership model in the data yet.
- Transfers don't check that the source account has a sufficient balance before debiting.
