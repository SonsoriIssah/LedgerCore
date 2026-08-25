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

- **Login / registration**, plus a one-click *Use a test account* button that creates a fresh account and signs straight in
- **Dashboard** real balance, transaction count, and recent activity for your account
- **Accounts** full transaction history with a running balance, plus a CSV statement export
- **Transfers** a confirm-before-send flow; the source is always your own account, and you enter the recipient's account number
- **Activity** your postings, newest first
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

- `pytest` runs two small, fast suites that need nothing running:
  `tests/test_regressions.py` pins down two performance fixes that are easy to
  undo by accident (see *Performance notes* below) — both were verified to fail
  when their fix is reverted.
- `pytest tests/test_ledgercore.py -v` covers successful transfers,
  idempotency-key deduplication, the system-wide invariant, and safe behavior
  under concurrent load. These are integration tests: they need the app running
  on `localhost:8001` (see `DEVELOPMENT.md`).
- **Chaos/load test** (`python tests/chaos_test.py`): 50 concurrent transfers
  fired at the same account under `SERIALIZABLE` isolation. Every request
  returned either a completed transfer or a clean `409` — zero unhandled
  failures, invariant held throughout. (An earlier version of this test
  surfaced a real bug: the retry loop's exception handler was too narrow and
  didn't catch Postgres's actual serialization error, causing conflicts to
  crash as 500s instead of retrying — fixed by broadening the caught
  exception types.)

## Performance notes

Two fixes, both measured on a local single-worker setup against PostgreSQL 16
in Docker, with Kafka absent:

- **The audit-chain lookup is bounded (`LIMIT 1`).** It reads only the newest
  `audit_log` row. Without the limit, Postgres returned the entire table and
  SQLAlchemy built an ORM object per row, so each transfer's cost grew with
  the ledger's whole history — roughly 48 ms with an empty table rising to
  1.6 s at 75,000 entries. With it, cost stays flat (~35 ms). The existing
  `ix_audit_log_id` index already serves this as an index scan; no new index
  was needed.
- **Retries back off with jitter.** Transfers retry up to 3 times on
  `SERIALIZABLE` conflicts. They previously retried instantly and re-collided
  in lockstep; a short jittered delay (30 ms base, exponential, capped so the
  total added wait stays under 100 ms) spreads them out instead.

SQL statement logging is off by default and enabled with `SQL_ECHO=1` — it
writes several lines per query, which is useful locally and costly in
production.

### Known limitation: the audit chain is a global write bottleneck

Every transfer reads the single newest `audit_log` row and appends the next
one, so under `SERIALIZABLE` **every transfer conflicts with every other
transfer ledger-wide** — not just those touching the same accounts. Confirmed
directly: 500 concurrent transfers in which every request used a distinct,
non-overlapping account pair still produced conflicts on the large majority.

Practically, this means ledger-wide write throughput is capped by one
serialized append point, and adding workers, connections, or CPU will not
raise that ceiling. Under heavy concurrency most transfers exhaust their
retries and return a clean `409` rather than failing unsafely — no invariant
was ever violated in testing — but the ceiling is real.

Fixing it properly means taking the global chain off the write path (for
example per-account chains, or moving hash-chaining to an asynchronous worker
fed by the existing outbox). That is a design change and has **not** been
attempted here.

## Stack

FastAPI · async SQLAlchemy 2.0 · PostgreSQL · Kafka (aiokafka) · Docker · pytest · Tailwind (CDN, frontend only)

## API overview

| Route | Purpose |
|---|---|
| `POST /register` | Create a user and their ledger account |
| `POST /login` | Returns `{access_token, refresh_token}` |
| `POST /refresh` | Exchange a refresh token for a new access token |
| `GET /accounts/me` | 🔒 Your account number, currency, and balance |
| `POST /transfers` | 🔒 Create a transfer (idempotent, SERIALIZABLE-safe) |
| `GET /accounts/{id}/balance` | 🔒 Derived balance for an account |
| `GET /accounts/{id}/summary` | 🔒 Balance + transaction count + last transaction time |
| `GET /accounts/{id}/postings` | 🔒 Recent postings for an account, newest first |
| `GET /accounts/{id}/statement` | 🔒 Full history as a downloadable CSV, with running balance |
| `GET /system/invariant-check` | System-wide debit/credit balance check |
| `GET /system/audit-verify` | Verifies the hash-chained audit log is intact |

🔒 = requires `Authorization: Bearer <access_token>`, and only ever acts on
the caller's own account.

## Accounts and ownership

Registering creates a user **and** their one ledger account, in a single
transaction. The account number is the next free id, and it is what you give
someone who wants to pay you.

From then on the API is scoped to whoever is holding the token:

- `POST /transfers` requires a token, and `from_account` must be your own
  account — sending from someone else's is a `403`. The destination can be any
  real account, which is how one user pays another. Transfers to an account
  that doesn't exist, to yourself, or for a non-positive amount are rejected.
- `GET /accounts/{id}/*` only answers for the account you own. Another user's
  account returns `404` rather than `403`, so the response can't be used to
  test whether an account number exists.
- `GET /accounts/me` tells the frontend which account is yours, so no account
  number is hardcoded anywhere in the UI.
- `GET /system/*` stays open: both endpoints report ledger-wide totals and
  neither exposes an individual account's activity.

## Known limitations

- Transfers don't check that the source account has a sufficient balance
  before debiting, so an account can go negative. Every new account starts at
  zero and there is no deposit route, so moving any money at all currently
  means going negative first.
- One account per user, fixed at registration. There's no way to open a
  second account, close one, or transfer ownership.
- `GET /system/*` is unauthenticated. Fine for a demo admin console, but it
  does mean anyone can read ledger-wide totals.
