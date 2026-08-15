# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Run the full stack locally (Postgres + Kafka + app)
```
docker compose up --build
```
App + web UI: http://localhost:8000 — interactive API docs at `/docs`.

### Run tests
Tests are integration tests against a live server on **port 8001** (not the
docker-compose app on 8000, and not `TestClient`-based). Start the app
separately first:
```
uvicorn main:app --port 8001
pytest test_ledgercore.py -v
```
Run a single test:
```
pytest test_ledgercore.py::test_transfer_completes -v
```

### Chaos/load test
Fires 50 concurrent transfers at the same account to exercise the
SERIALIZABLE retry path. Also expects the app on port 8001:
```
python chaos_test.py
```

### Rebuild after changing backend or frontend code
The Docker image bakes in a copy of the code at build time (no live volume
mount) — a container restart alone won't pick up changes to `*.py` or
`static/index.html`:
```
docker compose up --build -d app
```

### Reset local data after a model/schema change
`Base.metadata.create_all()` only creates missing tables — it never alters
existing ones. After adding/changing a column, wipe and recreate the volume:
```
docker compose down -v
docker compose up --build -d
```

## Architecture

### The core guarantee
Every transfer writes two permanent postings (one DEBIT, one CREDIT) that
always net to zero. Account balances are never stored directly — they're
always derived by summing an account's postings (`compute_balance()` in
`main.py`). The one invariant that must always hold — `SUM(debits) ==
SUM(credits)` — is checked live at `/system/invariant-check`.

### Transfer flow (`POST /transfers` in `main.py`)
1. Runs under `SERIALIZABLE` isolation, retrying up to 3 times on
   `OperationalError`/`DBAPIError` — Postgres detects the conflict when two
   transfers touch the same account concurrently, and the loser retries.
2. Checks `idempotency_key` first — a duplicate key returns the original
   transaction instead of creating a new one.
3. Creates the transaction, two postings, an outbox event, and an audit log
   entry, all in one commit.

### The outbox pattern (event delivery)
Kafka events are never published inline inside `/transfers` — they're
written to the `outbox` table in the *same transaction* as the transfer.
A separate background poller (`outbox_poller`, runs every 5s) picks up
unpublished rows and sends them. This means a transfer can never succeed
while silently failing to record its event, and Kafka being down doesn't
roll back a transfer. `outbox_consumer` dedupes by Kafka offset via the
`processed_events` table to survive Kafka's at-least-once delivery.

### Kafka is optional at runtime
`lifespan()` wraps `AIOKafkaProducer.start()` in try/except — if Kafka isn't
reachable, `producer` stays `None`, and `outbox_poller`/`outbox_consumer`
both no-op instead of crashing the app. The rest of the API works normally
either way. Don't assume Kafka is up when touching this code path.

### The audit hash chain
Every `audit_log` entry's hash is computed from its own transaction data
plus the *previous* entry's hash (`compute_entry_hash` in `main.py`).
`/system/audit-verify` walks the chain from `GENESIS_HASH` and reports the
first entry where it breaks — that's how tampering with old data gets
detected. If you reset local data, note the chain restarts from genesis, so
old and new data can't be mixed mid-chain meaningfully.

### Auth
JWT access tokens (30 min) + refresh tokens (7 days), both signed with
`SECRET_KEY` (env var, `auth.py`). `POST /login` returns both tokens.
No route currently enforces authentication — `get_current_user` exists in
`main.py` but isn't wired into `/transfers`, `/accounts/*`, etc. That's a
real, known gap, not something to silently "fix" without checking first.

### Frontend (`static/index.html`)
Single HTML file, no build step — vanilla JS + Tailwind via CDN, served by
FastAPI at `/` (see the `frontend()` route in `main.py`). Session
persistence: the refresh token (not the access token) is stored in
`localStorage`; on page load, `restoreSession()` exchanges it for a fresh
access token via `/refresh`, so a page reload doesn't bounce the user to
the login screen.

Two things worth knowing before editing the `<style>` block: Tailwind's CDN
script injects its generated stylesheet *after* this file's own inline
`<style>` block in the DOM, so a custom rule that needs to beat a Tailwind
utility on the same element (e.g. hiding a `flex` element) needs
`!important` — matching specificity alone loses to source order. The
sidebar's mobile slide-in animation is driven by plain CSS instead of
Tailwind's `translate-x` utilities for the same reason. Also: flex children
default to `min-width: auto`, so anything with `flex-1` next to a button
(inputs, especially) needs `min-w-0` too, or it'll refuse to shrink and
overflow on narrow screens.

The Dashboard and Accounts pages both show a single hardcoded account
(`MAIN_ACCOUNT_ID` in the script, default `1`) — there's no per-user account
ownership in the data model, so this is a deliberate simplification,
changeable at runtime via the Settings dropdown, not a bug.

### Timestamps aren't on every model
Only `PostingModel`, `OutboxModel`, `ProcessedEventModel`, and
`AuditLogModel` have `created_at`. `TransactionModel` does not.
