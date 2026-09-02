# Development Guide

Notes on running, testing, and understanding the architecture of this repository.

## Commands

### Run the full stack locally (Postgres + Kafka + app)
```
docker compose up --build
```
App + web UI: http://localhost:8000 — interactive API docs at `/docs`.

### Run tests
`tests/test_regressions.py` needs nothing running — it inspects the transfer
query and retry policy directly:
```
pytest tests/test_regressions.py -v
```

`tests/test_ledgercore.py` is an integration suite against a live server on
**port 8001** (not the docker-compose app on 8000, and not `TestClient`-based).
Start the app separately first:
```
uvicorn main:app --port 8001
pytest tests/test_ledgercore.py -v
```
Run a single test:
```
pytest tests/test_ledgercore.py::test_transfer_completes -v
```

### Chaos/load test
Fires 50 concurrent transfers at the same account to exercise the
SERIALIZABLE retry path. Also expects the app on port 8001:
```
python tests/chaos_test.py
```

### Rebuild after changing backend or frontend code
The Docker image bakes in a copy of the code at build time (no live volume
mount) — a container restart alone won't pick up changes to `*.py` or
`static/index.html`:
```
docker compose up --build -d app
```

### Wipe all users, accounts, and ledger history
```
python reset_ledger.py
```
Truncates every table and restarts ids at 1, so the next person to register
gets account #1. Needed because balances are derived by summing
`postings.account_id`: postings left over from before per-user accounts
existed aren't owned by anyone, and would otherwise become the opening
balance of whoever registers into that account number.

### Reset local data after a model/schema change
`Base.metadata.create_all()` only creates missing tables — it never alters
existing ones. After adding/changing a column, wipe and recreate the volume:
```
docker compose down -v
docker compose up --build -d
```

## Deployment troubleshooting

### "STARTUP FAILED: cannot reach the database at ..."
The app could not open a connection, so it refused to start. This is
deliberate -- a ledger that cannot reach its database should fail loudly
rather than serve requests it cannot record. Check, in order:

1. `DATABASE_URL` is set on the service, and starts with
   `postgresql+asyncpg://`. Managed providers hand out `postgres://...`;
   that prefix has to be changed or SQLAlchemy cannot load the driver.
2. The database still exists. Free-tier databases are often deleted after a
   period of inactivity, which leaves the hostname unresolvable -- the
   underlying error is `Name or service not known`.
3. The host is reachable from the service. An "internal" connection string
   usually only resolves from inside the same provider region; from
   anywhere else the hostname means nothing.

`Name or service not known` means DNS: the hostname is wrong or gone.
`Connection refused` means the host exists but nothing is listening there.

### Connecting through a managed provider (Supabase, Neon)

Managed Postgres providers front the database with a connection pooler
(PgBouncer). In transaction pooling mode a client does not keep the same
server connection between statements, so server-side prepared statements
break -- and asyncpg uses them by default. It does not fail at startup; it
fails intermittently under concurrency with:

    DuplicatePreparedStatementError: prepared statement
    "__asyncpg_stmt_1__" already exists

`database.py` detects a pooler from the host (`pooler.`, `pgbouncer`, or
port 6543) and disables both statement caches when it sees one, logging a
warning at startup so the mode is visible. `DB_DISABLE_PREPARED_STATEMENTS=1`
or `=0` overrides the detection either way.

Prefer a direct (non-pooled) connection string where the network allows it:
prepared statements stay on and queries are measurably faster. Note that
Supabase's direct endpoint is IPv6-only unless the IPv4 add-on is enabled,
so an IPv4-only host has to use the pooler.

Whichever string you use, change the `postgres://` prefix that providers
hand out to `postgresql+asyncpg://`, or SQLAlchemy cannot load the driver.

### "STARTUP FAILED: connected to ... but could not prepare the schema"
The connection worked, so `DATABASE_URL` is fine. Something in
`prepare_database()` failed -- usually a migration that cannot apply to the
data already present, such as adding a UNIQUE constraint to a column that
already holds duplicates.

### "No open ports detected"
A host that scans for a bound port reports this when the app exits during
startup. It is a symptom, not the cause -- the real error is above it in the
log.

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

### Auth and account ownership
JWT access tokens (30 min) + refresh tokens (7 days), both signed with
`SECRET_KEY` (env var, `auth.py`). `POST /login` returns both tokens.

`/transfers` and every `/accounts/*` route depends on `get_current_user`.
Two helpers in `main.py` do the actual scoping:

- `get_own_account(db, user)` — the one account whose `owner_id` is this
  user. `accounts.owner_id` is `UNIQUE`, so "one account per user" is a
  database constraint, not a convention to remember at each call site.
- `authorize_account_access(id, db, user)` — used by every
  `/accounts/{id}/*` route. Raises **404, not 403**, for an account you
  don't own: a 403 would confirm the account exists, which leaks
  information given ids are sequential.

In `/transfers` the ownership check runs *once, before* the retry loop. It
depends only on the caller and the request body, so a serialization conflict
can't change the answer, and re-running it per attempt would just repeat two
queries.

`/system/*` is deliberately left open — both endpoints report ledger-wide
totals and neither exposes an individual account.

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

`MAIN_ACCOUNT_ID` is filled in by `loadMyAccount()` from `/accounts/me`
immediately after login — it is never hardcoded, and `enterApp()` awaits it
before showing any panel, since every panel needs it. There's no account
picker: you can only read your own account, so there'd be nothing to pick.

Two auth-related traps in this file:

- `callApi()` attaches the `Authorization` header centrally. Adding a new
  call that bypasses it will 401.
- The CSV statement can't be a plain `<a href>` — a browser navigation
  carries no `Authorization` header, so it would 401. `downloadStatement()`
  fetches it with the header and hands the browser a blob instead.

### Timestamps aren't on every model
Only `PostingModel`, `AccountModel`, `OutboxModel`, `ProcessedEventModel`,
and `AuditLogModel` have `created_at`. `TransactionModel` does not.

### Schema changes to already-deployed tables
`create_all()` never alters an existing table, so a column or constraint
added to a model afterwards has to be patched in explicitly in `lifespan()`
— that's what the `ALTER TABLE ... IF NOT EXISTS` calls and the `DO $$` block
for the `accounts` constraints are doing. Adding a column to a model without
one of these will work on a fresh database and fail on an existing one,
which is exactly how the `accounts.created_at` addition first broke
`/register`.
