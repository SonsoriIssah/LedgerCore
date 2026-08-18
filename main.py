# This is the main file of the app. It starts the API, connects to Kafka,
# and defines all the routes (endpoints) that users can call.

from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from database import engine, get_db, Base, AsyncSessionLocal, KAFKA_BOOTSTRAP_SERVERS
from pydantic import BaseModel
from sqlalchemy import select, text
from models import UserModel, AccountModel, TransactionModel, PostingModel, OutboxModel, ProcessedEventModel, AuditLogModel
from auth import hash_password, verify_password, create_access_token, decode_access_token, create_refresh_token
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError
import json, asyncio, http, uuid, logging, csv, io, random
from sqlalchemy.exc import OperationalError, DBAPIError, IntegrityError
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import hashlib

logger = logging.getLogger(__name__)

# Holds the Kafka producer once it connects. Stays None if Kafka is not available.
producer = None

# Runs in the background. Every 5 seconds, it looks for unsent events in the
# outbox table and sends them to Kafka. If Kafka is not connected, it does nothing.
async def outbox_poller():
    if producer is None:
        logger.warning("Outbox poller not started: Kafka producer unavailable")
        return
    while True:
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(select(OutboxModel).where(OutboxModel.published == False))
                events = result.scalars().all()

                for event in events:
                    await producer.send_and_wait("ledger-events", event.payload.encode('utf-8'))
                    event.published = True

                await db.commit()
        except Exception:
            # A transient Kafka/DB error here must not kill this loop -- unpublished
            # events just stay unpublished and get retried on the next cycle.
            logger.exception("Outbox poller failed to publish events; will retry next cycle")

        await asyncio.sleep(5)

# Runs in the background. Reads events from Kafka and marks each one as processed,
# so the same event is never handled twice. If Kafka is not connected, it does nothing.
async def outbox_consumer():
    if producer is None:
        logger.warning("Outbox consumer not started: Kafka producer unavailable")
        return
    consumer = AIOKafkaConsumer(
        "ledger-events",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="ledger-consumer-group"
    )
    await consumer.start()
    try:
        async for message in consumer:
            event_data = json.loads(message.value.decode('utf-8'))
            event_id = message.offset

            async with AsyncSessionLocal() as db:
                try:
                    # Try to record this event as processed. If it is already recorded,
                    # this fails (because event_id must be unique) and we just skip it.
                    processed = ProcessedEventModel(event_id=event_id)
                    db.add(processed)
                    await db.commit()
                except Exception:
                    await db.rollback()
                    continue

                print(f"Processing event: {event_data}")
    finally:
        await consumer.stop()
# This runs once when the app starts, and once when it shuts down.
# It creates the database tables, connects to Kafka, and starts the background tasks.
@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # create_all() only creates missing tables -- it never adds columns to
        # ones that already exist. Patch in columns added after a table was
        # already deployed, so a model change like this doesn't need a manual
        # migration step against production. Safe to run every startup: it's
        # a no-op once the column exists.
        await conn.execute(text(
            "ALTER TABLE postings ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()"
        ))
        await conn.execute(text(
            "ALTER TABLE accounts ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()"
        ))
        # One account per user, and every account owned by a real user. These
        # are constraints rather than columns, so there is no ADD ... IF NOT
        # EXISTS form -- hence the explicit catalog check. Databases created
        # after these were added to models.py already have them and skip this.
        await conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'accounts_owner_id_key') THEN
                    ALTER TABLE accounts ADD CONSTRAINT accounts_owner_id_key UNIQUE (owner_id);
                END IF;
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'accounts_owner_id_fkey') THEN
                    ALTER TABLE accounts ADD CONSTRAINT accounts_owner_id_fkey
                        FOREIGN KEY (owner_id) REFERENCES "user"(id);
                END IF;
            END $$;
        """))
    try:
        # Try to connect to Kafka. If it fails, keep the app running without it,
        # so the API still works even if Kafka is down or not set up yet.
        candidate_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        await candidate_producer.start()
        producer = candidate_producer
    except Exception:
        logger.warning("Kafka unavailable at %s; starting without event publishing", KAFKA_BOOTSTRAP_SERVERS, exc_info=True)
        # Shut the failed producer down explicitly. Without this its internal
        # background task keeps retrying the unreachable broker forever,
        # logging on every cycle for the life of the process.
        try:
            await candidate_producer.stop()
        except Exception:
            pass
        producer = None
    poller_task = asyncio.create_task(outbox_poller())
    consumer_task = asyncio.create_task(outbox_consumer())
    yield
    # Code after "yield" runs when the app is shutting down.
    poller_task.cancel()
    consumer_task.cancel()
    if producer is not None:
        await producer.stop()
app = FastAPI(lifespan=lifespan)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl='login')

# Serve the simple frontend page at the root URL.
@app.get('/', include_in_schema=False)
async def frontend():
    return FileResponse('static/index.html')


# What a client must send to register or log in.
class User(BaseModel):
    email: str
    password: str

# What a client must send to get a new access token.
class RefreshRequest(BaseModel):
    refresh_token: str

# What a client must send to move money between two accounts.
class TransferRequest(BaseModel):
    from_account: int
    to_account: int
    amount: float
    idempotency_key: str

# Create a new user, plus the one ledger account that belongs to them.
# Both are written in a single transaction: a user without an account would
# have nowhere to hold money and no way to receive a transfer, so a partial
# result here is never useful.
@app.post('/register')
async def register(user: User, db=Depends(get_db)):
    password = hash_password(user.password)
    db_user = UserModel(email=user.email, hashed_password=password, role='user')
    db.add(db_user)
    try:
        # flush (not commit) to get the generated user id while staying inside
        # the same transaction as the account insert below.
        await db.flush()
        account = AccountModel(owner_id=db_user.id, currency='USD')
        db.add(account)
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail='Email already registered')
    await db.refresh(db_user)
    await db.refresh(account)
    return {
        'id': db_user.id,
        'email': db_user.email,
        'role': db_user.role,
        'account_id': account.id,
    }


# Look up the one account belonging to a user. Every registered user has
# exactly one, but users created before accounts existed do not -- hence the
# explicit error rather than an attribute access on None.
async def get_own_account(db, user: UserModel) -> AccountModel:
    result = await db.execute(select(AccountModel).where(AccountModel.owner_id == user.id))
    account = result.scalars().first()
    if account is None:
        raise HTTPException(status_code=404, detail='No account found for this user')
    return account

# Check a user's email and password, and give back an access token if they are correct.
@app.post('/login')
async def login(user: User, db=Depends(get_db)):
    result = await db.execute(select(UserModel).where(UserModel.email == user.email))
    db_user = result.scalars().first()
    if not db_user:
        raise HTTPException(status_code=401, detail='Invalid email or password')
    password = verify_password(user.password, db_user.hashed_password)
    if not password:
        raise HTTPException(status_code=401, detail='Invalid email or password')
    return {
        'access_token': create_access_token({'sub': user.email}),
        'refresh_token': create_refresh_token({'sub': user.email}),
    }

# Read the access token from the request and return the matching user.
# Other routes use this to know who is calling them.
async def get_current_user(token: str = Depends(oauth2_scheme), db=Depends(get_db)):
    try:
        payload = decode_access_token(token)
        email = payload.get('sub')
        token_type = payload.get('type')
    except JWTError:
        raise HTTPException(status_code=401, detail='Could not validate credentials')
    if token_type != 'access':
        raise HTTPException(status_code=401, detail='Invalid token type')
    result = await db.execute(select(UserModel).where(UserModel.email == email))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=401, detail='Could not validate credentials')
    return user

# Use a refresh token to get a brand new access token, without logging in again.
@app.post('/refresh')
async def refresh(token: RefreshRequest, db=Depends(get_db)):
    try:
        payload = decode_access_token(token.refresh_token)
        email = payload.get('sub')
        token_type = payload.get('type')
    except JWTError:
        raise HTTPException(status_code=401, detail='Could not validate credentials')
    if token_type != 'refresh':
        raise HTTPException(status_code=401, detail='Invalid token type')
    result = await db.execute(select(UserModel).where(UserModel.email == email))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=401, detail='Could not validate credentials')
    return create_access_token({'sub': user.email})


# Build a check that only lets a user through if they have the right role.
# Example: require_role("admin") blocks anyone who is not an admin.
def require_role(required_role: str):
    async def role_checker(current_user=Depends(get_current_user)):
        if current_user.role != required_role:
            raise HTTPException(status_code=403, detail="Not authorized")
        return current_user
    return role_checker

# The starting hash for the audit trail. The very first entry links back to this.
GENESIS_HASH = "0" * 64

# Retry policy for /transfers under SERIALIZABLE conflicts.
#
# Without a delay between attempts, conflicting transfers retry instantly and
# immediately collide again -- the losers keep colliding in lockstep and burn
# all their attempts inside a few milliseconds. Backing off spreads them out;
# the random jitter is what actually breaks the lockstep, since a fixed delay
# would just move every loser to the same later instant.
#
# Bounded on purpose: total added delay is at most
# BACKOFF_BASE_SECONDS * (1 + 2) * (jitter <= 1.0) = 90 ms before the caller
# gets a 409, so a hot account can't turn into an unbounded stall.
TRANSFER_MAX_ATTEMPTS = 3
BACKOFF_BASE_SECONDS = 0.03


async def _backoff_delay(attempt: int) -> None:
    """Sleep before retrying attempt N (0-indexed): exponential, fully jittered."""
    ceiling = BACKOFF_BASE_SECONDS * (2 ** attempt)
    await asyncio.sleep(random.uniform(0, ceiling))


def latest_audit_entry_query():
    """The query that finds the newest audit_log row to chain onto.

    Pulled out as a named function so a test can assert on the SQL it
    generates. The LIMIT is the whole point: only the newest row is ever
    used, and without it Postgres returns the entire audit_log while
    SQLAlchemy builds an ORM object per row -- making each transfer cost
    grow with the ledger's full history. The existing ix_audit_log_id index
    turns this into an index scan backward, so it stays O(1) as history grows.
    """
    return select(AuditLogModel).order_by(AuditLogModel.id.desc()).limit(1)

# Turn one transaction's details into a single hash (a short fingerprint).
# Each entry includes the previous entry's hash, so the entries form a chain.
# If any old entry is changed, the chain breaks and it is easy to detect.
def compute_entry_hash(transaction_id: int, amount: float, status: str, previous_hash: str) -> str:
    data = f"{transaction_id}{amount}{status}{previous_hash}"
    return hashlib.sha256(data.encode('utf-8')).hexdigest()

# Move money from one account to another.
# This is the main route of the app. It:
#   1. Checks if this transfer was already done before (using idempotency_key), to avoid duplicates.
#   2. Creates the transaction and its two postings (one debit, one credit).
#   3. Adds an event to the outbox, so Kafka can be told about it later.
#   4. Adds a new entry to the audit trail.
#   5. Retries up to TRANSFER_MAX_ATTEMPTS times, with jittered backoff, if the
#      database reports a conflict.
#
# Requires a logged-in caller, who may only send money FROM their own account.
# The destination can be anyone's account -- that's how one user pays another.
@app.post('/transfers')
async def transfer(transaction: TransferRequest, db=Depends(get_db), current_user=Depends(get_current_user)):
    own_account = await get_own_account(db, current_user)

    # Authorisation, checked once before the retry loop: this depends only on
    # who is calling and what they asked for, so a serialization conflict can
    # never change the answer, and re-checking it on every attempt would just
    # repeat the same two queries.
    if transaction.from_account != own_account.id:
        raise HTTPException(
            status_code=403,
            detail='You can only send money from your own account',
        )

    if transaction.to_account == own_account.id:
        raise HTTPException(status_code=400, detail='Cannot transfer to your own account')

    if transaction.amount <= 0:
        raise HTTPException(status_code=400, detail='Amount must be greater than zero')

    # The destination has to be a real account. Without this a typo'd account
    # number silently creates postings against an account that doesn't exist,
    # and the money is simply gone.
    destination = await db.execute(select(AccountModel).where(AccountModel.id == transaction.to_account))
    if destination.scalars().first() is None:
        raise HTTPException(status_code=404, detail=f'No account with id {transaction.to_account}')

    for attempt in range(TRANSFER_MAX_ATTEMPTS):
        try:
            await db.connection(execution_options={"isolation_level": "SERIALIZABLE"})

            # If this exact transfer was already made, return the old result instead of doing it again.
            result = await db.execute(select(TransactionModel).where(TransactionModel.idempotency_key == transaction.idempotency_key))
            existing = result.scalars().first()
            if existing:
                return existing

            db_transaction = TransactionModel(
                amount=transaction.amount,
                idempotency_key=transaction.idempotency_key,
                reference='TXN-PLACEHOLDER',
                status='pending'
            )
            db.add(db_transaction)
            await db.flush()

            # Every transfer makes two postings: money leaves one account (debit)
            # and enters another account (credit). Together they must balance.
            db_posting_debit = PostingModel(
                transaction_id=db_transaction.id,
                account_id=transaction.from_account,
                entry_type='DEBIT',
                amount=transaction.amount
            )
            db_posting_credit = PostingModel(
                transaction_id=db_transaction.id,
                account_id=transaction.to_account,
                entry_type='CREDIT',
                amount=transaction.amount
            )

            # Save this event so it can be sent to Kafka later, even if Kafka is down right now.
            outbox_event = OutboxModel(
                event_type="transfer.completed",
                transaction_id=db_transaction.id,
                payload=json.dumps({
                    "from_account": transaction.from_account,
                    "to_account": transaction.to_account,
                    "amount": transaction.amount
                })
            )
            db.add(outbox_event)
            db.add(db_posting_debit)
            db.add(db_posting_credit)

            db_transaction.status = 'completed'

            # Add this transfer to the audit trail, linked to the previous entry's hash.
            last_entry_result = await db.execute(latest_audit_entry_query())
            last_entry = last_entry_result.scalars().first()
            previous_hash = last_entry.entry_hash if last_entry else GENESIS_HASH
            new_hash = compute_entry_hash(db_transaction.id, db_transaction.amount, db_transaction.status, previous_hash)
            audit_entry = AuditLogModel(
                transaction_id=db_transaction.id,
                entry_hash=new_hash,
                previous_hash=previous_hash
            )
            db.add(audit_entry)

            await db.commit()
            await db.refresh(db_transaction)
            return db_transaction
        except (OperationalError,DBAPIError):
            # The database was busy or had a conflict. Back off briefly so the
            # competing transfers don't just collide again instantly, then try
            # again from the top. No sleep after the final attempt -- that delay
            # would only slow down the 409 the caller is already getting.
            await db.rollback()
            if attempt < TRANSFER_MAX_ATTEMPTS - 1:
                await _backoff_delay(attempt)
            continue
    raise HTTPException(status_code=409, detail="Transfer could not complete, please try again")


# Add up a list of postings into a single balance.
# Debits reduce the balance, credits increase it.
def compute_balance(postings) -> float:
    balance = 0
    for posting in postings:
        if posting.entry_type == 'DEBIT':
            balance -= posting.amount
        else:
            balance += posting.amount
    return balance


# Every /accounts/{id}/* route below reads one person's financial history, so
# each one goes through here first: you may only look at the account you own.
# Returning 404 rather than 403 for someone else's account is deliberate --
# 403 would confirm that the account number exists, which is a small
# enumeration leak when account ids are sequential.
async def authorize_account_access(id: int, db, current_user: UserModel) -> None:
    own_account = await get_own_account(db, current_user)
    if id != own_account.id:
        raise HTTPException(status_code=404, detail='Account not found')


# Who am I, and which account is mine? The frontend calls this right after
# login so it never has to guess or hardcode an account number.
@app.get('/accounts/me')
async def get_my_account(db=Depends(get_db), current_user=Depends(get_current_user)):
    account = await get_own_account(db, current_user)
    result = await db.execute(select(PostingModel).where(PostingModel.account_id == account.id))
    postings = result.scalars().all()
    return {
        "account_id": account.id,
        "owner_email": current_user.email,
        "currency": account.currency,
        "balance": compute_balance(postings),
    }


# Work out an account's balance by adding up all its postings.
@app.get('/accounts/{id}/balance')
async def get_balance(id: int, db=Depends(get_db), current_user=Depends(get_current_user)):
    await authorize_account_access(id, db, current_user)
    result = await db.execute(select(PostingModel).where(PostingModel.account_id == id))
    postings = result.scalars().all()
    return {"account_id": id, "balance": compute_balance(postings)}


# List an account's most recent postings (its "activity"), newest first.
@app.get('/accounts/{id}/postings')
async def get_account_postings(id: int, db=Depends(get_db), current_user=Depends(get_current_user)):
    await authorize_account_access(id, db, current_user)
    result = await db.execute(
        select(PostingModel).where(PostingModel.account_id == id).order_by(PostingModel.id.desc()).limit(50)
    )
    return result.scalars().all()


# A quick summary for the dashboard: balance, how many transactions touched
# this account, and when the most recent one happened.
@app.get('/accounts/{id}/summary')
async def get_account_summary(id: int, db=Depends(get_db), current_user=Depends(get_current_user)):
    await authorize_account_access(id, db, current_user)
    result = await db.execute(select(PostingModel).where(PostingModel.account_id == id))
    postings = result.scalars().all()

    last_transaction_at = max((p.created_at for p in postings), default=None)

    return {
        "account_id": id,
        "balance": compute_balance(postings),
        "transaction_count": len(postings),
        "last_transaction_at": last_transaction_at,
    }


# Download an account's full history as a CSV statement, oldest first, with a
# running balance column so each row shows the balance right after that posting.
@app.get('/accounts/{id}/statement')
async def export_account_statement(id: int, db=Depends(get_db), current_user=Depends(get_current_user)):
    await authorize_account_access(id, db, current_user)
    result = await db.execute(
        select(PostingModel).where(PostingModel.account_id == id).order_by(PostingModel.id.asc())
    )
    postings = result.scalars().all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Date", "Transaction", "Type", "Amount", "Balance"])

    balance = 0
    for posting in postings:
        if posting.entry_type == 'DEBIT':
            balance -= posting.amount
        else:
            balance += posting.amount
        writer.writerow([
            posting.created_at.isoformat() if posting.created_at else "",
            f"TXN-{posting.transaction_id}",
            posting.entry_type,
            f"{posting.amount:.2f}",
            f"{balance:.2f}",
        ])

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=account-{id}-statement.csv"},
    )


# Check that the whole ledger is balanced: total debits should equal total credits.
# This is a basic health check for the double-entry system.
@app.get('/system/invariant-check')
async def invariant_check(db=Depends(get_db)):
    result = await db.execute(select(PostingModel))
    postings = result.scalars().all()

    total_debits = 0
    total_credits = 0
    for posting in postings:
        if posting.entry_type == 'DEBIT':
            total_debits += posting.amount
        else:
            total_credits += posting.amount

    return {
        "total_debits": total_debits,
        "total_credits": total_credits,
        "balanced": total_debits == total_credits
    }

# Walk through the audit trail from the start and check that no entry was changed.
# Each entry's hash is recomputed and compared to the stored value. If they do not
# match, or if the chain of previous hashes is broken, the data was likely altered.
@app.get('/system/audit-verify')
async def audit_verify(db=Depends(get_db)):
    result = await db.execute(select(AuditLogModel).order_by(AuditLogModel.id.asc()))
    entries = result.scalars().all()

    expected_previous_hash = GENESIS_HASH

    for entry in entries:
        if entry.previous_hash != expected_previous_hash:
            return {"valid": False, "broken_at_entry_id": entry.id, "reason": "previous_hash mismatch"}

        recompute_result = await db.execute(select(TransactionModel).where(TransactionModel.id == entry.transaction_id))
        transaction = recompute_result.scalars().first()

        recomputed_hash = compute_entry_hash(transaction.id, transaction.amount, transaction.status, entry.previous_hash)
        if recomputed_hash != entry.entry_hash:
            return {"valid": False, "broken_at_entry_id": entry.id, "reason": "entry_hash mismatch — data was likely altered"}

        expected_previous_hash = entry.entry_hash

    return {"valid": True, "entries_checked": len(entries)}

