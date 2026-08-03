from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from database import engine, get_db, Base, AsyncSessionLocal
from pydantic import BaseModel
from sqlalchemy import select
from models import UserModel, TransactionModel, PostingModel, OutboxModel, ProcessedEventModel, AuditLogModel
from auth import hash_password, verify_password, create_access_token, decode_access_token, create_refresh_token
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError
import json, asyncio, http, uuid
from sqlalchemy.exc import OperationalError, DBAPIError
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import hashlib

producer = None

async def outbox_poller():
    while True:
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(OutboxModel).where(OutboxModel.published == False))
            events = result.scalars().all()

            for event in events:
                await producer.send_and_wait("ledger-events", event.payload.encode('utf-8'))
                event.published = True

            await db.commit()

        await asyncio.sleep(5)

async def outbox_consumer():
    consumer = AIOKafkaConsumer(
        "ledger-events",
        bootstrap_servers='localhost:9092',
        group_id="ledger-consumer-group"
    )
    await consumer.start()
    try:
        async for message in consumer:
            event_data = json.loads(message.value.decode('utf-8'))
            event_id = message.offset

            async with AsyncSessionLocal() as db:
                try:
                    processed = ProcessedEventModel(event_id=event_id)
                    db.add(processed)
                    await db.commit()
                except Exception:
                    await db.rollback()
                    continue

                print(f"Processing event: {event_data}")
    finally:
        await consumer.stop()
@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    await producer.start()
    poller_task = asyncio.create_task(outbox_poller())
    consumer_task = asyncio.create_task(outbox_consumer())
    yield
    poller_task.cancel()
    consumer_task.cancel()
    await producer.stop()
app = FastAPI(lifespan=lifespan)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl='login')


class User(BaseModel):
    email: str
    password: str

class RefreshRequest(BaseModel):
    refresh_token: str

class TransferRequest(BaseModel):
    from_account: int
    to_account: int
    amount: float
    idempotency_key: str

@app.post('/register')
async def register(user: User, db=Depends(get_db)):
    password = hash_password(user.password)
    db_user = UserModel(email=user.email, hashed_password=password, role='user')
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return db_user

@app.post('/login')
async def login(user: User, db=Depends(get_db)):
    result = await db.execute(select(UserModel).where(UserModel.email == user.email))
    db_user = result.scalars().first()
    if not db_user:
        raise HTTPException(status_code=401, detail='Invalid email or password')
    password = verify_password(user.password, db_user.hashed_password)
    if not password:
        raise HTTPException(status_code=401, detail='Invalid email or password')
    return create_access_token({'sub': user.email})

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


def require_role(required_role: str):
    async def role_checker(current_user=Depends(get_current_user)):
        if current_user.role != required_role:
            raise HTTPException(status_code=403, detail="Not authorized")
        return current_user
    return role_checker

GENESIS_HASH = "0" * 64

def compute_entry_hash(transaction_id: int, amount: float, status: str, previous_hash: str) -> str:
    data = f"{transaction_id}{amount}{status}{previous_hash}"
    return hashlib.sha256(data.encode('utf-8')).hexdigest()

@app.post('/transfers')
async def transfer(transaction: TransferRequest, db=Depends(get_db)):
    for attempt in range(3):
        try:
            await db.connection(execution_options={"isolation_level": "SERIALIZABLE"})

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
            await db.commit()
            await db.refresh(db_transaction)

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

            last_entry_result = await db.execute(select(AuditLogModel).order_by(AuditLogModel.id.desc()))
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
            await db.rollback()
            continue
    raise HTTPException(status_code=409, detail="Transfer could not complete, please try again")


@app.get('/accounts/{id}/balance')
async def get_balance(id: int, db=Depends(get_db)):
    result = await db.execute(select(PostingModel).where(PostingModel.account_id == id))
    postings = result.scalars().all()

    balance = 0
    for posting in postings:
        if posting.entry_type == 'DEBIT':
            balance -= posting.amount
        else:
            balance += posting.amount

    return {"account_id": id, "balance": balance}


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

