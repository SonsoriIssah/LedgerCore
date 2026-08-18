# This file describes the database tables (models) used by the app.
# Each class below becomes one table in the database.

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, String, Float
from database import Base
from datetime import datetime
from sqlalchemy import ForeignKey, func, Index

# A person who can log in to the app.
class UserModel(Base):
    __tablename__ = 'user'
    # There is no balance field here. Balance is always worked out by adding up this account's postings.
    id:Mapped[int] = mapped_column(primary_key=True, index=True)
    email:Mapped[str] = mapped_column(nullable=False,unique=True)
    hashed_password:Mapped[str] = mapped_column(nullable = False)
    role:Mapped[str] = mapped_column(nullable=False)


# A ledger account that money can move in and out of.
# Every user gets exactly one of these when they register, and its id is the
# account number they share with other people to receive transfers.
class AccountModel(Base):
    __tablename__ = 'accounts'
    id:Mapped[int] =mapped_column(primary_key=True, index=True)
    # unique: one account per user, enforced by the database rather than by
    # remembering to check it at every call site.
    owner_id:Mapped[int] = mapped_column(ForeignKey("user.id"), nullable=False, unique=True, index=True)
    currency: Mapped[str] = mapped_column(nullable=False, default='USD')
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

# One transfer request. It may create two postings (a debit and a credit).
class TransactionModel(Base):
    __tablename__ = 'transactions'
    id:Mapped[int] = mapped_column(primary_key=True, index=True)
    reference:Mapped[str] =mapped_column(nullable=False)# human-readable label (For the user), like a receipt number
    idempotency_key:Mapped[str] =mapped_column(nullable=False, unique=True) # blocks duplicate transfers from retries
    amount:Mapped[float] =mapped_column(nullable=False)
    status:Mapped[str] =mapped_column(nullable=False)

# One line in the ledger: money added to or taken from one account, as part of a transaction.
class PostingModel(Base):
    __tablename__ = 'postings'
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    transaction_id: Mapped[int] = mapped_column(nullable=False)
    account_id: Mapped[int] = mapped_column(nullable=False)# whose ledger this line is written to
    entry_type: Mapped[str] = mapped_column(nullable=False)
    amount: Mapped[float] = mapped_column(nullable=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())


# An event waiting to be sent to Kafka. This is the "outbox pattern":
# we save the event in the same database transaction as the transfer, so we never lose an event.
class OutboxModel(Base):
    __tablename__ = 'outbox'
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    event_type: Mapped[str] = mapped_column(nullable=False)
    transaction_id: Mapped[int] = mapped_column(ForeignKey("transactions.id"), nullable=False, index=True)
    published: Mapped[bool] = mapped_column(nullable=False, default=False, index=True)
    payload: Mapped[str] = mapped_column(nullable=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

# Keeps track of which Kafka events we have already handled, so we do not process the same event twice.
class ProcessedEventModel(Base):
    __tablename__ = 'processed_events'
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    event_id: Mapped[int] = mapped_column(nullable=False, unique=True)
    processed_at: Mapped[datetime] = mapped_column(server_default=func.now())

# One entry in the audit trail. Each entry links to the one before it with a hash,
# so anyone can check later if old records were changed.
class AuditLogModel(Base):
    __tablename__ = 'audit_log'
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    transaction_id: Mapped[int] = mapped_column(ForeignKey("transactions.id"), nullable=False)
    entry_hash: Mapped[str] = mapped_column(nullable=False)
    previous_hash: Mapped[str] = mapped_column(nullable=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())