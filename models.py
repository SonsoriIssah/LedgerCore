from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, String, Float
from database import Base
from datetime import datetime
from sqlalchemy import ForeignKey, func, Index

class UserModel(Base):
    __tablename__ = 'user'
    # no balance field — balance is always derived by summing this account's postings
    id:Mapped[int] = mapped_column(primary_key=True, index=True)
    email:Mapped[str] = mapped_column(nullable=False,unique=True)
    hashed_password:Mapped[str] = mapped_column(nullable = False)
    role:Mapped[str] = mapped_column(nullable=False)


class AccountModel(Base):
    __tablename__ = 'accounts'
    id:Mapped[int] =mapped_column(primary_key=True, index=True)
    owner_id:Mapped[int] = mapped_column(nullable=False) # which user this account belongs to
    currency: Mapped[str] = mapped_column(nullable=False)

class TransactionModel(Base):
    __tablename__ = 'transactions'
    id:Mapped[int] = mapped_column(primary_key=True, index=True)
    reference:Mapped[str] =mapped_column(nullable=False)# human-readable label (For the user), like a receipt number
    idempotency_key:Mapped[str] =mapped_column(nullable=False, unique=True) # blocks duplicate transfers from retries
    amount:Mapped[float] =mapped_column(nullable=False)
    status:Mapped[str] =mapped_column(nullable=False)

class PostingModel(Base):
    __tablename__ = 'postings'
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    transaction_id: Mapped[int] = mapped_column(nullable=False)
    account_id: Mapped[int] = mapped_column(nullable=False)# whose ledger this line is written to
    entry_type: Mapped[str] = mapped_column(nullable=False)
    amount: Mapped[float] = mapped_column(nullable=False)


class OutboxModel(Base):
    __tablename__ = 'outbox'
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    event_type: Mapped[str] = mapped_column(nullable=False)
    transaction_id: Mapped[int] = mapped_column(ForeignKey("transactions.id"), nullable=False, index=True)
    published: Mapped[bool] = mapped_column(nullable=False, default=False, index=True)
    payload: Mapped[str] = mapped_column(nullable=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

class ProcessedEventModel(Base):
    __tablename__ = 'processed_events'
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    event_id: Mapped[int] = mapped_column(nullable=False, unique=True)
    processed_at: Mapped[datetime] = mapped_column(server_default=func.now())

class AuditLogModel(Base):
    __tablename__ = 'audit_log'
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    transaction_id: Mapped[int] = mapped_column(ForeignKey("transactions.id"), nullable=False)
    entry_hash: Mapped[str] = mapped_column(nullable=False)
    previous_hash: Mapped[str] = mapped_column(nullable=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())