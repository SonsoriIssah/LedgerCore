# This file sets up the connection to the database and reads the Kafka address.
# Other files import from here to talk to the database.

from sqlalchemy.ext.asyncio import create_async_engine,AsyncSession
from sqlalchemy.orm import DeclarativeBase,sessionmaker
import os

# Read the database address from the environment.
# If it is not set, use a local Postgres database instead. This keeps local development working.
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost/fastapi_db")

# Read the Kafka address from the environment.
# If it is not set, use localhost. This keeps local development working.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# The engine is what actually connects to the database.
engine = create_async_engine(DATABASE_URL,echo=True)

# This creates new database sessions when we need them.
AsyncSessionLocal = sessionmaker(engine,class_=AsyncSession,expire_on_commit=False)

# All database tables inherit from this class.
class Base(DeclarativeBase):
    pass

# This gives each request its own database session, and closes it when done.
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
