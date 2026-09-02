# This file sets up the connection to the database and reads the Kafka address.
# Other files import from here to talk to the database.

from sqlalchemy.ext.asyncio import create_async_engine,AsyncSession
from sqlalchemy.orm import DeclarativeBase,sessionmaker
from urllib.parse import urlparse
import os

# Read the database address from the environment.
# If it is not set, use a local Postgres database instead. This keeps local development working.
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost/fastapi_db")

# Read the Kafka address from the environment.
# If it is not set, use localhost. This keeps local development working.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Log every SQL statement? Useful when debugging locally, but it writes a few
# lines per query, so leaving it on in production floods the logs and costs
# real time on hot paths. Off unless SQL_ECHO is explicitly set.
SQL_ECHO = os.getenv("SQL_ECHO", "").lower() in ("1", "true", "yes")

# Managed Postgres providers (Supabase, Neon, and anything else fronted by
# PgBouncer) put a connection pooler in front of the database. In transaction
# pooling mode a client does not keep the same server connection between
# statements, so server-side prepared statements break -- and asyncpg uses
# them by default. The symptom is not a startup failure but an intermittent
# one under concurrency:
#
#     DuplicatePreparedStatementError: prepared statement
#     "__asyncpg_stmt_1__" already exists
#
# Turning the statement cache off costs some performance, so it is only done
# when talking to a pooler. Set DB_DISABLE_PREPARED_STATEMENTS explicitly to
# override the detection either way.
def _looks_like_a_pooler(url: str) -> bool:
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        port = parsed.port
    except ValueError:
        # A malformed URL must not crash the import. urlparse raises on, for
        # example, square brackets left in from a connection-string template,
        # and an exception here would kill the app before anything can explain
        # why. Fall back to a plain substring check; lifespan() reports the
        # real problem with a message that actually names DATABASE_URL.
        lowered = url.lower()
        return "pooler." in lowered or "pgbouncer" in lowered or ":6543/" in lowered
    # 6543 is the conventional transaction-mode pooler port.
    return "pooler." in host or "pgbouncer" in host or port == 6543


_override = os.getenv("DB_DISABLE_PREPARED_STATEMENTS")
if _override is None:
    DISABLE_PREPARED_STATEMENTS = _looks_like_a_pooler(DATABASE_URL)
else:
    DISABLE_PREPARED_STATEMENTS = _override.lower() in ("1", "true", "yes")

# statement_cache_size is asyncpg's own cache; prepared_statement_cache_size is
# the one SQLAlchemy's asyncpg dialect keeps. Both have to go, or the other one
# still hands PgBouncer a prepared statement.
_connect_args = (
    {"statement_cache_size": 0, "prepared_statement_cache_size": 0}
    if DISABLE_PREPARED_STATEMENTS else {}
)

# The engine is what actually connects to the database.
engine = create_async_engine(DATABASE_URL, echo=SQL_ECHO, connect_args=_connect_args)

# This creates new database sessions when we need them.
AsyncSessionLocal = sessionmaker(engine,class_=AsyncSession,expire_on_commit=False)

# All database tables inherit from this class.
class Base(DeclarativeBase):
    pass

# This gives each request its own database session, and closes it when done.
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
