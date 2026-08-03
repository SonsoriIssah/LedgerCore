from sqlalchemy.ext.asyncio import create_async_engine,AsyncSession
from sqlalchemy.orm import DeclarativeBase,sessionmaker
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost/fastapi_db")
engine = create_async_engine(DATABASE_URL,echo=True) #This is what creates the a connection to the database 
AsyncSessionLocal = sessionmaker(engine,class_=AsyncSession,expire_on_commit=False)

class Base(DeclarativeBase):
    pass

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
