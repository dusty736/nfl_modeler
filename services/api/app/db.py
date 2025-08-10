import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

# Pull the database connection string from the environment
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://nfl_user:nfl_pass@db:5432/nfl"
)

# Create the async SQLAlchemy engine
engine = create_async_engine(DATABASE_URL, pool_size=10, max_overflow=10)

# Create a session factory for database interactions
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
