"""
Database bootstrap (async SQLAlchemy)
------------------------------------
Creates a single async Engine and an async session factory for the app.

Environment
-----------
- DATABASE_URL: full asyncpg URL. Defaults to:
  postgresql+asyncpg://nfl_user:nfl_pass@db:5432/nfl

Usage
-----
from app.db import AsyncSessionLocal

async with AsyncSessionLocal() as session:
    # ... await session.execute(...)

Notes
-----
- No functional changes here; this file adds documentation only.
- Engine and session factory are module singletons by design.
"""

# --- Configuration & singletons -----------------------------------------------
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
