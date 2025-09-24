"""
Database bootstrap (async SQLAlchemy)
------------------------------------
Creates a single async Engine and an async session factory for the app.

Env (preferred granular vars)
-----------------------------
- DB_HOST : "/cloudsql/PROJECT:REGION:INSTANCE"  (Cloud Run) OR "127.0.0.1"
- DB_NAME : "nfl"
- DB_USER : "nfl_app"
- DB_PASS : (from Secret Manager)
- DB_PORT : "5432" (TCP mode only)

Alt (single URL override)
-------------------------
- DATABASE_URL: full asyncpg URL (takes precedence), e.g.
  postgresql+asyncpg://user:pass@host:5432/dbname

Usage
-----
from app.db import AsyncSessionLocal
async with AsyncSessionLocal() as session:
    ...

Notes
-----
- When using the Cloud SQL Unix socket, we DO NOT put host in the DSN.
  We pass the socket directory via connect_args={"host": "/cloudsql/..."}.
"""

from __future__ import annotations
import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

# --- Config --------------------------------------------------------------------

# If the user explicitly gives a DATABASE_URL, use it as-is.
DATABASE_URL = os.getenv("DATABASE_URL")

DB_NAME = os.getenv("DB_NAME", "nfl")
DB_USER = os.getenv("DB_USER", "nfl_app")
DB_PASS = os.getenv("DB_PASS", "")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")  # Cloud Run: "/cloudsql/PROJECT:REGION:INSTANCE"
DB_PORT = os.getenv("DB_PORT", "5432")

CONNECT_ARGS: dict = {}

if not DATABASE_URL:
    if DB_HOST.startswith("/"):
        # Cloud SQL over Unix socket:
        # - omit host in the URL
        # - provide the socket path via connect_args["host"]
        DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@/{DB_NAME}"
        CONNECT_ARGS = {"host": DB_HOST, "ssl": False}
    else:
        # Standard TCP
        DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        CONNECT_ARGS = {}

# --- Engine / Session ----------------------------------------------------------

engine = create_async_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=5,
    pool_recycle=1800,        # refresh idle conns ~30 min
    connect_args=CONNECT_ARGS # critical for Unix socket mode
)

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

