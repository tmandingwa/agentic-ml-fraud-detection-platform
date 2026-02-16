# app/db.py
import os
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

Base = declarative_base()

def _build_async_db_url() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or os.getenv("POSTGRES_URL_NON_POOLING")
    if not url:
        raise RuntimeError("DATABASE_URL is not set (Railway Variables).")

    # Railway/Heroku sometimes use postgres://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)

    # Ensure async driver
    if url.startswith("postgresql://") and "postgresql+asyncpg://" not in url:
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)

    return url

DATABASE_URL = _build_async_db_url()

# SSL: hosted postgres commonly requires it (safe even if not required)
connect_args = {"ssl": True}

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    connect_args=connect_args,
)

SessionLocal = async_sessionmaker(engine, expire_on_commit=False)