# app/db.py
import os
import ssl
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base

Base = declarative_base()

def _normalize_db_url(url: str) -> str:
    """
    - Ensure SQLAlchemy async driver: postgresql+asyncpg://
    - Remove sslmode from query params (asyncpg doesn't accept it)
    """
    if not url:
        return url

    # Normalize scheme for SQLAlchemy asyncpg
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgresql://") and "+asyncpg" not in url:
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)

    # Strip sslmode from query parameters (causes asyncpg TypeError)
    parts = urlsplit(url)
    q = [(k, v) for (k, v) in parse_qsl(parts.query, keep_blank_values=True) if k.lower() != "sslmode"]
    new_query = urlencode(q)

    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))

DATABASE_URL = _normalize_db_url(os.getenv("DATABASE_URL", ""))

# Railway may present a self-signed chain -> don't verify host/cert
ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

engine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"ssl": ssl_ctx},
)

SessionLocal = async_sessionmaker(engine, expire_on_commit=False)
