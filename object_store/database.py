"""Session factory; production uses PostgreSQL, tests use isolated SQLite."""
import os
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool


def make_database(url=None):
    url = url or os.environ.get("DATABASE_URL", "postgresql+psycopg://store:store@postgres/store")
    options = {"pool_pre_ping": True}
    if url.startswith("sqlite"):
        options.update(connect_args={"check_same_thread": False})
        if ":memory:" in url:
            options["poolclass"] = StaticPool
    else:
        options.update(connect_args={"connect_timeout": 3}, pool_timeout=5)
    engine = create_engine(url, **options)
    if url.startswith("sqlite"):
        @event.listens_for(engine, "connect")
        def foreign_keys(connection, record):
            connection.execute("PRAGMA foreign_keys=ON")
    return engine, sessionmaker(engine, expire_on_commit=False)
