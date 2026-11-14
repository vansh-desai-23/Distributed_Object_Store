"""Run schema migrations using the same database URL as the controller."""
from alembic import context
from object_store.database import make_database
from object_store.models import Base

engine, _ = make_database()
with engine.connect() as connection:
    context.configure(connection=connection, target_metadata=Base.metadata)
    with context.begin_transaction():
        context.run_migrations()
engine.dispose()
