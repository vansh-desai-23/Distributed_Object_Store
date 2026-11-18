"""Persist upload idempotency bindings and committed response snapshots."""
from alembic import op
import sqlalchemy as sa

revision = '0004'
down_revision = '0003'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('upload_receipts',
        sa.Column('key', sa.String(128), primary_key=True),
        sa.Column('fingerprint', sa.String(64), nullable=False),
        sa.Column('object_id', sa.String(36), nullable=False, unique=True),
        sa.Column('state', sa.String(16), nullable=False),
        sa.Column('response', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("state IN ('pending', 'completed', 'failed')"))


def downgrade():
    op.drop_table('upload_receipts')
