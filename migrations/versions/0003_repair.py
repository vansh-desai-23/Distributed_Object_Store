"""Durable operation ordering, damage receipts and coalesced repair jobs."""
from alembic import op
import sqlalchemy as sa

revision = '0003'
down_revision = '0002'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('workers', sa.Column('scan_progress', sa.JSON(), nullable=True))
    op.create_table('operation_clock', sa.Column('id', sa.Integer(), primary_key=True),
                    sa.Column('value', sa.BigInteger(), nullable=False))
    op.execute('INSERT INTO operation_clock (id,value) VALUES (1,0)')
    op.create_table('worker_operations', sa.Column('chunk_id', sa.String(36), primary_key=True),
        sa.Column('worker_id', sa.String(64), primary_key=True), sa.Column('sequence', sa.BigInteger(), nullable=False),
        sa.Column('method', sa.String(8), nullable=False), sa.Column('state', sa.String(16), nullable=False))
    op.create_table('repair_jobs', sa.Column('chunk_id', sa.String(36), sa.ForeignKey('chunks.id', ondelete='CASCADE'), primary_key=True),
        sa.Column('revision', sa.Integer(), nullable=False), sa.Column('reason', sa.String(64), nullable=False),
        sa.Column('attempts', sa.Integer(), nullable=False), sa.Column('last_error', sa.String(255), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False))
    op.create_table('damage_receipts', sa.Column('event_id', sa.String(36), primary_key=True),
        sa.Column('worker_id', sa.String(64), nullable=False), sa.Column('storage_id', sa.String(36), nullable=False))
    # Existing copies must receive authoritative expectations for local scans.
    op.execute('UPDATE workers SET eligible = false, membership_version = membership_version + 1')
    op.execute("UPDATE replicas SET state='pending'")


def downgrade():
    for table in ('damage_receipts', 'repair_jobs', 'worker_operations', 'operation_clock'):
        op.drop_table(table)
    op.drop_column('workers', 'scan_progress')
