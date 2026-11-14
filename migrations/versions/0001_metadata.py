"""Initial metadata schema, including durable replica intents."""
from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table("objects",
        sa.Column("id", sa.String(36), primary_key=True), sa.Column("name", sa.String(255), nullable=False),
        sa.Column("size", sa.BigInteger(), nullable=False), sa.Column("content_type", sa.String(255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("modified_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("checksum", sa.String(64), nullable=False), sa.Column("chunk_count", sa.Integer(), nullable=False),
        sa.Column("custom_metadata", sa.JSON(), nullable=False), sa.Column("state", sa.String(16), nullable=False),
        sa.CheckConstraint("state IN ('uploading', 'available', 'deleting')"))
    op.create_index("ix_objects_state", "objects", ["state"])
    op.create_table("workers", sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("url", sa.String(255), nullable=False, unique=True),
        sa.Column("last_seen", sa.DateTime(timezone=True), nullable=False),
        sa.Column("free_bytes", sa.BigInteger(), nullable=False))
    op.create_table("chunks", sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("object_id", sa.String(36), sa.ForeignKey("objects.id"), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False), sa.Column("size", sa.Integer(), nullable=False),
        sa.Column("checksum", sa.String(64), nullable=False), sa.UniqueConstraint("object_id", "position"),
        sa.CheckConstraint("size > 0 AND size <= 1000000"))
    op.create_index("ix_chunks_object_id", "chunks", ["object_id"])
    op.create_table("replicas", sa.Column("chunk_id", sa.String(36), sa.ForeignKey("chunks.id"), primary_key=True),
        sa.Column("worker_id", sa.String(64), sa.ForeignKey("workers.id"), primary_key=True),
        sa.Column("state", sa.String(16), nullable=False), sa.CheckConstraint("state IN ('pending', 'healthy', 'bad')"))


def downgrade():
    for table in ("replicas", "chunks", "workers", "objects"):
        op.drop_table(table)
