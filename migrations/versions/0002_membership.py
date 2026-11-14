"""Persist storage/boot identity and reconciliation eligibility."""
from alembic import op
import sqlalchemy as sa

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("workers") as batch:
        batch.add_column(sa.Column("storage_id", sa.String(36), nullable=True))
        batch.add_column(sa.Column("boot_id", sa.String(36), nullable=True))
        batch.add_column(sa.Column("eligible", sa.Boolean(), nullable=False, server_default=sa.false()))
        batch.add_column(sa.Column("membership_version", sa.Integer(), nullable=False, server_default="0"))
        batch.create_unique_constraint("uq_workers_storage_id", ["storage_id"])
    op.execute("UPDATE replicas SET state = 'pending' WHERE state = 'healthy'")


def downgrade():
    with op.batch_alter_table("workers") as batch:
        batch.drop_constraint("uq_workers_storage_id", type_="unique")
        for name in ("membership_version", "eligible", "boot_id", "storage_id"):
            batch.drop_column(name)
