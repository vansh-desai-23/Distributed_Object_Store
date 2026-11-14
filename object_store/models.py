"""Relational metadata. File bytes never enter these tables."""
from datetime import datetime, timezone
from sqlalchemy import BigInteger, CheckConstraint, DateTime, ForeignKey, Integer, JSON, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def now():
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class StoredObject(Base):
    __tablename__ = "objects"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    size: Mapped[int] = mapped_column(BigInteger, default=0)
    content_type: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    modified_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    checksum: Mapped[str] = mapped_column(String(64), default="")
    chunk_count: Mapped[int] = mapped_column(Integer, default=0)
    custom_metadata: Mapped[dict] = mapped_column(JSON, default=dict)
    state: Mapped[str] = mapped_column(String(16), default="uploading", index=True)
    __table_args__ = (CheckConstraint("state IN ('uploading', 'available', 'deleting')"),)


class Worker(Base):
    __tablename__ = "workers"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    url: Mapped[str] = mapped_column(String(255), unique=True)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    free_bytes: Mapped[int] = mapped_column(BigInteger, default=0)


class Chunk(Base):
    __tablename__ = "chunks"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    object_id: Mapped[str] = mapped_column(ForeignKey("objects.id"), index=True)
    position: Mapped[int] = mapped_column(Integer)
    size: Mapped[int] = mapped_column(Integer)
    checksum: Mapped[str] = mapped_column(String(64))
    __table_args__ = (UniqueConstraint("object_id", "position"), CheckConstraint("size > 0 AND size <= 1000000"))


class Replica(Base):
    __tablename__ = "replicas"
    chunk_id: Mapped[str] = mapped_column(ForeignKey("chunks.id"), primary_key=True)
    worker_id: Mapped[str] = mapped_column(ForeignKey("workers.id"), primary_key=True)
    state: Mapped[str] = mapped_column(String(16), default="pending")
    __table_args__ = (CheckConstraint("state IN ('pending', 'healthy', 'bad')"),)
