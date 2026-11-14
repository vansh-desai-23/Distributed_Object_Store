"""Transactional, coalesced repair intents and current-intent checks."""
from sqlalchemy import select
from object_store.models import RepairJob, Chunk, StoredObject, Replica, Worker, now


def enqueue(session, chunk_id, reason, invalidate=False):
    if session.get(Chunk, chunk_id) is None:
        return
    job = session.get(RepairJob, chunk_id)
    if job is None:
        session.add(RepairJob(chunk_id=chunk_id, reason=reason, revision=1, attempts=0))
    elif invalidate:
        job.revision += 1
        job.reason = reason
        job.updated_at = now()
        job.last_error = None


def current(session, chunk, revision=None):
    obj = session.get(StoredObject, chunk.object_id)
    if obj is None or obj.state not in ('available', 'uploading'):
        return False
    if revision is not None:
        job = session.get(RepairJob, chunk.id)
        if job is None or job.revision != revision:
            return False
    return True


def needs_repair(store, session, chunk_id):
    replicas = session.scalars(select(Replica).where(Replica.chunk_id == chunk_id)).all()
    return (len(replicas) != 2 or
            any(r.state != 'healthy' or not store.live(session.get(Worker, r.worker_id)) for r in replicas))
