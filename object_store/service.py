"""Controller operations and durable reconciliation, serialized per object."""
import asyncio
from collections import defaultdict
from datetime import timezone
import hashlib
import logging
from uuid import uuid4

from fastapi import HTTPException
import httpx
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import SQLAlchemyError

from object_store.common import CHUNK_SIZE, checksum
from object_store.models import Chunk, Replica, StoredObject, Worker, OperationClock, WorkerOperation, RepairJob, now

from object_store.repair_queue import enqueue, current, needs_repair

log = logging.getLogger(__name__)


class Store:
    def __init__(self, sessions, client, worker_timeout=10):
        self.sessions = sessions
        self.client = client
        self.worker_timeout = worker_timeout
        self.locks = defaultdict(asyncio.Lock)
        self.maintenance_lock = asyncio.Lock()
        self.upload_slots = asyncio.Semaphore(2)
        self.download_slots = asyncio.Semaphore(4)
        self.repair_intents = {}

    def live(self, worker):
        return worker.eligible and self.reachable(worker)

    def reachable(self, worker):
        timestamp = worker.last_seen.replace(tzinfo=timezone.utc)
        return (now() - timestamp).total_seconds() < self.worker_timeout

    def available(self, session, object_id):
        obj = session.get(StoredObject, object_id)
        if obj is None or obj.state != "available":
            raise HTTPException(404, "Object not found")
        return obj

    async def request(self, method, worker, chunk_id, **kwargs):
        headers = dict(kwargs.pop("headers", {}))
        headers.update({"X-Worker-Storage": worker.storage_id or "", "X-Worker-Boot": worker.boot_id or ""})
        sequence = None
        with self.sessions() as session:
            chunk = session.get(Chunk, chunk_id)
            revision = self.repair_intents.get(chunk_id)
            if chunk and (method == 'PUT' or revision is not None) and not current(session, chunk, revision):
                return None
        if method in ('PUT', 'DELETE'):
            with self.sessions.begin() as session:
                if session.get(OperationClock, 1) is None:
                    session.add(OperationClock(id=1, value=0))
                    session.flush()
                sequence = session.scalar(update(OperationClock).where(OperationClock.id == 1)
                                          .values(value=OperationClock.value + 1).returning(OperationClock.value))
                operation = session.get(WorkerOperation, (chunk_id, worker.id))
                if operation is None:
                    operation = WorkerOperation(chunk_id=chunk_id, worker_id=worker.id)
                    session.add(operation)
                operation.sequence, operation.method, operation.state = sequence, method, 'pending'
            headers['X-Operation-Sequence'] = str(sequence)
        # PUT/DELETE are idempotent; retry once after an ambiguous network failure.
        for attempt in range(2):
            try:
                response = await self.client.request(method, f"{worker.url}/internal/chunks/{chunk_id}", headers=headers, **kwargs)
                if response.status_code < 500 or attempt:
                    with self.sessions() as session:
                        registered = session.get(Worker, worker.id)
                        if registered is None or registered.membership_version != worker.membership_version:
                            return None
                    if sequence is not None:
                        with self.sessions.begin() as session:
                            operation = session.get(WorkerOperation, (chunk_id, worker.id))
                            if operation.sequence != sequence:
                                return None
                            operation.state = ('done' if response.status_code in (200, 204) else
                                               'pending' if response.status_code >= 500 else 'rejected')
                    return response
            except httpx.HTTPError:
                if attempt:
                    return None
            await asyncio.sleep(0.1)
        return None

    async def fetch(self, chunk):
        with self.sessions() as session:
            replicas = session.scalars(select(Replica).where(Replica.chunk_id == chunk.id)).all()
            workers = [(r, session.get(Worker, r.worker_id)) for r in replicas]
        # Rejoining and expired workers are excluded until inventory is reconciled.
        workers.sort(key=lambda pair: (not self.live(pair[1]), pair[0].state != "healthy"))
        for replica, worker in workers:
            if not self.live(worker):
                continue
            response = await self.request("GET", worker, chunk.id)
            if response is not None and response.status_code == 200:
                data = response.content
                if len(data) == chunk.size and checksum(data) == chunk.checksum:
                    with self.sessions.begin() as session:
                        row = session.get(Replica, (chunk.id, worker.id))
                        if row:
                            row.state = "healthy"
                    return data
            if response is not None and response.status_code in (200, 404, 409):
                log.warning("Replica missing or invalid: object=%s chunk=%s worker=%s",
                            chunk.object_id, chunk.id, worker.id)
                with self.sessions.begin() as session:
                    row = session.get(Replica, (chunk.id, worker.id))
                    if row:
                        row.state = "bad"
                    enqueue(session, chunk.id, "read_failure", invalidate=True)
        raise HTTPException(503, f"No valid reachable replica for chunk {chunk.position}")

    async def place(self, chunk, data):
        revision = self.repair_intents.get(chunk.id)
        with self.sessions() as session:
            if not current(session, chunk, revision):
                return False
            replicas = session.scalars(select(Replica).where(Replica.chunk_id == chunk.id)).all()
            healthy = {r.worker_id for r in replicas if r.state == "healthy" and self.live(session.get(Worker, r.worker_id))}
            workers = [w for w in session.scalars(select(Worker)) if self.live(w) and w.id not in healthy]
            counts = dict(session.execute(select(Replica.worker_id, func.count()).group_by(Replica.worker_id)).all())
        workers.sort(key=lambda w: (counts.get(w.id, 0), w.id))
        for worker in workers:
            with self.sessions() as session:
                if not current(session, chunk, revision):
                    return False
            if len(healthy) >= 2:
                break
            if worker.free_bytes < chunk.size + 10_000_000:
                continue
            prior = next((r for r in replicas if r.worker_id == worker.id), None)
            if prior and prior.state == "bad":
                # We already hold verified replacement bytes; remove a damaged manifest too.
                response = await self.request("DELETE", worker, chunk.id)
                if response is None or response.status_code not in (204, 404):
                    continue
            # Record the destination before sending bytes. A lost acknowledgment can be cleaned up.
            with self.sessions.begin() as session:
                if not current(session, chunk, revision):
                    return False
                replica = session.get(Replica, (chunk.id, worker.id))
                if replica is None:
                    session.add(Replica(chunk_id=chunk.id, worker_id=worker.id, state="pending"))
                else:
                    replica.state = "pending"
            response = await self.request("PUT", worker, chunk.id, content=data,
                                          headers={"X-Chunk-Checksum": chunk.checksum,
                                                   "Content-Type": "application/octet-stream"})
            if response is not None and response.status_code == 200:
                try:
                    ack = response.json()
                    valid = ack["chunk_id"] == chunk.id and ack["checksum"] == chunk.checksum and ack["size"] == chunk.size
                except (ValueError, KeyError, TypeError):
                    valid = False
                if valid:
                    verified = await self.request('GET', worker, chunk.id)
                    valid = (verified is not None and verified.status_code == 200
                             and len(verified.content) == chunk.size and checksum(verified.content) == chunk.checksum)
                if valid:
                    with self.sessions.begin() as session:
                        if not current(session, chunk, revision):
                            return False
                        session.get(Replica, (chunk.id, worker.id)).state = "healthy"
                    healthy.add(worker.id)
        return len(healthy) >= 2

    async def upload(self, file, metadata):
        identity = str(uuid4())
        async with self.upload_slots, self.locks[identity]:
            with self.sessions() as session:
                if sum(self.live(w) for w in session.scalars(select(Worker))) < 2:
                    raise HTTPException(503, "At least two available workers are required")
            name = (file.filename or "object").replace("\\", "/").split("/")[-1] or "object"
            content_type = file.content_type or "application/octet-stream"
            if len(name) > 255 or len(content_type) > 255 or any(ord(c) < 32 or ord(c) > 126 for c in content_type):
                raise HTTPException(400, "Invalid filename or content type")
            with self.sessions.begin() as session:
                session.add(StoredObject(id=identity, name=name, content_type=content_type, custom_metadata=metadata))
            try:
                digest = hashlib.sha256()
                size = count = 0
                while data := await file.read(CHUNK_SIZE):
                    digest.update(data)
                    chunk = Chunk(id=str(uuid4()), object_id=identity, position=count,
                                  size=len(data), checksum=checksum(data))
                    with self.sessions.begin() as session:
                        session.add(chunk)
                    if not await self.place(chunk, data):
                        raise HTTPException(503, "Unable to store two durable replicas; upload aborted")
                    size += len(data)
                    count += 1
                with self.sessions.begin() as session:
                    obj = session.get(StoredObject, identity)
                    obj.size, obj.chunk_count, obj.checksum = size, count, digest.hexdigest()
                    obj.state = "available"
                    obj.modified_at = now()
            except BaseException:
                # Preserve the original failure if the DB is also unavailable. Once the
                # object lock is released, maintenance can reclaim the uploading intent.
                try:
                    with self.sessions.begin() as session:
                        obj = session.get(StoredObject, identity)
                        if obj:
                            obj.state = "deleting"
                except SQLAlchemyError:
                    log.exception("Upload cleanup marker deferred: object=%s", identity)
                raise
        return self.metadata(identity)

    def metadata(self, identity):
        with self.sessions() as session:
            obj = self.available(session, identity)
            chunks = session.scalars(select(Chunk).where(Chunk.object_id == identity).order_by(Chunk.position)).all()
            result = self.summary(obj)
            result.update(created_at=obj.created_at.isoformat(), modified_at=obj.modified_at.isoformat(),
                          checksum=obj.checksum, chunk_count=obj.chunk_count, metadata=obj.custom_metadata,
                          chunk_size=CHUNK_SIZE, chunks=[])
            for chunk in chunks:
                replicas = session.scalars(select(Replica).where(Replica.chunk_id == chunk.id)).all()
                result["chunks"].append({"chunk_id": chunk.id, "position": chunk.position, "size": chunk.size,
                    "checksum": chunk.checksum, "replicas": [{"worker_id": r.worker_id, "state": r.state,
                    "available": self.live(session.get(Worker, r.worker_id))} for r in replicas]})
            return result

    @staticmethod
    def summary(obj):
        return {"object_id": obj.id, "name": obj.name, "size": obj.size, "content_type": obj.content_type}

    async def delete_object(self, identity):
        # Active download holds this same lock, avoiding removal beneath an established stream.
        async with self.locks[identity]:
            with self.sessions.begin() as session:
                obj = self.available(session, identity)
                obj.state = "deleting"
            # The next maintenance pass does physical cleanup. Logical deletion is now committed.

    async def cleanup(self, identity):
        with self.sessions() as session:
            chunks = session.scalars(select(Chunk).where(Chunk.object_id == identity)).all()
        for chunk in chunks:
            # A surplus-copy deletion may have removed a replica record while an old
            # PUT is still in flight. Fence this chunk on every registered worker.
            with self.sessions.begin() as session:
                for worker in session.scalars(select(Worker)):
                    if session.get(Replica, (chunk.id, worker.id)) is None:
                        session.add(Replica(chunk_id=chunk.id, worker_id=worker.id, state="pending"))
            with self.sessions() as session:
                replicas = session.scalars(select(Replica).where(Replica.chunk_id == chunk.id)).all()
                targets = [(r.worker_id, session.get(Worker, r.worker_id)) for r in replicas]
            for worker_id, worker in targets:
                if not self.live(worker):
                    continue
                response = await self.request("DELETE", worker, chunk.id,
                                              headers={"X-Delete-Permanent": "true"})
                if response is not None and response.status_code in (204, 404):
                    with self.sessions.begin() as session:
                        session.execute(delete(Replica).where(Replica.chunk_id == chunk.id, Replica.worker_id == worker_id))
            with self.sessions.begin() as session:
                if not session.scalar(select(func.count()).select_from(Replica).where(Replica.chunk_id == chunk.id)):
                    session.delete(session.get(Chunk, chunk.id))
        with self.sessions.begin() as session:
            if not session.scalar(select(func.count()).select_from(Chunk).where(Chunk.object_id == identity)):
                obj = session.get(StoredObject, identity)
                if obj:
                    session.delete(obj)

    async def repair(self, chunk, scrub=False):
        with self.sessions() as session:
            if not current(session, chunk, self.repair_intents.get(chunk.id)):
                return
            replicas = session.scalars(select(Replica).where(Replica.chunk_id == chunk.id)).all()
            targets = [(r, session.get(Worker, r.worker_id)) for r in replicas]
        valid = []
        data = None
        for replica, worker in targets:
            if not self.live(worker):
                continue
            if scrub or replica.state != "healthy":
                response = await self.request("GET", worker, chunk.id)
                if response is not None and response.status_code == 200 and len(response.content) == chunk.size and checksum(response.content) == chunk.checksum:
                    data = response.content
                    state = "healthy"
                elif response is not None and response.status_code in (200, 404, 409):
                    state = "bad"
                    log.warning("Scrub found missing or invalid replica: object=%s chunk=%s worker=%s",
                                chunk.object_id, chunk.id, worker.id)
                else:
                    # A timeout/server error cannot establish whether the bytes are corrupt.
                    state = "pending" if replica.state != "bad" else "bad"
                with self.sessions.begin() as session:
                    if not current(session, chunk, self.repair_intents.get(chunk.id)):
                        return
                    session.get(Replica, (chunk.id, worker.id)).state = state
                if state == "healthy":
                    valid.append(worker.id)
            else:
                valid.append(worker.id)
        if len(valid) < 2:
            try:
                data = data if data is not None else await self.fetch(chunk)
            except HTTPException:
                log.error("No readable replica: object=%s chunk=%s", chunk.object_id, chunk.id)
                return
            await self.place(chunk, data)
        # Before trimming extras, verify at least two actual copies. Never trust stale inventory.
        with self.sessions() as session:
            replicas = session.scalars(select(Replica).where(Replica.chunk_id == chunk.id)).all()
            targets = [(r, session.get(Worker, r.worker_id)) for r in replicas]
        if len(targets) <= 2:
            return
        verified = []
        for replica, worker in sorted(targets, key=lambda pair: pair[0].worker_id):
            if not self.live(worker):
                continue
            response = await self.request("GET", worker, chunk.id)
            if response is not None and response.status_code == 200 and checksum(response.content) == chunk.checksum and len(response.content) == chunk.size:
                verified.append(worker.id)
                with self.sessions.begin() as session:
                    if not current(session, chunk, self.repair_intents.get(chunk.id)):
                        return
                    session.get(Replica, (chunk.id, worker.id)).state = "healthy"
            if len(verified) == 2:
                break
        if len(verified) < 2:
            return
        for replica, worker in targets:
            with self.sessions() as session:
                if not current(session, chunk, self.repair_intents.get(chunk.id)):
                    return
            if worker.id in verified or not self.live(worker):
                continue
            response = await self.request("DELETE", worker, chunk.id)
            if response is not None and response.status_code in (204, 404):
                with self.sessions.begin() as session:
                    session.execute(delete(Replica).where(Replica.chunk_id == chunk.id, Replica.worker_id == worker.id))

    def recover_startup(self):
        with self.sessions.begin() as session:
            for obj in session.scalars(select(StoredObject).where(StoredObject.state == "uploading")):
                obj.state = "deleting"

    async def maintenance(self, full_scrub=False):
        async with self.maintenance_lock:
            await self.registry.reconcile()
            with self.sessions() as session:
                objects = session.scalars(select(StoredObject.id).order_by(StoredObject.id)).all()
            for identity in objects:
                if self.locks[identity].locked():
                    continue
                async with self.locks[identity]:
                    # Re-read under the lock; an upload may have completed since the
                    # pass began. An uploading object without an active lock is abandoned.
                    with self.sessions.begin() as session:
                        obj = session.get(StoredObject, identity)
                        if obj is None:
                            continue
                        if obj.state == "uploading":
                            obj.state = "deleting"
                        state = obj.state
                    if state == "deleting":
                        await self.cleanup(identity)
                    elif state == "available":
                        with self.sessions() as session:
                            obj = session.get(StoredObject, identity)
                            if obj is None or obj.state != "available":
                                continue
                            chunks = session.scalars(select(Chunk).where(Chunk.object_id == identity)).all()
                        with self.sessions.begin() as session:
                            for chunk in chunks:
                                if full_scrub or needs_repair(self, session, chunk.id):
                                    enqueue(session, chunk.id, 'verification' if full_scrub else 'availability')
            with self.sessions() as session:
                jobs = session.scalars(select(RepairJob).order_by(RepairJob.updated_at, RepairJob.chunk_id).limit(32)).all()
            for job in jobs:
                with self.sessions() as session:
                    chunk = session.get(Chunk, job.chunk_id)
                if chunk is None:
                    continue
                if self.locks[chunk.object_id].locked():
                    with self.sessions.begin() as session:
                        pending = session.get(RepairJob, chunk.id)
                        if pending:
                            pending.updated_at = now()
                    continue
                async with self.locks[chunk.object_id]:
                    with self.sessions.begin() as session:
                        if not current(session, chunk, job.revision):
                            continue
                        session.get(RepairJob, chunk.id).attempts += 1
                    self.repair_intents[chunk.id] = job.revision
                    try:
                        await self.repair(chunk, scrub=True)
                        with self.sessions.begin() as session:
                            if current(session, chunk, job.revision):
                                pending = session.get(RepairJob, chunk.id)
                                if not needs_repair(self, session, chunk.id):
                                    session.delete(pending)
                                else:
                                    pending.last_error = 'Awaiting valid source, capacity or worker recovery'
                                    pending.updated_at = now()
                    except Exception as error:
                        log.exception('Repair job deferred: chunk=%s', chunk.id)
                        with self.sessions.begin() as session:
                            pending = session.get(RepairJob, chunk.id)
                            if pending:
                                pending.last_error = type(error).__name__
                                pending.updated_at = now()
                    finally:
                        self.repair_intents.pop(chunk.id, None)
