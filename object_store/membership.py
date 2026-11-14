"""Boot-aware registration and inventory reconciliation against trusted metadata."""
import asyncio
from collections import defaultdict
import logging
from uuid import UUID

from fastapi import HTTPException
import httpx
from sqlalchemy import select, update

from object_store.models import Worker, Chunk, Replica, StoredObject, WorkerOperation, DamageReceipt, now

from object_store.repair_queue import enqueue

log = logging.getLogger(__name__)


class Registry:
    def __init__(self, store):
        self.store = store
        self.locks = defaultdict(asyncio.Lock)

    @staticmethod
    def matches(worker, body):
        return (body.get("worker_id") == worker.id and
                body.get("storage_id") == worker.storage_id and body.get("boot_id") == worker.boot_id)

    async def report(self, body, registration):
        identity = body["worker_id"]
        async with self.locks[identity]:
            # Check the actual process before accepting a registration, so a delayed
            # old boot's registration cannot replace the currently running boot.
            if registration:
                try:
                    reply = await self.store.client.get(f'{body["url"]}/health')
                    reply.raise_for_status()
                    actual = reply.json()
                except (httpx.HTTPError, ValueError):
                    raise HTTPException(503, "Worker identity probe failed")
                if any(actual.get(key) != body[key] for key in ("worker_id", "storage_id", "boot_id")):
                    raise HTTPException(409, "Registration does not match the running worker")
            if registration:
                changed_volume = False
                with self.store.sessions.begin() as session:
                    worker = session.get(Worker, identity)
                    if worker and worker.storage_id and worker.storage_id != body["storage_id"]:
                        worker.eligible = False
                        worker.membership_version += 1
                        session.execute(update(Replica).where(Replica.worker_id == identity).values(state="pending"))
                        changed_volume = True
                if changed_volume:
                    raise HTTPException(409, "Worker storage identity changed; restore the original volume")
            with self.store.sessions.begin() as session:
                worker = session.get(Worker, identity)
                if worker is None:
                    if not registration:
                        raise HTTPException(404, "Worker must register first")
                    worker = Worker(id=identity, url=body["url"], membership_version=0)
                    session.add(worker)
                elif worker.storage_id and worker.storage_id != body["storage_id"]:
                    raise HTTPException(409, "Worker storage identity changed; restore the original volume")
                elif not registration and not self.matches(worker, body):
                    raise HTTPException(404, "Boot must register first")
                if registration or not self.store.reachable(worker):
                    worker.eligible = False
                    worker.membership_version += 1
                    session.execute(update(Replica).where(Replica.worker_id == identity).values(state="pending"))
                worker.storage_id, worker.boot_id = body["storage_id"], body["boot_id"]
                worker.last_seen, worker.free_bytes = now(), body["free_bytes"]
                worker.scan_progress = body.get('scan_progress')
                acknowledged = []
                for event in body.get('damage_reports', []):
                    receipt = session.get(DamageReceipt, event['event_id'])
                    if receipt is None:
                        session.add(DamageReceipt(event_id=event['event_id'], worker_id=identity,
                                                  storage_id=worker.storage_id))
                        operation = session.get(WorkerOperation, (event['chunk_id'], identity))
                        expected_sequence = operation.sequence if operation else 0
                        replica = session.get(Replica, (event['chunk_id'], identity))
                        if replica and event['sequence'] == expected_sequence:
                            replica.state = 'bad'
                            enqueue(session, event['chunk_id'], 'worker_damage', invalidate=True)
                    acknowledged.append(event['event_id'])
                eligible = worker.eligible
            return {"worker_id": identity, "status": "registered", "eligible": eligible, "acknowledged_damage": acknowledged}

    def current(self, session, snapshot):
        worker = session.get(Worker, snapshot.id)
        return (worker is not None and worker.membership_version == snapshot.membership_version
                and self.matches(worker, {"worker_id": snapshot.id, "storage_id": snapshot.storage_id,
                                          "boot_id": snapshot.boot_id}) and self.store.reachable(worker))

    async def reconcile(self):
        with self.store.sessions() as session:
            workers = session.scalars(select(Worker).where(Worker.eligible.is_(False))).all()
        for worker in workers:
            if not worker.boot_id or not self.store.reachable(worker):
                continue
            try:
                await self.reconcile_one(worker)
            except (httpx.HTTPError, ValueError, KeyError, TypeError):
                log.warning("Inventory reconciliation deferred: worker=%s", worker.id)

    async def reconcile_one(self, worker):
        response = await self.store.client.get(f"{worker.url}/internal/workers/{worker.id}/chunks")
        response.raise_for_status()
        body = response.json()
        if not self.matches(worker, body):
            return
        inventory = {}
        for item in body["chunks"]:
            identity = str(UUID(item["chunk_id"]))
            if identity in inventory or identity != item["chunk_id"] or type(item.get("valid")) is not bool:
                raise ValueError("Malformed inventory")
            inventory[identity] = item
        with self.store.sessions() as session:
            recorded = session.scalars(select(Replica.chunk_id).where(Replica.worker_id == worker.id)).all()
        expectations = []
        for identity in sorted(set(recorded) | inventory.keys()):
            with self.store.sessions() as session:
                chunk = session.get(Chunk, identity)
            if chunk is None:
                # Unknown UUIDs cannot become objects or overwrite authoritative hashes.
                # Repeated permanent deletion also handles a lost acknowledgement.
                with self.store.sessions() as session:
                    if not self.current(session, worker):
                        return
                reply = await self.store.request("DELETE", worker, identity,
                                                  headers={"X-Delete-Permanent": "true"})
                if reply is None or reply.status_code not in (204, 404):
                    return
                continue
            async with self.store.locks[chunk.object_id]:
                with self.store.sessions.begin() as session:
                    if not self.current(session, worker):
                        return
                    chunk = session.get(Chunk, identity)
                    if chunk is None:
                        continue
                    obj = session.get(StoredObject, chunk.object_id)
                    item = inventory.get(identity, {})
                    valid = (item.get("valid") is True and item.get("size") == chunk.size
                             and item.get("checksum") == chunk.checksum)
                    replica = session.get(Replica, (identity, worker.id))
                    if replica is None:
                        replica = Replica(chunk_id=identity, worker_id=worker.id)
                        session.add(replica)
                    replica.state = ("pending" if obj.state != "available" else "healthy" if valid else "bad")
                    operation = session.get(WorkerOperation, (identity, worker.id))
                    if valid and operation and operation.method == 'PUT':
                        operation.state = 'done'
                    expectations.append({'chunk_id': identity, 'size': chunk.size, 'checksum': chunk.checksum,
                                         'sequence': operation.sequence if operation else 0})
                    if obj.state == 'available':
                        enqueue(session, identity, 'inventory', invalidate=True)
        for offset in range(0, len(expectations), 100):
            response = await self.store.client.post(f'{worker.url}/internal/chunks/reconcile',
                json={'chunks': expectations[offset:offset + 100]},
                headers={'X-Worker-Storage': worker.storage_id, 'X-Worker-Boot': worker.boot_id})
            response.raise_for_status()
        # Probe again: a restart during inventory must not make the replacement eligible.
        response = await self.store.client.get(f"{worker.url}/health")
        response.raise_for_status()
        if not self.matches(worker, response.json()):
            return
        with self.store.sessions.begin() as session:
            if self.current(session, worker):
                session.get(Worker, worker.id).eligible = True
