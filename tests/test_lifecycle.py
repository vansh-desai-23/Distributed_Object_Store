"""Phase 4: durable lifecycle intents, late requests, and concurrency boundaries."""
import asyncio
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from object_store.common import checksum
from object_store.models import Chunk, Replica, StoredObject
from object_store.worker import ChunkDisk


async def test_inactive_upload_reclaimed_without_controller_restart(cluster):
    identity = str(uuid4())
    with cluster.store.sessions.begin() as session:
        session.add(StoredObject(id=identity, name='abandoned', content_type='application/octet-stream'))
    # Represents an upload whose error cleanup could not reach the DB, which later recovers.
    await cluster.store.maintenance()
    with cluster.store.sessions() as session:
        assert session.get(StoredObject, identity) is None


async def test_delayed_put_cannot_recreate_deleted_chunk(cluster):
    meta = (await cluster.upload()).json()
    chunk = meta['chunks'][0]
    target = chunk['replicas'][0]['worker_id']
    started, release = asyncio.Event(), asyncio.Event()

    async def late_body():
        started.set()
        await release.wait()
        yield b'example'

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=cluster.workers[target]), base_url='http://worker') as worker:
        task = asyncio.create_task(worker.put(f"/internal/chunks/{chunk['chunk_id']}",
            content=late_body(), headers={'X-Chunk-Checksum': checksum(b'example')}))
        try:
            await asyncio.wait_for(started.wait(), 2)
            assert (await cluster.client.delete(f"/api/v1/objects/{meta['object_id']}")).status_code == 204
            await cluster.store.maintenance()
            release.set()
            response = await asyncio.wait_for(task, 2)
            assert response.status_code == 409
            assert cluster.workers[target].state.disk.inventory() == []
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_active_upload_stays_hidden_and_is_not_reclaimed(cluster):
    entered, release = asyncio.Event(), asyncio.Event()
    class PausedUpload:
        filename = 'in-progress.bin'
        content_type = 'application/octet-stream'
        reads = 0
        async def read(self, size):
            self.reads += 1
            if self.reads == 1:
                return b'partial bytes'
            entered.set()
            await release.wait()
            return b''
    task = asyncio.create_task(cluster.store.upload(PausedUpload(), {}))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        with cluster.store.sessions() as session:
            identity = session.scalar(select(StoredObject.id))
        assert (await cluster.client.get('/api/v1/objects')).json()['objects'] == []
        assert (await cluster.client.get(f'/api/v1/objects/{identity}/metadata')).status_code == 404
        await cluster.store.maintenance()
        with cluster.store.sessions() as session:
            assert session.get(StoredObject, identity).state == 'uploading'
        release.set()
        assert (await asyncio.wait_for(task, 2))['object_id'] == identity
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_lost_delete_ack_retains_intent_until_retry(cluster, monkeypatch):
    meta = (await cluster.upload()).json()
    original = cluster.store.request
    async def lose_reply(method, *args, **kwargs):
        response = await original(method, *args, **kwargs)
        return None if method == 'DELETE' else response
    monkeypatch.setattr(cluster.store, 'request', lose_reply)
    await cluster.store.delete_object(meta['object_id'])
    await cluster.store.maintenance()
    with cluster.store.sessions() as session:
        assert session.get(StoredObject, meta['object_id']).state == 'deleting'
    assert all(app.state.disk.inventory() == [] for app in cluster.workers.values())
    monkeypatch.setattr(cluster.store, 'request', original)
    await cluster.store.maintenance()
    with cluster.store.sessions() as session:
        assert session.get(StoredObject, meta['object_id']) is None


async def test_final_delete_blocks_late_copy_on_previously_unlisted_worker(cluster):
    meta = (await cluster.upload()).json()
    chunk = meta['chunks'][0]
    holders = {r['worker_id'] for r in chunk['replicas']}
    target = next(w for w in cluster.workers if w not in holders)
    await cluster.store.delete_object(meta['object_id'])
    await cluster.store.maintenance()
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as error:
        cluster.workers[target].state.disk.put(chunk['chunk_id'], b'example', checksum(b'example'))
    assert error.value.status_code == 409


def test_permanent_delete_survives_worker_restart(tmp_path):
    from fastapi import HTTPException
    disk = ChunkDisk(tmp_path)
    identity = str(uuid4())
    disk.put(identity, b'original', checksum(b'original'))
    disk.delete(identity, permanent=True)
    # Simulate a crash after the tombstone but before removal of old bytes.
    disk.path(identity).write_bytes(b'old bytes')
    restarted = ChunkDisk(tmp_path)
    assert not restarted.path(identity).exists()
    with pytest.raises(HTTPException) as error:
        restarted.put(identity, b'original', checksum(b'original'))
    assert error.value.status_code == 409
    restarted.delete(identity, permanent=True)
    assert restarted.inventory() == []


async def test_repair_and_delete_serialize_without_resurrection(cluster, monkeypatch):
    meta = (await cluster.upload()).json()
    chunk = meta['chunks'][0]
    worker = chunk['replicas'][0]['worker_id']
    cluster.stop(worker)
    entered, release = asyncio.Event(), asyncio.Event()
    original = cluster.store.place
    async def pause_repair(*args):
        entered.set()
        await release.wait()
        return await original(*args)
    monkeypatch.setattr(cluster.store, 'place', pause_repair)
    maintenance = asyncio.create_task(cluster.store.maintenance())
    deletion = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        deletion = asyncio.create_task(cluster.store.delete_object(meta['object_id']))
        await asyncio.sleep(0)
        assert not deletion.done()
        release.set()
        await asyncio.wait_for(maintenance, 3)
        await asyncio.wait_for(deletion, 2)
        cluster.offline.remove(worker)
        await cluster.register(worker)
        await cluster.store.maintenance()
        assert (await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")).status_code == 404
        assert all(app.state.disk.inventory() == [] for app in cluster.workers.values())
    finally:
        release.set()
        await asyncio.gather(maintenance, *([deletion] if deletion else []), return_exceptions=True)


async def test_download_disconnect_releases_lifecycle_lock(cluster):
    from starlette.requests import ClientDisconnect
    meta = (await cluster.upload()).json()
    endpoint = next(r.endpoint for r in cluster.app.routes if getattr(r, 'path', '') == '/api/v1/objects/{object_id}' and 'GET' in r.methods)
    response = await endpoint(meta['object_id'])
    async def receive():
        return {'type': 'http.disconnect'}
    async def send(message):
        if message['type'] == 'http.response.body':
            raise OSError('client disconnected')
    with pytest.raises(ClientDisconnect):
        await response({'type': 'http', 'asgi': {'spec_version': '2.4'}}, receive, send)
    await asyncio.wait_for(cluster.store.delete_object(meta['object_id']), 1)
    assert (await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")).status_code == 404


async def test_cleanup_db_outage_preserves_original_upload_failure(cluster, monkeypatch):
    from sqlalchemy.exc import OperationalError
    original_sessions = cluster.store.sessions
    class UnavailableSessions:
        def begin(self):
            raise OperationalError('database temporarily offline', {}, Exception('offline'))
    async def interrupted_place(*args):
        monkeypatch.setattr(cluster.store, 'sessions', UnavailableSessions())
        raise asyncio.CancelledError()
    monkeypatch.setattr(cluster.store, 'place', interrupted_place)
    class File:
        filename = 'interrupted.bin'
        content_type = 'application/octet-stream'
        async def read(self, size):
            return b'bytes'
    with pytest.raises(asyncio.CancelledError):
        await cluster.store.upload(File(), {})
    monkeypatch.setattr(cluster.store, 'sessions', original_sessions)
    with original_sessions() as session:
        assert session.scalar(select(StoredObject)).state == 'uploading'
    await cluster.store.maintenance()
    with original_sessions() as session:
        assert session.scalar(select(StoredObject)) is None
