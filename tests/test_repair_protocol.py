"""Ordered worker mutations, resumable scans, acknowledged reports and repair races."""
import asyncio
import json
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from object_store.common import checksum
from object_store.models import Chunk, RepairJob, Replica, WorkerOperation, DamageReceipt, StoredObject
from object_store.repair_queue import enqueue
from object_store.worker import ChunkDisk


def test_receipt_retry_restart_and_reordered_operations(tmp_path):
    identity = str(uuid4())
    disk = ChunkDisk(tmp_path)
    original = disk.mutate('PUT', identity, 10, b'bytes', checksum(b'bytes'))
    disk = ChunkDisk(tmp_path)
    assert disk.mutate('PUT', identity, 10, b'bytes', checksum(b'bytes')) == original
    disk.mutate('DELETE', identity, 11)
    for sequence, content in [(10, b'bytes'), (11, b'other')]:
        with pytest.raises(HTTPException) as error:
            disk.mutate('PUT', identity, sequence, content, checksum(content))
        assert error.value.status_code == 409
    disk.mutate('PUT', identity, 12, b'bytes', checksum(b'bytes'))
    with pytest.raises(HTTPException):
        disk.mutate('DELETE', identity, 11)
    assert disk.get(identity) == b'bytes'
    disk.mutate('DELETE', identity, 13, permanent=True)
    with pytest.raises(HTTPException):
        disk.mutate('PUT', identity, 14, b'bytes', checksum(b'bytes'))


def test_interrupted_publication_finishes_after_restart(tmp_path, monkeypatch):
    disk = ChunkDisk(tmp_path)
    identity = str(uuid4())
    original = disk.atomic
    def interrupted(path, data):
        if path.suffix == '.json':
            raise OSError('crash after chunk rename')
        original(path, data)
    monkeypatch.setattr(disk, 'atomic', interrupted)
    with pytest.raises(OSError):
        disk.mutate('PUT', identity, 3, b'bytes', checksum(b'bytes'))
    assert disk.catalog.operation(identity)['state'] == 'pending'
    restarted = ChunkDisk(tmp_path)
    assert restarted.get(identity) == b'bytes'
    assert restarted.catalog.operation(identity)['state'] == 'done'


def test_interrupted_delete_replayed_after_restart(tmp_path, monkeypatch):
    disk = ChunkDisk(tmp_path)
    identity = str(uuid4())
    disk.mutate('PUT', identity, 1, b'bytes', checksum(b'bytes'))
    def interrupted(*args):
        raise OSError('crash before delete')
    monkeypatch.setattr(disk, 'delete', interrupted)
    with pytest.raises(OSError):
        disk.mutate('DELETE', identity, 2, permanent=True)
    restarted = ChunkDisk(tmp_path)
    assert not restarted.path(identity).exists()
    assert restarted.catalog.operation(identity)['state'] == 'done'
    with pytest.raises(HTTPException):
        restarted.mutate('PUT', identity, 3, b'bytes', checksum(b'bytes'))


def test_scan_progress_resumes_and_reports_survive_restart(tmp_path):
    disk = ChunkDisk(tmp_path)
    ids = [f'00000000-0000-0000-0000-{i:012d}' for i in range(1, 20)]
    for identity in ids:
        disk.put(identity, b'bytes', checksum(b'bytes'))
    disk.path(ids[-1]).write_bytes(b'bad')
    progress = disk.scan_batch(8)
    assert progress['cursor'] == ids[7] and progress['checked'] == 8
    disk = ChunkDisk(tmp_path)
    progress = disk.scan_batch(8)
    assert progress['cursor'] == ids[15]
    disk = ChunkDisk(tmp_path)
    progress = disk.scan_batch(8)
    assert progress['cycles'] == 1 and progress['checked'] == 19
    event = disk.catalog.report()['damage_reports'][0]
    assert event['chunk_id'] == ids[-1]
    disk = ChunkDisk(tmp_path)
    assert disk.catalog.report()['damage_reports'] == [event]
    disk.scan_batch(32)
    assert disk.catalog.report()['damage_reports'] == [event]
    disk.catalog.acknowledge([event['event_id']])
    assert disk.catalog.report()['damage_reports'] == []


def test_scanner_uses_catalog_even_if_bytes_and_manifest_agree(tmp_path):
    disk = ChunkDisk(tmp_path)
    identity = str(uuid4())
    disk.put(identity, b'good', checksum(b'good'))
    disk.path(identity).write_bytes(b'evil')
    disk.path(identity).with_suffix('.json').write_text(json.dumps({'checksum': checksum(b'evil'), 'size': 4}))
    disk.scan_batch()
    assert disk.catalog.report()['damage_reports'][0]['chunk_id'] == identity
    assert disk.catalog.expected(identity)['checksum'] == checksum(b'good')


async def send_damage(cluster, worker):
    disk = cluster.workers[worker].state.disk
    disk.scan_batch(100)
    body = {**cluster.report(worker), **disk.catalog.report()}
    response = await cluster.client.post('/internal/workers/heartbeat', json=body)
    assert response.status_code == 200
    return body, response.json()


async def test_damage_is_coalesced_acknowledged_and_repaired_without_read(cluster):
    meta = (await cluster.upload()).json()
    chunk = meta['chunks'][0]
    worker = chunk['replicas'][0]['worker_id']
    disk = cluster.workers[worker].state.disk
    disk.path(chunk['chunk_id']).write_bytes(b'bad')
    body, ack = await send_damage(cluster, worker)
    with cluster.store.sessions() as session:
        job = session.get(RepairJob, chunk['chunk_id'])
        revision = job.revision
        assert session.get(Replica, (chunk['chunk_id'], worker)).state == 'bad'
    # Lose the first acknowledgment, then deliver the same event again.
    again = await cluster.client.post('/internal/workers/heartbeat', json=body)
    assert again.json()['acknowledged_damage'] == ack['acknowledged_damage']
    with cluster.store.sessions() as session:
        assert session.get(RepairJob, chunk['chunk_id']).revision == revision
        assert len(session.scalars(select(DamageReceipt)).all()) == 1
    disk.catalog.acknowledge(again.json()['acknowledged_damage'])
    cluster.store.recover_startup()
    await cluster.store.maintenance()
    with cluster.store.sessions() as session:
        assert session.get(RepairJob, chunk['chunk_id']) is None
    for replica in cluster.store.metadata(meta['object_id'])['chunks'][0]['replicas']:
        assert cluster.workers[replica['worker_id']].state.disk.get(chunk['chunk_id']) == b'example'


async def test_old_damage_after_replacement_is_acknowledged_without_repair(cluster):
    meta = (await cluster.upload()).json()
    chunk = meta['chunks'][0]
    worker = chunk['replicas'][0]['worker_id']
    disk = cluster.workers[worker].state.disk
    disk.path(chunk['chunk_id']).write_bytes(b'bad')
    disk.scan_batch()
    delayed = {**cluster.report(worker), **disk.catalog.report()}
    await cluster.store.maintenance(full_scrub=True)
    response = await cluster.client.post('/internal/workers/heartbeat', json=delayed)
    assert response.json()['acknowledged_damage']
    with cluster.store.sessions() as session:
        assert session.get(RepairJob, chunk['chunk_id']) is None
        replica = session.get(Replica, (chunk['chunk_id'], worker))
        assert replica is None or replica.state == 'healthy'


async def test_new_repair_intent_during_put_cannot_publish_old_intent(cluster, monkeypatch):
    meta = (await cluster.upload()).json()
    chunk = meta['chunks'][0]
    cluster.stop(chunk['replicas'][0]['worker_id'])
    original = cluster.store.request
    invalidated = False
    async def invalidate(method, worker, identity, **kwargs):
        nonlocal invalidated
        response = await original(method, worker, identity, **kwargs)
        if method == 'PUT' and not invalidated:
            invalidated = True
            with cluster.store.sessions.begin() as session:
                enqueue(session, identity, 'new_report', invalidate=True)
        return response
    monkeypatch.setattr(cluster.store, 'request', invalidate)
    await cluster.store.maintenance()
    with cluster.store.sessions() as session:
        job = session.get(RepairJob, chunk['chunk_id'])
        assert job is not None and job.revision > 1
        assert any(r.state == 'pending' for r in session.scalars(select(Replica).where(Replica.chunk_id == chunk['chunk_id'])))
    await cluster.store.maintenance()
    assert sum(r['available'] and r['state'] == 'healthy' for r in cluster.store.metadata(meta['object_id'])['chunks'][0]['replicas']) == 2


async def test_delete_removes_queued_repair_and_never_republishes(cluster):
    meta = (await cluster.upload()).json()
    chunk = meta['chunks'][0]
    with cluster.store.sessions.begin() as session:
        enqueue(session, chunk['chunk_id'], 'test')
    await cluster.store.delete_object(meta['object_id'])
    await cluster.store.maintenance()
    with cluster.store.sessions() as session:
        assert session.get(RepairJob, chunk['chunk_id']) is None
        assert session.get(StoredObject, meta['object_id']) is None
        assert session.scalars(select(WorkerOperation)).first() is not None
    assert all(not app.state.disk.inventory() for app in cluster.workers.values())


async def test_reconciliation_seeds_trusted_catalog_for_legacy_copy(cluster):
    meta = (await cluster.upload()).json()
    chunk = meta['chunks'][0]
    worker = chunk['replicas'][0]['worker_id']
    disk = cluster.workers[worker].state.disk
    with disk.catalog.connect() as db:
        db.execute('DELETE FROM chunks WHERE id=?', (chunk['chunk_id'],))
    assert disk.catalog.expected(chunk['chunk_id']) is None
    await cluster.register(worker)
    await cluster.store.registry.reconcile()
    assert disk.catalog.expected(chunk['chunk_id'])['checksum'] == chunk['checksum']
    disk.path(chunk['chunk_id']).unlink()
    disk.scan_batch()
    assert disk.catalog.report()['damage_reports'][0]['chunk_id'] == chunk['chunk_id']


async def test_controller_operation_order_survives_restart(cluster):
    meta = (await cluster.upload()).json()
    with cluster.store.sessions() as session:
        previous = max(o.sequence for o in session.scalars(select(WorkerOperation)))
    cluster.store.recover_startup()
    await cluster.store.delete_object(meta['object_id'])
    await cluster.store.maintenance()
    with cluster.store.sessions() as session:
        assert all(o.sequence > previous for o in session.scalars(select(WorkerOperation)))


def test_fixed_scan_cycle_is_not_extended_by_new_high_ids(tmp_path):
    disk = ChunkDisk(tmp_path)
    ids = [f'00000000-0000-0000-0000-{i:012d}' for i in range(1, 5)]
    for identity in ids[:3]:
        disk.put(identity, b'x', checksum(b'x'))
    disk.scan_batch(1)
    disk.put(ids[3], b'x', checksum(b'x'))
    disk.scan_batch(2)
    report = disk.catalog.report()['scan_progress']
    assert report['cycles'] == 1 and report['checked'] == 3
    disk.scan_batch(10)
    assert disk.catalog.report()['scan_progress']['checked'] == 7
