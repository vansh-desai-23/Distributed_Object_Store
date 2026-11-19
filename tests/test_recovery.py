"""failure detection, replica availability, and returning workers."""
from datetime import timedelta

import pytest
from sqlalchemy import select

from object_store.models import Chunk, Replica, Worker, now


async def test_returning_heartbeat_invalidates_stale_replica_health(cluster):
    meta = (await cluster.upload()).json()
    worker = meta['chunks'][0]['replicas'][0]['worker_id']
    cluster.stop(worker)
    cluster.offline.remove(worker)
    response = await cluster.client.post('/internal/workers/heartbeat', json=cluster.report(worker))
    assert response.status_code == 200
    with cluster.store.sessions() as session:
        assert session.get(Replica, (meta['chunks'][0]['chunk_id'], worker)).state == 'pending'


async def test_explicit_reregistration_invalidates_health_even_before_timeout(cluster):
    meta = (await cluster.upload()).json()
    worker = meta['chunks'][0]['replicas'][0]['worker_id']
    await cluster.register(worker)
    with cluster.store.sessions() as session:
        assert session.get(Replica, (meta['chunks'][0]['chunk_id'], worker)).state == 'pending'


async def test_network_failure_is_not_reported_as_corruption(cluster, monkeypatch):
    meta = (await cluster.upload()).json()
    async def unreachable(*args, **kwargs):
        return None
    monkeypatch.setattr(cluster.store, 'request', unreachable)
    await cluster.store.maintenance(full_scrub=True)
    with cluster.store.sessions() as session:
        assert all(r.state != 'bad' for r in session.scalars(select(Replica)))


async def test_regular_heartbeat_preserves_validated_replicas(cluster):
    meta = (await cluster.upload()).json()
    worker = meta['chunks'][0]['replicas'][0]['worker_id']
    response = await cluster.client.post('/internal/workers/heartbeat', json=cluster.report(worker))
    assert response.status_code == 200
    with cluster.store.sessions() as session:
        assert session.get(Replica, (meta['chunks'][0]['chunk_id'], worker)).state == 'healthy'


async def test_failure_before_heartbeat_expiry_falls_back_and_uploads_elsewhere(cluster):
    meta = (await cluster.upload()).json()
    worker = meta['chunks'][0]['replicas'][0]['worker_id']
    cluster.offline.add(worker)  # No timestamp change: controller has not detected it yet.
    response = await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")
    assert response.status_code == 200 and response.content == b'example'
    upload = await cluster.upload(b'new data')
    assert upload.status_code == 201
    assert all(r['worker_id'] != worker for c in upload.json()['chunks'] for r in c['replicas'] if r['state'] == 'healthy')


async def test_repair_waits_for_capacity_then_recovers(cluster):
    meta = (await cluster.upload()).json()
    chunk = meta['chunks'][0]
    holders = {r['worker_id'] for r in chunk['replicas']}
    failed = next(iter(holders))
    cluster.stop(failed)
    with cluster.store.sessions.begin() as session:
        for worker in session.scalars(select(Worker)):
            if worker.id not in holders:
                worker.free_bytes = 0
    await cluster.store.maintenance()
    replicas = cluster.store.metadata(meta['object_id'])['chunks'][0]['replicas']
    assert sum(r['available'] and r['state'] == 'healthy' for r in replicas) == 1
    assert (await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")).content == b'example'
    with cluster.store.sessions.begin() as session:
        for worker in session.scalars(select(Worker)):
            worker.free_bytes = 10**9
    await cluster.store.maintenance()
    replicas = cluster.store.metadata(meta['object_id'])['chunks'][0]['replicas']
    assert sum(r['available'] and r['state'] == 'healthy' for r in replicas) == 2


async def test_no_source_preserved_until_worker_returns(cluster):
    meta = (await cluster.upload()).json()
    chunk = meta['chunks'][0]
    holders = [r['worker_id'] for r in chunk['replicas']]
    for worker in holders:
        cluster.stop(worker)
    await cluster.store.maintenance()
    assert (await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")).status_code == 503
    with cluster.store.sessions() as session:
        assert session.get(Chunk, chunk['chunk_id']) is not None
    cluster.offline.remove(holders[0])
    await cluster.register(holders[0])
    await cluster.store.maintenance()
    assert (await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")).content == b'example'
    replicas = cluster.store.metadata(meta['object_id'])['chunks'][0]['replicas']
    assert sum(r['available'] and r['state'] == 'healthy' for r in replicas) == 2


async def test_returning_worker_missing_disk_copy_is_repaired(cluster):
    meta = (await cluster.upload()).json()
    chunk = meta['chunks'][0]
    worker = chunk['replicas'][0]['worker_id']
    cluster.stop(worker)
    cluster.workers[worker].state.disk.delete(chunk['chunk_id'])
    cluster.offline.remove(worker)
    await cluster.register(worker)
    with cluster.store.sessions() as session:
        stored = session.get(Chunk, chunk['chunk_id'])
    await cluster.store.registry.reconcile()
    # Not in the scrub batch: pending return still forces validation immediately.
    await cluster.store.repair(stored, scrub=False)
    replicas = cluster.store.metadata(meta['object_id'])['chunks'][0]['replicas']
    assert sum(r['available'] and r['state'] == 'healthy' for r in replicas) == 2
    for replica in replicas:
        if replica['state'] == 'healthy':
            assert cluster.workers[replica['worker_id']].state.disk.get(chunk['chunk_id']) == b'example'


async def test_heartbeat_expiry_boundary(cluster, monkeypatch):
    import object_store.service as service
    instant = now()
    monkeypatch.setattr(service, 'now', lambda: instant)
    with cluster.store.sessions() as session:
        worker = session.get(Worker, 'worker1')
    worker.last_seen = instant - timedelta(seconds=9.999)
    assert cluster.store.live(worker)
    worker.last_seen = instant - timedelta(seconds=10)
    assert not cluster.store.live(worker)
