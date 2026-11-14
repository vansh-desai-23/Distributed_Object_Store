"""Persistent identities, boot validation and inventory-gated eligibility."""
from uuid import uuid4

import json
import httpx
import pytest
from sqlalchemy import select

from object_store.common import checksum
from object_store.models import Chunk, Replica, Worker
from object_store.worker import create_app


def test_identity_survives_restart_but_boot_changes(tmp_path):
    first = create_app(tmp_path, "worker1", heartbeat=False)
    second = create_app(tmp_path, "worker1", heartbeat=False)
    assert first.state.membership['storage_id'] == second.state.membership['storage_id']
    assert first.state.membership['boot_id'] != second.state.membership['boot_id']
    assert second.state.disk.inventory() == []
    with pytest.raises(ValueError, match='different worker'):
        create_app(tmp_path, "worker2", heartbeat=False)


async def test_unreconciled_worker_is_not_a_read_or_placement_candidate(cluster):
    meta = (await cluster.upload()).json()
    holders = [r['worker_id'] for r in meta['chunks'][0]['replicas']]
    for worker in holders:
        await cluster.register(worker)
    assert (await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")).status_code == 503
    other = (await cluster.upload()).json()
    assert not set(holders) & {r['worker_id'] for r in other['chunks'][0]['replicas']}
    await cluster.store.registry.reconcile()
    assert (await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")).content == b'example'


async def test_restart_requires_register_and_rejects_old_boot(cluster):
    worker = 'worker1'
    old = cluster.report(worker)
    root = cluster.workers[worker].state.disk.root
    cluster.workers[worker] = create_app(root, worker, heartbeat=False)
    assert (await cluster.client.post('/internal/workers/heartbeat', json=cluster.report(worker))).status_code == 404
    await cluster.register(worker)
    assert (await cluster.client.post('/internal/workers/heartbeat', json=old)).status_code == 404
    assert (await cluster.client.post('/internal/workers/register', json=old)).status_code == 409
    await cluster.store.registry.reconcile()
    with cluster.store.sessions() as session:
        assert cluster.store.live(session.get(Worker, worker))


async def test_replaced_volume_cannot_impersonate_original(cluster, tmp_path):
    cluster.workers['worker1'] = create_app(tmp_path / 'replacement', 'worker1', heartbeat=False)
    reply = await cluster.client.post('/internal/workers/register', json=cluster.report('worker1'))
    assert reply.status_code == 409


async def test_inventory_matches_authority_discovers_extra_and_removes_orphan(cluster):
    meta = (await cluster.upload()).json()
    chunk = meta['chunks'][0]
    holders = {r['worker_id'] for r in chunk['replicas']}
    extra = next(w for w in cluster.workers if w not in holders)
    disk = cluster.workers[extra].state.disk
    disk.put(chunk['chunk_id'], b'example', checksum(b'example'))
    orphan = str(uuid4())
    disk.put(orphan, b'orphan', checksum(b'orphan'))
    await cluster.register(extra)
    await cluster.store.registry.reconcile()
    with cluster.store.sessions() as session:
        assert session.get(Replica, (chunk['chunk_id'], extra)).state == 'healthy'
        assert session.get(Chunk, chunk['chunk_id']).checksum == checksum(b'example')
    assert (disk.root / f'{orphan}.deleted').exists()
    await cluster.store.maintenance()
    with cluster.store.sessions() as session:
        assert len(session.scalars(select(Replica).where(Replica.chunk_id == chunk['chunk_id'])).all()) == 2


async def test_missing_and_consistently_corrupt_inventory_cannot_change_expected_hash(cluster):
    meta = (await cluster.upload()).json()
    chunk = meta['chunks'][0]
    first, second = [r['worker_id'] for r in chunk['replicas']]
    cluster.workers[first].state.disk.delete(chunk['chunk_id'])
    disk = cluster.workers[second].state.disk
    disk.delete(chunk['chunk_id'])
    disk.path(chunk['chunk_id']).write_bytes(b'changed')
    disk.path(chunk['chunk_id']).with_suffix('.json').write_text(json.dumps({'size': 7, 'checksum': checksum(b'changed')}))
    for worker in (first, second):
        await cluster.register(worker)
    await cluster.store.registry.reconcile()
    with cluster.store.sessions() as session:
        assert session.get(Chunk, chunk['chunk_id']).checksum == checksum(b'example')
        assert all(session.get(Replica, (chunk['chunk_id'], w)).state == 'bad' for w in (first, second))
    assert (await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")).status_code == 503


async def test_restart_during_inventory_does_not_enable_worker(cluster, monkeypatch):
    await cluster.register('worker1')
    original = cluster.store.client.get

    async def restarting(url, **kwargs):
        response = await original(url, **kwargs)
        if url.endswith('/worker1/chunks'):
            app = cluster.workers['worker1']
            cluster.workers['worker1'] = create_app(app.state.disk.root, 'worker1', heartbeat=False)
        return response

    monkeypatch.setattr(cluster.store.client, 'get', restarting)
    await cluster.store.registry.reconcile()
    with cluster.store.sessions() as session:
        assert not session.get(Worker, 'worker1').eligible


async def test_inventory_failure_leaves_worker_ineligible_and_retries(cluster, monkeypatch):
    await cluster.register('worker1')
    original = cluster.store.client.get

    async def failing(url, **kwargs):
        if url.endswith('/worker1/chunks'):
            return httpx.Response(503, request=httpx.Request('GET', url))
        return await original(url, **kwargs)

    monkeypatch.setattr(cluster.store.client, 'get', failing)
    await cluster.store.registry.reconcile()
    with cluster.store.sessions() as session:
        assert not session.get(Worker, 'worker1').eligible
    monkeypatch.setattr(cluster.store.client, 'get', original)
    await cluster.store.registry.reconcile()
    with cluster.store.sessions() as session:
        assert session.get(Worker, 'worker1').eligible


async def test_replaced_volume_disables_old_membership(cluster, tmp_path):
    cluster.workers['worker1'] = create_app(tmp_path / 'new-volume', 'worker1', heartbeat=False)
    await cluster.client.post('/internal/workers/register', json=cluster.report('worker1'))
    with cluster.store.sessions() as session:
        assert not session.get(Worker, 'worker1').eligible


async def test_stale_inventory_cannot_complete_new_registration(cluster, monkeypatch):
    await cluster.register('worker1')
    original = cluster.store.client.get

    async def reregistering(url, **kwargs):
        response = await original(url, **kwargs)
        if url.endswith('/worker1/chunks'):
            await cluster.register('worker1')
        return response

    monkeypatch.setattr(cluster.store.client, 'get', reregistering)
    await cluster.store.registry.reconcile()
    with cluster.store.sessions() as session:
        assert not session.get(Worker, 'worker1').eligible


async def test_target_boot_header_prevents_request_to_restarted_worker(cluster):
    old = cluster.report('worker1')
    app = cluster.workers['worker1']
    app = create_app(app.state.disk.root, 'worker1', heartbeat=False)
    chunk = str(uuid4())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://worker1') as client:
        response = await client.put(f'/internal/chunks/{chunk}', content=b'x', headers={
            'X-Chunk-Checksum': checksum(b'x'), 'X-Worker-Boot': old['boot_id']})
        assert response.status_code == 409
        assert (await client.get('/ready')).status_code == 503
    assert not app.state.disk.path(chunk).exists()


def test_migration_preserves_existing_membership_and_demotes_it(tmp_path, monkeypatch):
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import create_engine, text
    url = f'sqlite:///{tmp_path / "migration.db"}'
    monkeypatch.setenv('DATABASE_URL', url)
    config = Config('alembic.ini')
    command.upgrade(config, '0001')
    engine = create_engine(url)
    with engine.begin() as session:
        session.execute(text("INSERT INTO workers (id,url,last_seen,free_bytes) VALUES ('worker1','http://worker1:8000',CURRENT_TIMESTAMP,1000)"))
    command.upgrade(config, 'head')
    with engine.connect() as session:
        row = session.execute(text('SELECT storage_id,boot_id,eligible,membership_version,free_bytes FROM workers')).one()
        assert tuple(row) == (None, None, 0, 1, 1000)
    engine.dispose()
