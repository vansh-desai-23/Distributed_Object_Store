"""corruption detection, authoritative hashes, and safe repair."""
import asyncio
import json
from uuid import uuid4

import httpx
import pytest
from fastapi import HTTPException
from sqlalchemy import select

from object_store.common import CHUNK_SIZE, checksum
from object_store.models import Chunk, Replica
from object_store.worker import ChunkDisk


@pytest.mark.parametrize('mode', ['oversized', 'boolean_size', 'oversized_manifest'])
def test_worker_rejects_invalid_manifest_and_chunk_bounds(tmp_path, mode):
    disk = ChunkDisk(tmp_path)
    identity = str(uuid4())
    data = b'x' * (CHUNK_SIZE + 1) if mode == 'oversized' else b'x'
    disk.path(identity).write_bytes(data)
    info = {'size': True if mode == 'boolean_size' else len(data), 'checksum': checksum(data)}
    if mode == 'oversized_manifest':
        info['unexpected_padding'] = 'x' * 10_000
    disk.path(identity).with_suffix('.json').write_text(json.dumps(info))
    with pytest.raises(HTTPException) as error:
        disk.get(identity)
    assert error.value.status_code == 409
    assert disk.inventory()[0]['valid'] is False


@pytest.mark.parametrize('mode', ['flip', 'truncate', 'append', 'missing_manifest', 'invalid_json', 'false_manifest_hash'])
async def test_corruption_variants_fall_back_and_repair(cluster, mode):
    data = b'original binary\x00\xff'
    meta = (await cluster.upload(data)).json()
    chunk = meta['chunks'][0]
    target = chunk['replicas'][0]['worker_id']
    path = cluster.workers[target].state.disk.path(chunk['chunk_id'])
    if mode == 'flip':
        path.write_bytes(bytes([data[0] ^ 1]) + data[1:])
    elif mode == 'truncate':
        path.write_bytes(data[:-1])
    elif mode == 'append':
        path.write_bytes(data + b'extra')
    elif mode == 'missing_manifest':
        path.with_suffix('.json').unlink()
    elif mode == 'invalid_json':
        path.with_suffix('.json').write_text('{invalid')
    else:
        path.with_suffix('.json').write_text(json.dumps({'size': len(data), 'checksum': '0' * 64}))
    response = await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")
    assert response.status_code == 200 and response.content == data
    await cluster.store.maintenance(full_scrub=True)
    replicas = cluster.store.metadata(meta['object_id'])['chunks'][0]['replicas']
    assert len(replicas) == 2
    for replica in replicas:
        assert replica['state'] == 'healthy'
        assert cluster.workers[replica['worker_id']].state.disk.get(chunk['chunk_id']) == data


async def test_controller_hash_rejects_consistently_altered_worker_copy(cluster):
    data = b'good data'
    meta = (await cluster.upload(data)).json()
    chunk = meta['chunks'][0]
    target = chunk['replicas'][0]['worker_id']
    disk = cluster.workers[target].state.disk
    altered = b'evil data'
    disk.path(chunk['chunk_id']).write_bytes(altered)
    disk.path(chunk['chunk_id']).with_suffix('.json').write_text(json.dumps({'size': len(altered), 'checksum': checksum(altered)}))
    with pytest.raises(HTTPException):
        disk.get(chunk['chunk_id'])  # The durable catalog also rejects the altered manifest.
    response = await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")
    assert response.status_code == 200 and response.content == data
    with cluster.store.sessions() as session:
        assert session.get(Replica, (chunk['chunk_id'], target)).state == 'bad'
    await cluster.store.maintenance(full_scrub=True)
    for replica in cluster.store.metadata(meta['object_id'])['chunks'][0]['replicas']:
        assert cluster.workers[replica['worker_id']].state.disk.get(chunk['chunk_id']) == data


async def test_rotating_scrub_finds_corruption_without_download(cluster):
    meta = (await cluster.upload(b'x' * (CHUNK_SIZE * 17))).json()
    chunks = sorted(meta['chunks'], key=lambda c: c['chunk_id'])
    chunk = chunks[-1]  # Beyond the first 16-record scrub batch.
    target = chunk['replicas'][0]['worker_id']
    cluster.workers[target].state.disk.path(chunk['chunk_id']).write_bytes(b'broken')
    for _ in range(2):
        for worker, app in cluster.workers.items():
            app.state.disk.scan_batch()
            reply = await cluster.client.post('/internal/workers/heartbeat', json={
                **cluster.report(worker), **app.state.disk.catalog.report()})
            assert reply.status_code == 200
            app.state.disk.catalog.acknowledge(reply.json()['acknowledged_damage'])
        await cluster.store.maintenance()
    current = next(c for c in cluster.store.metadata(meta['object_id'])['chunks'] if c['chunk_id'] == chunk['chunk_id'])
    assert len(current['replicas']) == 2
    for replica in current['replicas']:
        assert cluster.workers[replica['worker_id']].state.disk.get(chunk['chunk_id']) == b'x' * CHUNK_SIZE


async def test_both_corrupt_copies_are_not_propagated_or_discarded(cluster):
    meta = (await cluster.upload()).json()
    chunk = meta['chunks'][0]
    holders = {r['worker_id'] for r in chunk['replicas']}
    for worker in holders:
        cluster.workers[worker].state.disk.path(chunk['chunk_id']).write_bytes(b'broken')
    await cluster.store.maintenance(full_scrub=True)
    assert (await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")).status_code == 503
    with cluster.store.sessions() as session:
        rows = session.scalars(select(Replica).where(Replica.chunk_id == chunk['chunk_id'])).all()
        assert {r.worker_id for r in rows} == holders
        assert all(r.state == 'bad' for r in rows)
    assert all(cluster.workers[w].state.disk.path(chunk['chunk_id']).read_bytes() == b'broken' for w in holders)
    assert all(not cluster.workers[w].state.disk.inventory() for w in set(cluster.workers) - holders)


async def test_bad_later_chunk_never_emitted_and_stream_lock_released(cluster):
    original = b'a' * CHUNK_SIZE + b'final chunk'
    meta = (await cluster.upload(original)).json()
    chunk = meta['chunks'][1]
    for replica in chunk['replicas']:
        cluster.workers[replica['worker_id']].state.disk.path(chunk['chunk_id']).write_bytes(b'bad')
    endpoint = next(r.endpoint for r in cluster.app.routes if getattr(r, 'path', '') == '/api/v1/objects/{object_id}' and 'GET' in r.methods)
    response = await endpoint(meta['object_id'])
    messages = []
    async def receive():
        return {'type': 'http.disconnect'}
    async def send(message):
        messages.append(message)
    with pytest.raises(HTTPException) as error:
        await response({'type': 'http', 'asgi': {'spec_version': '2.4'}}, receive, send)
    assert error.value.status_code == 503
    emitted = b''.join(m.get('body', b'') for m in messages)
    assert emitted == original[:CHUNK_SIZE]
    assert not any(m['type'] == 'http.response.body' and not m.get('more_body', False) for m in messages)
    await asyncio.wait_for(cluster.store.delete_object(meta['object_id']), 1)


async def test_worker_rejects_wrong_incoming_hash(cluster):
    identity = str(uuid4())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=cluster.workers['worker1']), base_url='http://worker') as client:
        response = await client.put(f'/internal/chunks/{identity}', content=b'new bytes', headers={'X-Chunk-Checksum': '0' * 64})
    assert response.status_code == 400
    assert not cluster.workers['worker1'].state.disk.path(identity).exists()
