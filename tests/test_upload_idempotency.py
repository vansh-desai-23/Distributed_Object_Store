"""Durable upload replay, conflicting requests and recovery boundaries."""
import asyncio
from io import BytesIO
import json
from uuid import uuid4

import httpx
import pytest
from fastapi import HTTPException
from sqlalchemy import select, func
from starlette.datastructures import UploadFile

from object_store.common import checksum
from object_store.controller import create_app
from object_store.idempotency import fingerprint
from object_store.models import Base, StoredObject, UploadReceipt, Chunk, WorkerOperation


async def upload(client, key='test-key', data=b'original', name='test.bin', metadata=None, content_type='application/octet-stream'):
    return await client.post('/api/v1/objects', headers={'Idempotency-Key': key},
                             files={'file': (name, data, content_type)},
                             data={'metadata': json.dumps(metadata or {'owner': 'student'})})


@pytest.mark.parametrize('data', [b'', b'original', b'x' * 1_000_001])
async def test_identical_retry_returns_exact_original_without_writes(cluster, data):
    first = await upload(cluster.client, data=data)
    assert first.status_code == 201 and first.headers['idempotency-replayed'] == 'false'
    with cluster.store.sessions() as session:
        operations = [(o.chunk_id, o.worker_id, o.sequence) for o in session.scalars(select(WorkerOperation))]
    # Replay does not require healthy workers and must not place the bytes again.
    for worker in cluster.workers:
        cluster.stop(worker)
    second = await upload(cluster.client, data=data)
    assert second.status_code == 201 and second.json() == first.json()
    assert second.headers['idempotency-replayed'] == 'true'
    with cluster.store.sessions() as session:
        assert session.scalar(select(func.count()).select_from(StoredObject)) == 1
        assert operations == [(o.chunk_id, o.worker_id, o.sequence) for o in session.scalars(select(WorkerOperation))]


@pytest.mark.parametrize('changed', [dict(data=b'different'), dict(name='other.bin'),
                                    dict(content_type='text/plain'), dict(metadata={'owner': 'someone'})])
async def test_key_conflicts_with_changed_request(cluster, changed):
    assert (await upload(cluster.client)).status_code == 201
    assert (await upload(cluster.client, **changed)).status_code == 409
    with cluster.store.sessions() as session:
        assert session.scalar(select(func.count()).select_from(StoredObject)) == 1


async def test_metadata_order_is_not_a_conflict(cluster):
    first = await upload(cluster.client, metadata={'a': '1', 'b': '2'})
    second = await upload(cluster.client, metadata={'b': '2', 'a': '1'})
    assert first.json() == second.json()
    assert second.headers['idempotency-replayed'] == 'true'


@pytest.mark.parametrize('key', ['', 'bad key', 'x' * 129, 'bad/key'])
async def test_invalid_keys_fail_before_creating_objects(cluster, key):
    assert (await upload(cluster.client, key=key)).status_code == 400
    assert (await cluster.client.get('/api/v1/objects')).json()['objects'] == []


async def test_duplicate_key_headers_are_rejected(cluster):
    response = await cluster.client.post('/api/v1/objects',
        headers=[('Idempotency-Key', 'one'), ('Idempotency-Key', 'two')], files={'file': ('a', b'x')})
    assert response.status_code == 400


async def test_lost_response_retry_replays_committed_upload(cluster):
    lost = True
    async def transport(request):
        nonlocal lost
        response = await cluster.client.request(request.method, request.url.path,
                                                headers=request.headers, content=await request.aread())
        if lost:
            lost = False
            raise httpx.ReadError('response lost after commit', request=request)
        return response
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport), base_url='http://controller') as client:
        with pytest.raises(httpx.ReadError):
            await upload(client)
        retry = await upload(client)
    assert retry.status_code == 201 and retry.headers['idempotency-replayed'] == 'true'
    assert len((await cluster.client.get('/api/v1/objects')).json()['objects']) == 1


async def test_concurrent_identical_requests_publish_one_object(cluster, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    original = cluster.store.place
    async def paused(*args):
        entered.set()
        await release.wait()
        return await original(*args)
    monkeypatch.setattr(cluster.store, 'place', paused)
    first = asyncio.create_task(upload(cluster.client))
    await asyncio.wait_for(entered.wait(), 3)
    second = asyncio.create_task(upload(cluster.client))
    try:
        await asyncio.sleep(0)
        release.set()
        responses = await asyncio.wait_for(asyncio.gather(first, second), 8)
        assert all(r.status_code == 201 for r in responses)
        assert responses[0].json() == responses[1].json()
        assert {r.headers['idempotency-replayed'] for r in responses} == {'false', 'true'}
        assert len((await cluster.client.get('/api/v1/objects')).json()['objects']) == 1
    finally:
        release.set()
        await asyncio.gather(first, second, return_exceptions=True)


async def test_failed_upload_can_retry_same_key_and_cleans_old_attempt(cluster, monkeypatch):
    original = cluster.store.place
    async def failed(*args):
        return False
    monkeypatch.setattr(cluster.store, 'place', failed)
    assert (await upload(cluster.client)).status_code == 503
    with cluster.store.sessions() as session:
        old_id = session.get(UploadReceipt, 'test-key').object_id
        assert session.get(UploadReceipt, 'test-key').state == 'failed'
    monkeypatch.setattr(cluster.store, 'place', original)
    retry = await upload(cluster.client)
    assert retry.status_code == 201 and retry.json()['object_id'] != old_id
    await cluster.store.maintenance()
    with cluster.store.sessions() as session:
        assert session.get(StoredObject, old_id) is None
        assert session.get(UploadReceipt, 'test-key').state == 'completed'


async def test_deleted_upload_is_not_recreated_and_patch_does_not_change_receipt(cluster):
    first = await upload(cluster.client)
    identity = first.json()['object_id']
    await cluster.client.patch(f'/api/v1/objects/{identity}/metadata', json={'owner': 'updated'})
    assert (await upload(cluster.client)).json() == first.json()
    await cluster.store.delete_object(identity)
    assert (await upload(cluster.client)).status_code == 410
    await cluster.store.maintenance()
    assert (await upload(cluster.client)).status_code == 410
    assert (await upload(cluster.client, key='new-key')).status_code == 201


async def test_unkeyed_uploads_and_different_keys_remain_independent(cluster):
    responses = [await cluster.upload(), await cluster.upload(),
                 await upload(cluster.client, key='a'), await upload(cluster.client, key='b')]
    assert len({r.json()['object_id'] for r in responses}) == 4


async def test_cancelled_upload_releases_key_and_retries(cluster, monkeypatch):
    original = cluster.store.place
    async def cancel(*args):
        raise asyncio.CancelledError()
    monkeypatch.setattr(cluster.store, 'place', cancel)
    file = UploadFile(BytesIO(b'original'), filename='test.bin')
    with pytest.raises(asyncio.CancelledError):
        await cluster.store.idempotency.upload(file, {'owner': 'student'}, 'test-key')
    await file.close()
    monkeypatch.setattr(cluster.store, 'place', original)
    assert (await upload(cluster.client)).status_code == 201


async def test_receipt_survives_real_database_reopen(cluster, tmp_path):
    url = f'sqlite:///{tmp_path / "durable.db"}'
    first_body = None
    for attempt in range(2):
        app = create_app(url, transport=httpx.MockTransport(cluster.route), background=False)
        Base.metadata.create_all(app.state.engine)
        async with app.router.lifespan_context(app), httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url='http://controller') as client:
            if attempt == 0:
                for worker in cluster.workers:
                    assert (await client.post('/internal/workers/register', json=cluster.report(worker))).status_code == 200
                await app.state.store.registry.reconcile()
            response = await upload(client)
            assert response.status_code == 201
            if attempt == 0:
                first_body = response.json()
            else:
                assert response.json() == first_body
                assert response.headers['idempotency-replayed'] == 'true'


async def test_abandoned_pending_upload_is_retryable_after_startup(cluster):
    identity = str(uuid4())
    with cluster.store.sessions.begin() as session:
        session.add(StoredObject(id=identity, name='test.bin', content_type='application/octet-stream'))
        session.add(UploadReceipt(key='test-key', object_id=identity, state='pending', fingerprint=fingerprint(
            'test.bin', 'application/octet-stream', {'owner': 'student'}, 8, checksum(b'original'))))
    cluster.store.recover_startup()
    result = await upload(cluster.client)
    assert result.status_code == 201 and result.json()['object_id'] != identity
    await cluster.store.maintenance()
    with cluster.store.sessions() as session:
        assert session.get(StoredObject, identity) is None


async def test_receipt_and_publication_roll_back_together(cluster, monkeypatch):
    original = cluster.store.metadata
    def failing(identity, session=None):
        if session is not None:
            raise RuntimeError('receipt construction failed before commit')
        return original(identity)
    monkeypatch.setattr(cluster.store, 'metadata', failing)
    with pytest.raises(RuntimeError):
        await upload(cluster.client)
    assert (await cluster.client.get('/api/v1/objects')).json()['objects'] == []
    with cluster.store.sessions() as session:
        assert session.get(UploadReceipt, 'test-key').state == 'failed'
    monkeypatch.setattr(cluster.store, 'metadata', original)
    assert (await upload(cluster.client)).status_code == 201


async def test_lost_database_commit_ack_does_not_delete_committed_object(cluster, monkeypatch):
    from contextlib import contextmanager
    from sqlalchemy.exc import OperationalError
    original = cluster.store.sessions.begin
    failed = False
    @contextmanager
    def uncertain_commit():
        nonlocal failed
        should_fail = False
        with original() as session:
            yield session
            receipt = session.get(UploadReceipt, 'test-key')
            should_fail = not failed and receipt is not None and receipt.state == 'completed'
        if should_fail:
            failed = True
            raise OperationalError('COMMIT acknowledgment lost', {}, Exception('connection lost'))
    monkeypatch.setattr(cluster.store.sessions, 'begin', uncertain_commit)
    assert (await upload(cluster.client)).status_code == 503
    retry = await upload(cluster.client)
    assert retry.status_code == 201 and retry.headers['idempotency-replayed'] == 'true'
    with cluster.store.sessions() as session:
        assert session.get(StoredObject, retry.json()['object_id']).state == 'available'
        assert session.get(UploadReceipt, 'test-key').state == 'completed'


async def test_pending_upload_returns_conflict_without_another_attempt(cluster):
    identity = str(uuid4())
    with cluster.store.sessions.begin() as session:
        session.add(StoredObject(id=identity, name='test.bin', content_type='application/octet-stream'))
        session.add(UploadReceipt(key='test-key', object_id=identity, state='pending', fingerprint=fingerprint(
            'test.bin', 'application/octet-stream', {'owner': 'student'}, 8, checksum(b'original'))))
    response = await upload(cluster.client)
    assert response.status_code == 409
    with cluster.store.sessions() as session:
        assert session.scalar(select(func.count()).select_from(StoredObject)) == 1
        assert session.scalar(select(func.count()).select_from(Chunk)) == 0


async def test_openapi_documents_key_and_replay_contract(cluster):
    operation = (await cluster.client.get('/openapi.json')).json()['paths']['/api/v1/objects']['post']
    assert any(p['name'] == 'Idempotency-Key' and p['in'] == 'header' for p in operation['parameters'])
    assert 'Idempotency-Replayed' in operation['responses']['201']['headers']
    assert {'409', '410'} <= operation['responses'].keys()
