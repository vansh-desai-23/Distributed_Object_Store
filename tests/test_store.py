"""Behavioral integration checks, including failure and corruption cases."""
import asyncio
from io import BytesIO
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select
from starlette.datastructures import UploadFile

from object_store.common import CHUNK_SIZE, checksum
from object_store.models import Chunk, Replica, StoredObject
from object_store.worker import ChunkDisk


@pytest.mark.parametrize("size", [0, 1, CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1, 2 * CHUNK_SIZE, 2 * CHUNK_SIZE + 1, 2_500_000])
async def test_roundtrip_boundaries(cluster, size):
    data = (b"\x00\xffbinary\r\n" * (size // 10 + 1))[:size]
    response = await cluster.upload(data)
    assert response.status_code == 201, response.text
    meta = response.json()
    assert meta["size"] == size
    assert meta["checksum"] == checksum(data)
    assert meta["chunk_count"] == (size + CHUNK_SIZE - 1) // CHUNK_SIZE
    for position, chunk in enumerate(meta["chunks"]):
        assert chunk["position"] == position
        assert len({r["worker_id"] for r in chunk["replicas"]}) == 2
    download = await cluster.client.get(f'/api/v1/objects/{meta["object_id"]}')
    assert download.status_code == 200
    assert download.content == data
    assert download.headers["x-object-checksum"] == checksum(data)


async def test_metadata_list_and_delete(cluster):
    meta = (await cluster.upload()).json()
    path = f'/api/v1/objects/{meta["object_id"]}'
    response = await cluster.client.patch(path + "/metadata", json={"owner": "updated", "tag": "demo"})
    assert response.status_code == 200
    assert response.json()["metadata"] == {"owner": "updated", "tag": "demo"}
    assert response.json()["checksum"] == meta["checksum"]
    assert len((await cluster.client.get("/api/v1/objects")).json()["objects"]) == 1
    assert (await cluster.client.delete(path)).status_code == 204
    assert (await cluster.client.get(path)).status_code == 404
    assert (await cluster.client.delete(path)).status_code == 404
    await cluster.store.maintenance()
    assert all(not app.state.disk.inventory() for app in cluster.workers.values())


async def test_worker_failure_repair_and_return(cluster):
    data = b"a" * 2_500_000
    meta = (await cluster.upload(data)).json()
    worker = meta["chunks"][0]["replicas"][0]["worker_id"]
    cluster.stop(worker)
    assert (await cluster.client.get(f'/api/v1/objects/{meta["object_id"]}')).content == data
    await cluster.store.maintenance(full_scrub=True)
    repaired = cluster.store.metadata(meta["object_id"])
    assert all(sum(r["available"] and r["state"] == "healthy" for r in c["replicas"]) == 2 for c in repaired["chunks"])
    cluster.offline.remove(worker)
    await cluster.register(worker)
    await cluster.store.maintenance(full_scrub=True)
    repaired = cluster.store.metadata(meta["object_id"])
    assert all(len(c["replicas"]) == 2 for c in repaired["chunks"])
    inventories = [app.state.disk.inventory() for app in cluster.workers.values()]
    assert sum(len(items) for items in inventories) == 6


async def test_corruption_fallback_and_repair(cluster):
    data = b"original bytes"
    meta = (await cluster.upload(data)).json()
    chunk = meta["chunks"][0]
    worker = chunk["replicas"][0]["worker_id"]
    disk = cluster.workers[worker].state.disk
    disk.path(chunk["chunk_id"]).write_bytes(b"corrupt")
    assert (await cluster.client.get(f'/api/v1/objects/{meta["object_id"]}')).content == data
    await cluster.store.maintenance(full_scrub=True)
    for replica in cluster.store.metadata(meta["object_id"])["chunks"][0]["replicas"]:
        assert cluster.workers[replica["worker_id"]].state.disk.get(chunk["chunk_id"]) == data


async def test_all_copies_corrupt_returns_503(cluster):
    meta = (await cluster.upload()).json()
    chunk = meta["chunks"][0]
    for replica in chunk["replicas"]:
        cluster.workers[replica["worker_id"]].state.disk.path(chunk["chunk_id"]).write_bytes(b"broken")
    response = await cluster.client.get(f'/api/v1/objects/{meta["object_id"]}')
    assert response.status_code == 503
    assert "No valid" in response.json()["error"]["message"]


async def test_offline_delete_is_durable(cluster):
    meta = (await cluster.upload()).json()
    worker = meta["chunks"][0]["replicas"][0]["worker_id"]
    cluster.stop(worker)
    await cluster.client.delete(f'/api/v1/objects/{meta["object_id"]}')
    await cluster.store.maintenance()
    with cluster.store.sessions() as session:
        assert session.get(StoredObject, meta["object_id"]).state == "deleting"
    cluster.store.recover_startup()
    cluster.offline.remove(worker)
    await cluster.register(worker)
    await cluster.store.maintenance()
    assert all(not app.state.disk.inventory() for app in cluster.workers.values())
    with cluster.store.sessions() as session:
        assert session.get(StoredObject, meta["object_id"]) is None


async def test_upload_rejected_when_only_one_worker(cluster):
    for worker in ("worker2", "worker3", "worker4"):
        cluster.stop(worker)
    assert (await cluster.upload()).status_code == 503
    assert (await cluster.client.get("/api/v1/objects")).json()["objects"] == []


async def test_lost_acknowledgment_cleanup(cluster):
    cluster.lose_put_ack = True
    assert (await cluster.upload()).status_code == 503
    assert any(app.state.disk.inventory() for app in cluster.workers.values())
    assert (await cluster.client.get("/api/v1/objects")).json()["objects"] == []
    await cluster.store.maintenance()
    assert all(not app.state.disk.inventory() for app in cluster.workers.values())


async def test_restart_cleans_interrupted_upload(cluster):
    identity = str(uuid4())
    with cluster.store.sessions.begin() as session:
        session.add(StoredObject(id=identity, name="interrupted", content_type="application/octet-stream"))
    cluster.store.recover_startup()
    await cluster.store.maintenance()
    with cluster.store.sessions() as session:
        assert session.get(StoredObject, identity) is None


@pytest.mark.parametrize("metadata", ['[]', '{', '{"key":123}', '{"": "value"}'])
async def test_invalid_metadata(cluster, metadata):
    assert (await cluster.upload(metadata=metadata)).status_code == 400


async def test_unknown_object_and_invalid_id(cluster):
    assert (await cluster.client.get(f"/api/v1/objects/{uuid4()}")).status_code == 404
    assert (await cluster.client.get("/api/v1/objects/invalid")).status_code == 400


async def test_chunk_retry_conflict_and_restart(tmp_path):
    disk = ChunkDisk(tmp_path)
    identity = str(uuid4())
    assert disk.put(identity, b"first", checksum(b"first"))["size"] == 5
    assert disk.put(identity, b"first", checksum(b"first"))["size"] == 5
    assert ChunkDisk(tmp_path).get(identity) == b"first"
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as error:
        disk.put(identity, b"different", checksum(b"different"))
    assert error.value.status_code == 409
    disk.delete(identity)
    disk.delete(identity)
    assert disk.inventory() == []


async def test_concurrent_metadata_updates_merge(cluster):
    identity = (await cluster.upload()).json()["object_id"]
    responses = await asyncio.gather(*(cluster.client.patch(f"/api/v1/objects/{identity}/metadata", json={str(i): str(i)}) for i in range(8)))
    assert all(r.status_code == 200 for r in responses)
    assert len(cluster.store.metadata(identity)["metadata"]) == 9


async def test_upload_cancel_retains_cleanup_record(cluster):
    class InterruptedFile:
        filename = "partial.bin"
        content_type = "application/octet-stream"
        reads = 0
        async def read(self, size):
            self.reads += 1
            if self.reads == 1:
                return b"first chunk"
            raise asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await cluster.store.upload(InterruptedFile(), {})
    await cluster.store.maintenance()
    assert all(not app.state.disk.inventory() for app in cluster.workers.values())


async def test_manifest_corruption_repaired_with_only_two_workers(cluster):
    meta = (await cluster.upload()).json()
    chunk = meta['chunks'][0]
    holders = {r['worker_id'] for r in chunk['replicas']}
    for worker in set(cluster.workers) - holders:
        cluster.stop(worker)
    damaged = next(iter(holders))
    cluster.workers[damaged].state.disk.path(chunk['chunk_id']).with_suffix('.json').write_text('{broken')
    await cluster.store.maintenance(full_scrub=True)
    assert cluster.workers[damaged].state.disk.get(chunk['chunk_id']) == b'example'


async def test_full_worker_disks_fail_cleanly(cluster, monkeypatch):
    def full_disk(*args):
        raise OSError(28, 'No space left on device')
    for app in cluster.workers.values():
        monkeypatch.setattr(app.state.disk, 'atomic', full_disk)
    response = await cluster.upload()
    assert response.status_code == 503
    assert (await cluster.client.get('/api/v1/objects')).json()['objects'] == []
    await cluster.store.maintenance()
    # A full disk also prevents persisting the late-write deletion marker.
    # Keep durable cleanup work until disk writes become possible again.
    with cluster.store.sessions() as session:
        assert session.scalar(select(StoredObject)).state == 'deleting'
    monkeypatch.undo()
    await cluster.store.maintenance()
    with cluster.store.sessions() as session:
        assert session.scalar(select(func.count()).select_from(StoredObject)) == 0


async def test_worker_registration_rejects_arbitrary_url(cluster):
    response = await cluster.client.post('/internal/workers/register', json={
        **cluster.report('worker1'), 'url': 'http://unexpected-host'})
    assert response.status_code == 400


async def test_delete_waits_for_existing_object_lock(cluster):
    identity = (await cluster.upload()).json()['object_id']
    lock = cluster.store.locks[identity]
    await lock.acquire()
    task = asyncio.create_task(cluster.store.delete_object(identity))
    await asyncio.sleep(0)
    assert not task.done()
    lock.release()
    await task
    assert (await cluster.client.get(f'/api/v1/objects/{identity}')).status_code == 404


async def test_duplicate_filenames_are_independent_and_paginated(cluster):
    first = (await cluster.upload(b'first content')).json()
    second = (await cluster.upload(b'second content')).json()
    assert first['name'] == second['name']
    assert first['object_id'] != second['object_id']
    pages = [await cluster.client.get('/api/v1/objects', params={'offset': i, 'limit': 1}) for i in range(2)]
    assert {p.json()['objects'][0]['object_id'] for p in pages} == {first['object_id'], second['object_id']}
    for meta, expected in ((first, b'first content'), (second, b'second content')):
        assert (await cluster.client.get(f"/api/v1/objects/{meta['object_id']}")).content == expected


async def test_metadata_patch_preserves_chunk_layout_and_creation_time(cluster):
    original = (await cluster.upload(b'a' * 2_500_000)).json()
    response = await cluster.client.patch(f"/api/v1/objects/{original['object_id']}/metadata", json={'tag': 'new'})
    assert response.status_code == 200
    updated = response.json()
    for key in ('created_at', 'size', 'checksum', 'chunk_count', 'chunks'):
        assert updated[key] == original[key]
    assert updated['metadata'] == {'owner': 'student', 'tag': 'new'}


async def test_download_preserves_content_type_and_unicode_filename(cluster):
    response = await cluster.client.post('/api/v1/objects', files={
        'file': ('résumé.pdf', b'%PDF-arbitrary-test-bytes', 'application/pdf')})
    assert response.status_code == 201
    download = await cluster.client.get(f"/api/v1/objects/{response.json()['object_id']}")
    assert download.headers['content-type'] == 'application/pdf'
    assert download.headers['content-disposition'] == "attachment; filename*=UTF-8''r%C3%A9sum%C3%A9.pdf"
    assert int(download.headers['content-length']) == len(download.content)
