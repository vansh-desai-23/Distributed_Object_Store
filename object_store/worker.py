"""Persistent, checksum-verified chunk service and controller heartbeat client."""
import asyncio
from contextlib import asynccontextmanager, suppress
import json
import logging
import os
from pathlib import Path
import re
import shutil
import tempfile
from uuid import uuid4, UUID
import time
import sqlite3

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, Response
from starlette.concurrency import run_in_threadpool

from object_store.catalog import Catalog
from object_store.common import CHUNK_SIZE, canonical_id, checksum, install_errors

log = logging.getLogger(__name__)


class ChunkDisk:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.catalog = Catalog(self.root)
        # Interrupted atomic writes are never advertised as committed chunks.
        for path in self.root.glob("*.tmp"):
            path.unlink(missing_ok=True)
        # Finish a deletion interrupted after its durable marker was written.
        for path in self.root.glob("*.deleted"):
            self.delete(path.stem)
        self.recover_operations()

    def storage_identity(self, worker_id):
        # The identity file deliberately has no .json suffix: it is not a chunk manifest.
        path = self.root / "worker.identity"
        if path.exists():
            value = json.loads(path.read_text())
            if value["worker_id"] != worker_id:
                raise ValueError("Storage volume belongs to a different worker")
            return str(UUID(value["storage_id"]))
        value = {"worker_id": worker_id, "storage_id": str(uuid4())}
        self.atomic(path, json.dumps(value).encode())
        return value["storage_id"]

    def path(self, chunk_id):
        return self.root / f"{canonical_id(chunk_id)}.chunk"

    def load_manifest(self, path):
        """Validate local metadata without trusting its types, size, or allocation demands."""
        with path.open("rb") as stream:
            raw = stream.read(4097)
        if len(raw) > 4096:
            raise ValueError("Manifest exceeds 4096 bytes")
        info = json.loads(raw)
        if not isinstance(info, dict) or type(info.get("size")) is not int:
            raise ValueError("Invalid manifest size type")
        if not 1 <= info["size"] <= CHUNK_SIZE:
            raise ValueError("Invalid manifest chunk size")
        if not isinstance(info.get("checksum"), str) or not re.fullmatch(r"[0-9a-f]{64}", info["checksum"]):
            raise ValueError("Invalid manifest checksum")
        return info

    def put(self, chunk_id, data, expected, sequence=0):
        if not data or len(data) > CHUNK_SIZE or checksum(data) != expected:
            raise HTTPException(400, "Invalid chunk size or SHA-256 checksum")
        path = self.path(chunk_id)
        if path.with_suffix(".deleted").exists():
            raise HTTPException(409, "Chunk was permanently deleted")
        manifest = path.with_suffix(".json")
        if manifest.exists():
            try:
                info = self.load_manifest(manifest)
                stored_checksum = info["checksum"]
            except (ValueError, KeyError, TypeError):
                raise HTTPException(409, "Chunk manifest is corrupt; delete before repair") from None
            if stored_checksum != expected:
                raise HTTPException(409, "Chunk ID already belongs to different content")
        self.catalog.expect(chunk_id, expected, len(data), sequence)
        self.atomic(path, data)
        self.atomic(manifest, json.dumps({"checksum": expected, "size": len(data)}).encode())
        self.catalog.state(chunk_id, "committed")
        return {"chunk_id": chunk_id, "checksum": expected, "size": len(data)}

    def atomic(self, path, data):
        fd, name = tempfile.mkstemp(suffix=".tmp", dir=self.root)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, path)
            directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            Path(name).unlink(missing_ok=True)

    def get(self, chunk_id):
        path = self.path(chunk_id)
        if path.with_suffix(".deleted").exists() or not path.exists():
            raise HTTPException(404, "Chunk not found")
        try:
            info = self.load_manifest(path.with_suffix(".json"))
            with path.open("rb") as stream:
                data = stream.read(CHUNK_SIZE + 1)
            trusted = self.catalog.expected(chunk_id)
            if trusted and (info['checksum'] != trusted['checksum'] or info['size'] != trusted['size']):
                raise ValueError("Manifest differs from trusted catalog")
            if len(data) != info["size"] or checksum(data) != info["checksum"]:
                raise ValueError("Checksum mismatch")
        except (ValueError, KeyError, TypeError, FileNotFoundError):
            raise HTTPException(409, "Chunk is corrupt or incomplete") from None
        return data

    def delete(self, chunk_id, permanent=False):
        path = self.path(chunk_id)
        if permanent:
            # Write before removing bytes: late/in-flight PUTs must remain rejected
            # even after a worker restart or a lost DELETE acknowledgment.
            self.atomic(path.with_suffix(".deleted"), b"")
        path.unlink(missing_ok=True)
        path.with_suffix(".json").unlink(missing_ok=True)
        self.catalog.state(chunk_id, "deleted" if permanent or path.with_suffix(".deleted").exists() else "removed")
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def mutate(self, method, chunk_id, sequence, data=b'', expected='', permanent=False):
        # Controller sequences are global and durable; unsequenced diagnostics cannot
        # overwrite a location after it has participated in the ordered protocol.
        sequence = sequence or 0
        signature = json.dumps([method, expected, len(data), permanent])
        if method == 'PUT' and (not data or len(data) > CHUNK_SIZE or checksum(data) != expected):
            raise HTTPException(400, 'Invalid chunk size or SHA-256 checksum')
        receipt = self.catalog.begin(chunk_id, sequence, signature)
        if receipt is not None:
            return receipt
        if method == 'PUT':
            receipt = self.put(chunk_id, data, expected, sequence)
        else:
            self.delete(chunk_id, permanent)
            receipt = {'chunk_id': chunk_id}
        receipt['sequence'] = sequence
        self.catalog.finish(chunk_id, receipt)
        return receipt

    def recover_operations(self):
        with self.catalog.connect() as db:
            rows = [dict(row) for row in db.execute("SELECT * FROM operations WHERE state='pending'")]
        for row in rows:
            method, expected, size, permanent = json.loads(row['signature'])
            identity = row['chunk_id']
            if method == 'DELETE':
                self.delete(identity, permanent)
                self.catalog.finish(identity, {'chunk_id': identity, 'sequence': row['sequence']})
                continue
            path = self.path(identity)
            if path.with_suffix('.deleted').exists():
                continue
            data = b''
            if path.exists():
                with path.open('rb') as stream:
                    data = stream.read(CHUNK_SIZE + 1)
            if len(data) == size and checksum(data) == expected:
                self.atomic(path.with_suffix('.json'), json.dumps({'checksum': expected, 'size': size}).encode())
                receipt = self.put(identity, data, expected, row['sequence'])
                self.catalog.finish(identity, {**receipt, 'sequence': row['sequence']})
            else:
                self.catalog.damage(identity, row['sequence'], 'interrupted_publication')

    def scan_batch(self, limit=16):
        for identity in self.catalog.scan_ids(limit):
            expected = self.catalog.expected(identity)
            try:
                self.get(identity)
            except (HTTPException, OSError):
                self.catalog.damage(identity, expected['sequence'], 'missing_or_corrupt')
            self.catalog.scanned(identity)
        self.catalog.complete_cycle()
        return self.catalog.report()['scan_progress']

    def reconcile_expected(self, items):
        # Only the controller supplies expectations for legacy or discovered copies.
        for item in items:
            identity = canonical_id(item['chunk_id'])
            operation = self.catalog.operation(identity)
            if operation and operation['sequence'] > item['sequence']:
                raise HTTPException(409, 'Reconciliation expectation predates a mutation')
            self.catalog.expect(identity, item['checksum'], item['size'], item['sequence'], 'committed')

    def inventory(self):
        result = []
        ids = {p.stem for pattern in ("*.chunk", "*.json") for p in self.root.glob(pattern)}
        for chunk_id in sorted(ids):
            try:
                data = self.get(chunk_id)
                result.append({"chunk_id": chunk_id, "size": len(data), "checksum": checksum(data), "valid": True})
            except HTTPException:
                result.append({"chunk_id": chunk_id, "valid": False})
        return result


def create_app(root=None, worker_id=None, heartbeat=True):
    disk = ChunkDisk(root or os.environ.get("STORAGE_DIR", "/data"))
    identity = worker_id or os.environ.get("WORKER_ID", "worker1")
    membership = {"storage_id": disk.storage_identity(identity), "boot_id": str(uuid4())}
    lock = asyncio.Lock()
    last_ready = 0.0

    async def report():
        nonlocal last_ready
        controller = os.environ.get("CONTROLLER_URL", "http://controller:8000")
        url = os.environ.get("WORKER_URL", f"http://{identity}:8000")
        registered = False
        async with httpx.AsyncClient(timeout=5) as client:
            while True:
                try:
                    async with lock:
                        details = await run_in_threadpool(disk.catalog.report)
                    body = {**details, "worker_id": identity, "url": url, "free_bytes": shutil.disk_usage(disk.root).free, **membership}
                    endpoint = "heartbeat" if registered else "register"
                    response = await client.post(f"{controller}/internal/workers/{endpoint}", json=body)
                    if response.status_code == 404:
                        response = await client.post(f"{controller}/internal/workers/register", json=body)
                    response.raise_for_status()
                    async with lock:
                        await run_in_threadpool(disk.catalog.acknowledge, response.json().get('acknowledged_damage', []))
                    registered = True
                    last_ready = time.monotonic() if response.json().get("eligible") else 0.0
                except (httpx.HTTPError, OSError, sqlite3.Error):
                    last_ready = 0.0
                    log.warning("Controller heartbeat failed for %s", identity)
                await asyncio.sleep(float(os.environ.get("HEARTBEAT_INTERVAL", "2")))

    async def scan():
        while True:
            try:
                async with lock:
                    await run_in_threadpool(disk.scan_batch)
            except Exception:
                log.exception('Local integrity scan deferred')
            await asyncio.sleep(float(os.environ.get('SCAN_INTERVAL', '3')))

    @asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(report()) if heartbeat else None
        scanner = asyncio.create_task(scan()) if heartbeat else None
        yield
        if scanner:
            scanner.cancel()
            with suppress(asyncio.CancelledError):
                await scanner
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    app = FastAPI(title=f"Object Store Worker: {identity}", lifespan=lifespan)
    app.state.disk = disk
    app.state.membership = membership
    install_errors(app)

    @app.middleware("http")
    async def check_target_boot(request, call_next):
        if request.url.path.startswith("/internal/chunks/"):
            for header, field in (("x-worker-storage", "storage_id"), ("x-worker-boot", "boot_id")):
                if header in request.headers and request.headers[header] != membership[field]:
                    return Response(status_code=409, content='{"error":{"message":"Worker incarnation changed"}}',
                                    media_type="application/json")
        return await call_next(request)

    @app.get("/health")
    async def health():
        return {"worker_id": identity, "status": "ok", **membership}

    @app.get("/ready")
    async def ready():
        if not last_ready or time.monotonic() - last_ready >= 10:
            raise HTTPException(503, "Worker has not completed registration and reconciliation")
        return {"worker_id": identity, "status": "ready", **membership}

    @app.put("/internal/chunks/{chunk_id}", openapi_extra={
        "requestBody": {"required": True, "content": {"application/octet-stream": {
            "schema": {"type": "string", "format": "binary"}}}}})
    async def put(chunk_id: str, request: Request, x_chunk_checksum: str = Header(), x_operation_sequence: int = Header(default=0, ge=0, le=9223372036854775807)):
        chunk_id = canonical_id(chunk_id)
        data = bytearray()
        async for part in request.stream():
            data.extend(part)
            if len(data) > CHUNK_SIZE:
                raise HTTPException(400, "Chunk exceeds 1 MB")
        async with lock:
            return await run_in_threadpool(disk.mutate, "PUT", chunk_id, x_operation_sequence, bytes(data), x_chunk_checksum)

    @app.get("/internal/chunks/{chunk_id}")
    async def get(chunk_id: str):
        async with lock:
            data = await run_in_threadpool(disk.get, canonical_id(chunk_id))
        return Response(data, media_type="application/octet-stream", headers={"X-Chunk-Checksum": checksum(data)})

    @app.delete("/internal/chunks/{chunk_id}", status_code=204)
    async def delete(chunk_id: str, x_delete_permanent: bool = Header(default=False), x_operation_sequence: int = Header(default=0, ge=0, le=9223372036854775807)):
        async with lock:
            await run_in_threadpool(disk.mutate, "DELETE", canonical_id(chunk_id), x_operation_sequence, b"", "", x_delete_permanent)
        return Response(status_code=204)

    @app.post('/internal/chunks/reconcile')
    async def reconcile(request: Request):
        body = await request.json()
        items = body.get('chunks')
        if not isinstance(items, list) or len(items) > 100:
            raise HTTPException(400, 'Expected at most 100 chunk expectations')
        for item in items:
            if (not isinstance(item, dict) or not isinstance(item.get('checksum'), str)
                    or not re.fullmatch(r'[0-9a-f]{64}', item['checksum'])
                    or type(item.get('size')) is not int or not 1 <= item['size'] <= CHUNK_SIZE
                    or type(item.get('sequence')) is not int or item['sequence'] < 0):
                raise HTTPException(400, 'Invalid trusted expectation')
        async with lock:
            await run_in_threadpool(disk.reconcile_expected, items)
        return {'accepted': len(items)}

    @app.get('/internal/scan')
    async def scan_status():
        async with lock:
            return await run_in_threadpool(disk.catalog.report)

    @app.get("/internal/workers/{worker_id}/chunks")
    async def inventory(worker_id: str):
        if worker_id != identity:
            raise HTTPException(404, "Unknown worker")
        async with lock:
            return {"worker_id": identity, **membership, "chunks": await run_in_threadpool(disk.inventory)}

    return app
