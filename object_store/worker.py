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

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, Response
from starlette.concurrency import run_in_threadpool

from object_store.common import CHUNK_SIZE, canonical_id, checksum, install_errors

log = logging.getLogger(__name__)


class ChunkDisk:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        # Interrupted atomic writes are never advertised as committed chunks.
        for path in self.root.glob("*.tmp"):
            path.unlink(missing_ok=True)
        # Finish a deletion interrupted after its durable marker was written.
        for path in self.root.glob("*.deleted"):
            self.delete(path.stem)

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

    def put(self, chunk_id, data, expected):
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
        self.atomic(path, data)
        self.atomic(manifest, json.dumps({"checksum": expected, "size": len(data)}).encode())
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
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

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
    lock = asyncio.Lock()

    async def report():
        controller = os.environ.get("CONTROLLER_URL", "http://controller:8000")
        url = os.environ.get("WORKER_URL", f"http://{identity}:8000")
        registered = False
        async with httpx.AsyncClient(timeout=5) as client:
            while True:
                try:
                    body = {"worker_id": identity, "url": url, "free_bytes": shutil.disk_usage(disk.root).free}
                    endpoint = "heartbeat" if registered else "register"
                    response = await client.post(f"{controller}/internal/workers/{endpoint}", json=body)
                    if response.status_code == 404:
                        response = await client.post(f"{controller}/internal/workers/register", json=body)
                    response.raise_for_status()
                    registered = True
                except (httpx.HTTPError, OSError):
                    log.warning("Controller heartbeat failed for %s", identity)
                await asyncio.sleep(float(os.environ.get("HEARTBEAT_INTERVAL", "2")))

    @asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(report()) if heartbeat else None
        yield
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    app = FastAPI(title=f"Object Store Worker: {identity}", lifespan=lifespan)
    app.state.disk = disk
    install_errors(app)

    @app.get("/health")
    async def health():
        return {"worker_id": identity, "status": "ok"}

    @app.put("/internal/chunks/{chunk_id}", openapi_extra={
        "requestBody": {"required": True, "content": {"application/octet-stream": {
            "schema": {"type": "string", "format": "binary"}}}}})
    async def put(chunk_id: str, request: Request, x_chunk_checksum: str = Header()):
        chunk_id = canonical_id(chunk_id)
        data = bytearray()
        async for part in request.stream():
            data.extend(part)
            if len(data) > CHUNK_SIZE:
                raise HTTPException(400, "Chunk exceeds 1 MB")
        async with lock:
            return await run_in_threadpool(disk.put, chunk_id, bytes(data), x_chunk_checksum)

    @app.get("/internal/chunks/{chunk_id}")
    async def get(chunk_id: str):
        async with lock:
            data = await run_in_threadpool(disk.get, canonical_id(chunk_id))
        return Response(data, media_type="application/octet-stream", headers={"X-Chunk-Checksum": checksum(data)})

    @app.delete("/internal/chunks/{chunk_id}", status_code=204)
    async def delete(chunk_id: str, x_delete_permanent: bool = Header(default=False)):
        async with lock:
            await run_in_threadpool(disk.delete, canonical_id(chunk_id), x_delete_permanent)
        return Response(status_code=204)

    @app.get("/internal/workers/{worker_id}/chunks")
    async def inventory(worker_id: str):
        if worker_id != identity:
            raise HTTPException(404, "Unknown worker")
        async with lock:
            return {"worker_id": identity, "chunks": await run_in_threadpool(disk.inventory)}

    return app
