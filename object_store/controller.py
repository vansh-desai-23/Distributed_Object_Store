"""Client REST API, worker membership, and supervised maintenance loop."""
import asyncio
from contextlib import asynccontextmanager, suppress
import json
import logging
import os
import shutil
import tempfile
from urllib.parse import quote

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import select, text, update
from starlette.datastructures import UploadFile

from object_store.common import canonical_id, install_errors, validate_metadata
from object_store.database import make_database
from object_store.models import Chunk, Replica, StoredObject, Worker, now
from object_store.service import Store

log = logging.getLogger(__name__)


class UploadBudget:
    """Limit uploads before multipart parsing, including temporary-disk spooling."""
    def __init__(self, app):
        self.app = app
        self.slots = asyncio.Semaphore(2)

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] != "POST" or scope["path"] != "/api/v1/objects":
            return await self.app(scope, receive, send)
        async with self.slots:
            async def checked_receive():
                if shutil.disk_usage(tempfile.gettempdir()).free < 50_000_000:
                    raise HTTPException(503, "Insufficient upload spool space")
                return await receive()
            await self.app(scope, checked_receive, send)


class WorkerReport(BaseModel):
    worker_id: str = Field(min_length=1, max_length=64)
    url: str = Field(max_length=255)
    free_bytes: int = Field(ge=0)


def create_app(database_url=None, transport=None, background=True, allowed_workers=None):
    engine, sessions = make_database(database_url)
    client = httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(5, connect=2),
                              limits=httpx.Limits(max_connections=16), trust_env=False)
    store = Store(sessions, client, float(os.environ.get("WORKER_TIMEOUT", "10")))
    allowed = allowed_workers or {f"worker{i}": f"http://worker{i}:8000" for i in range(1, 5)}

    async def maintain():
        while True:
            try:
                await store.maintenance()
            except Exception:
                log.exception("Maintenance pass failed; durable records will be retried")
            await asyncio.sleep(float(os.environ.get("REPAIR_INTERVAL", "3")))

    @asynccontextmanager
    async def lifespan(app):
        store.recover_startup()
        task = asyncio.create_task(maintain()) if background else None
        yield
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        await client.aclose()
        engine.dispose()

    app = FastAPI(title="Mini Distributed Cloud Object Store", version="1.0.0", lifespan=lifespan)
    app.add_middleware(UploadBudget)
    app.state.store, app.state.engine = store, engine
    install_errors(app)

    @app.get("/health")
    async def health():
        with sessions() as session:
            session.execute(text("SELECT 1"))
        return {"status": "ok"}

    @app.post("/internal/workers/register")
    @app.post("/internal/workers/heartbeat")
    async def register(report: WorkerReport, request: Request):
        if allowed.get(report.worker_id) != report.url:
            raise HTTPException(400, "Worker identity and URL must match configured membership")
        with sessions.begin() as session:
            worker = session.get(Worker, report.worker_id)
            if worker is None:
                if request.url.path.endswith("heartbeat"):
                    raise HTTPException(404, "Worker must register first")
                worker = Worker(id=report.worker_id, url=report.url)
                session.add(worker)
            elif request.url.path.endswith("register") or not store.live(worker):
                # Heartbeats prove process reachability, not that old disk copies survived.
                session.execute(update(Replica).where(Replica.worker_id == worker.id,
                    Replica.state == "healthy").values(state="pending"))
            worker.last_seen = now()
            worker.free_bytes = report.free_bytes
        return {"worker_id": report.worker_id, "status": "registered"}

    @app.get("/api/v1/status")
    async def status():
        with sessions() as session:
            workers = [{"worker_id": w.id, "available": store.live(w), "last_seen": w.last_seen.isoformat(),
                        "free_bytes": w.free_bytes} for w in session.scalars(select(Worker).order_by(Worker.id))]
            pending = len(session.scalars(select(StoredObject.id).where(StoredObject.state == "deleting")).all())
        return {"workers": workers, "pending_cleanup_objects": pending, "replication_factor": 2}

    @app.post("/api/v1/objects", status_code=201, openapi_extra={
        "requestBody": {"required": True, "content": {"multipart/form-data": {"schema": {
            "type": "object", "required": ["file"], "properties": {
                "file": {"type": "string", "format": "binary"},
                "metadata": {"type": "string", "default": "{}",
                             "description": "JSON object containing custom string key-value pairs"}
            }}}}}})
    async def upload(request: Request):
        if not request.headers.get("content-type", "").startswith("multipart/form-data"):
            raise HTTPException(400, "Upload requires multipart/form-data")
        async with request.form(max_files=1, max_fields=1, max_part_size=250_000) as form:
            file = form.get("file")
            if not isinstance(file, UploadFile):
                raise HTTPException(400, "A file field is required")
            if any(key not in ("file", "metadata") for key in form):
                raise HTTPException(400, "Unexpected multipart field")
            try:
                metadata = validate_metadata(json.loads(form.get("metadata", "{}")))
            except (ValueError, TypeError):
                raise HTTPException(400, "Metadata must be a JSON object of string pairs") from None
            result = await store.upload(file, metadata)
        return result

    @app.get("/api/v1/objects")
    async def list_objects(offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=1000)):
        with sessions() as session:
            objects = session.scalars(select(StoredObject).where(StoredObject.state == "available")
                                      .order_by(StoredObject.created_at, StoredObject.id).offset(offset).limit(limit)).all()
            return {"objects": [store.summary(obj) for obj in objects], "offset": offset, "limit": limit}

    @app.get("/api/v1/objects/{object_id}/metadata")
    async def metadata(object_id: str):
        return store.metadata(canonical_id(object_id))

    @app.patch("/api/v1/objects/{object_id}/metadata", openapi_extra={
        "requestBody": {"required": True, "content": {"application/json": {"schema": {
            "type": "object", "maxProperties": 100,
            "propertyNames": {"minLength": 1, "maxLength": 128},
            "additionalProperties": {"type": "string", "maxLength": 2048},
            "example": {"course": "cloud"}
        }}}}})
    async def patch_metadata(object_id: str, request: Request):
        identity = canonical_id(object_id)
        try:
            updates = validate_metadata(await request.json())
        except ValueError:
            raise HTTPException(400, "Invalid JSON") from None
        async with store.locks[identity]:
            with sessions.begin() as session:
                obj = store.available(session, identity)
                obj.custom_metadata = validate_metadata({**obj.custom_metadata, **updates})
                obj.modified_at = now()
            return store.metadata(identity)

    @app.delete("/api/v1/objects/{object_id}", status_code=204)
    async def delete_object(object_id: str):
        await store.delete_object(canonical_id(object_id))
        return Response(status_code=204)

    @app.get("/api/v1/objects/{object_id}", response_class=Response, responses={
        200: {"description": "Original binary object; Content-Type follows stored metadata",
              "content": {"application/octet-stream": {"schema": {"type": "string", "format": "binary"}}}}})
    async def download(object_id: str):
        identity = canonical_id(object_id)
        await store.download_slots.acquire()
        lock = store.locks[identity]
        try:
            await lock.acquire()
        except BaseException:
            store.download_slots.release()
            raise
        try:
            with sessions() as session:
                obj = store.available(session, identity)
                chunks = session.scalars(select(Chunk).where(Chunk.object_id == identity).order_by(Chunk.position)).all()
            first = await store.fetch(chunks[0]) if chunks else b""
        except BaseException:
            lock.release()
            store.download_slots.release()
            raise

        async def stream():
            yield first
            for chunk in chunks[1:]:
                yield await store.fetch(chunk)

        class LockedStream(StreamingResponse):
            async def __call__(self, scope, receive, send):
                try:
                    await super().__call__(scope, receive, send)
                finally:
                    lock.release()
                    store.download_slots.release()

        return LockedStream(stream(), media_type=obj.content_type, headers={
            "Content-Length": str(obj.size), "X-Object-Checksum": obj.checksum,
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(obj.name, safe='')}"})

    return app
