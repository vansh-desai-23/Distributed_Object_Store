"""Run real controller/worker ASGI apps with controllable transport failures."""
from contextlib import AsyncExitStack
from datetime import timedelta

import httpx
import pytest_asyncio

from object_store.controller import create_app
from object_store.models import Base, Worker, now
from object_store.worker import create_app as worker_app


class Cluster:
    def __init__(self, tmp_path):
        self.workers = {f"worker{i}": worker_app(tmp_path / f"worker{i}", f"worker{i}", heartbeat=False)
                        for i in range(1, 5)}
        self.offline = set()
        self.lose_put_ack = False
        self.app = create_app("sqlite:///:memory:", transport=httpx.MockTransport(self.route), background=False)
        Base.metadata.create_all(self.app.state.engine)
        self.store = self.app.state.store
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://controller")

    async def route(self, request):
        worker = request.url.host
        if worker in self.offline:
            raise httpx.ConnectError("Worker stopped", request=request)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.workers[worker]), base_url=f"http://{worker}") as client:
            response = await client.request(request.method, request.url.path, headers=request.headers,
                                            content=await request.aread())
        if self.lose_put_ack and request.method == "PUT":
            raise httpx.ReadError("Lost acknowledgment", request=request)
        return response

    def report(self, worker):
        return {"worker_id": worker, "url": f"http://{worker}:8000", "free_bytes": 10**10,
                **self.workers[worker].state.membership}

    async def register(self, worker):
        response = await self.client.post("/internal/workers/register", json=self.report(worker))
        assert response.status_code == 200

    def stop(self, worker):
        self.offline.add(worker)
        with self.store.sessions.begin() as session:
            session.get(Worker, worker).last_seen = now() - timedelta(seconds=30)

    async def upload(self, data=b"example", metadata='{"owner":"student"}'):
        return await self.client.post("/api/v1/objects", files={"file": ("test.bin", data, "application/octet-stream")},
                                      data={"metadata": metadata})


@pytest_asyncio.fixture
async def cluster(tmp_path):
    cluster = Cluster(tmp_path)
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(cluster.app.router.lifespan_context(cluster.app))
        await stack.enter_async_context(cluster.client)
        for worker in cluster.workers:
            await cluster.register(worker)
        await cluster.store.registry.reconcile()
        yield cluster
