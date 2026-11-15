"""Phase 3 acceptance: normal storage APIs and actual replica bytes, without fault injection."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import time

import httpx

ROOT = Path(__file__).resolve().parents[1]
CHUNK_SIZE = 1_000_000


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def inventories():
    """Read worker inventory APIs through the controller's Compose network."""
    code = '''
import json, urllib.request
result = {}
for i in range(1, 5):
    worker = f"worker{i}"
    with urllib.request.urlopen(f"http://{worker}:8000/internal/workers/{worker}/chunks", timeout=30) as response:
        result[worker] = json.load(response)["chunks"]
print(json.dumps(result))
'''
    result = subprocess.run(["docker", "compose", "exec", "-T", "controller", "python", "-c", code],
                            cwd=ROOT, text=True, capture_output=True, check=True, timeout=150)
    return json.loads(result.stdout)


def listed_objects(client):
    offset = 0
    while True:
        response = client.get("/api/v1/objects", params={"offset": offset, "limit": 100})
        response.raise_for_status()
        page = response.json()["objects"]
        yield from page
        if len(page) < 100:
            break
        offset += len(page)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size-mb", type=int, default=100, choices=range(1, 501), metavar="1..500")
    args = parser.parse_args()
    created = []
    chunk_ids = set()
    expected_replicas = {}
    # Fixed local URL: worker inventories must belong to the same Compose deployment.
    with httpx.Client(base_url="http://127.0.0.1:8000", timeout=300, trust_env=False) as client, tempfile.TemporaryDirectory(prefix="phase3-") as directory:
        status = client.get("/api/v1/status")
        status.raise_for_status()
        require({w["worker_id"] for w in status.json()["workers"] if w["available"]} ==
                {f"worker{i}" for i in range(1, 5)}, "All four local workers must be available")
        try:
            for size in (0, 1, CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1, 2 * CHUNK_SIZE, 2 * CHUNK_SIZE + 1, 2_500_000, args.size_mb * CHUNK_SIZE):
                path = Path(directory) / "phase3.bin"
                expected_hashes = []
                digest = hashlib.sha256()
                remaining = size
                with path.open("wb") as stream:
                    index = 0
                    while remaining:
                        block = (hashlib.sha256(str(index).encode()).digest() * 31250)[:min(CHUNK_SIZE, remaining)]
                        stream.write(block)
                        digest.update(block)
                        expected_hashes.append(hashlib.sha256(block).hexdigest())
                        remaining -= len(block)
                        index += 1
                with path.open("rb") as stream:
                    response = client.post("/api/v1/objects", files={"file": (path.name, stream, "application/octet-stream")},
                                           data={"metadata": json.dumps({"phase": "3", "size": str(size)})})
                response.raise_for_status()
                meta = response.json()
                identity = meta["object_id"]
                created.append(identity)
                require(response.status_code == 201, "Upload must return 201")
                require(len(set(created)) == len(created), "Duplicate filenames must receive unique IDs")
                require(meta["size"] == size and meta["checksum"] == digest.hexdigest(), "Object size/hash mismatch")
                chunks = meta["chunks"]
                require(meta["chunk_count"] == len(chunks) == len(expected_hashes), "Chunk count mismatch")
                used_workers = set()
                for index, chunk in enumerate(chunks):
                    chunk_ids.add(chunk["chunk_id"])
                    expected_size = min(CHUNK_SIZE, size - index * CHUNK_SIZE)
                    require(chunk["position"] == index and chunk["size"] == expected_size and
                            chunk["checksum"] == expected_hashes[index], "Chunk order/size/hash mismatch")
                    workers = {r["worker_id"] for r in chunk["replicas"]}
                    require(len(chunk["replicas"]) == len(workers) == 2 and
                            all(r["state"] == "healthy" and r["available"] for r in chunk["replicas"]),
                            "Each chunk must have two available distinct replicas")
                    used_workers.update(workers)
                    expected_replicas[chunk["chunk_id"]] = (workers, expected_size, expected_hashes[index])
                if size == args.size_mb * CHUNK_SIZE and args.size_mb >= 4:
                    require(len(used_workers) == 4, "Main test object must be distributed across all workers")
                fetched = client.get(f"/api/v1/objects/{identity}/metadata")
                fetched.raise_for_status()
                require(fetched.json() == meta, "Metadata GET disagrees with upload response")
                response = client.patch(f"/api/v1/objects/{identity}/metadata", json={"phase": "3-verified", "tag": "patched"})
                response.raise_for_status()
                updated = response.json()
                require(updated["metadata"] == {"phase": "3-verified", "size": str(size), "tag": "patched"}, "PATCH did not merge custom metadata")
                require(all(updated[key] == meta[key] for key in ("checksum", "chunks", "size", "created_at")),
                        "PATCH changed stored object data/layout")
                received, actual = 0, hashlib.sha256()
                with client.stream("GET", f"/api/v1/objects/{identity}") as response:
                    response.raise_for_status()
                    require(response.headers["content-type"] == "application/octet-stream" and
                            int(response.headers["content-length"]) == size and
                            response.headers["x-object-checksum"] == digest.hexdigest(), "Download headers mismatch")
                    for block in response.iter_bytes(CHUNK_SIZE):
                        received += len(block)
                        actual.update(block)
                require(received == size and actual.hexdigest() == digest.hexdigest(), "Download bytes mismatch")
                print(f"PASS: {size:,} bytes; upload, chunks, replicas, metadata GET/PATCH and download", flush=True)
            listed = {obj["object_id"]: obj for obj in listed_objects(client)}
            require(set(created) <= listed.keys(), "List API omitted created objects")
            require(all({"object_id", "name", "size", "content_type"} <= listed[i].keys() for i in created), "List fields missing")
            print("PASS: list API includes each distinct object with required fields", flush=True)
            found = {}
            for worker, items in inventories().items():
                for item in items:
                    identity = item["chunk_id"]
                    if identity in expected_replicas:
                        _, size, expected_hash = expected_replicas[identity]
                        require(item["valid"] and item["size"] == size and item["checksum"] == expected_hash,
                                "Worker inventory found invalid replica bytes")
                        found.setdefault(identity, set()).add(worker)
            require(all(found.get(identity) == expected[0] for identity, expected in expected_replicas.items()),
                    "Physical replica locations differ from controller metadata")
            print("PASS: worker inventories independently confirm every replica's bytes and location", flush=True)
        finally:
            for identity in created:
                response = client.delete(f"/api/v1/objects/{identity}")
                require(response.status_code in (204, 404), f"Cleanup failed for {identity}: {response.status_code}")
            deadline = time.monotonic() + 180
            while created:
                remaining = {item["chunk_id"] for items in inventories().values() for item in items} & chunk_ids
                if not remaining:
                    require(not (set(created) & {obj["object_id"] for obj in listed_objects(client)}), "Deleted test objects remain listed")
                    print("PASS: only this run's objects deleted; test chunks removed from all workers", flush=True)
                    break
                require(time.monotonic() < deadline, "Timed out cleaning test chunks")
                time.sleep(2)
    print("Phase 3 storage acceptance passed.")


if __name__ == "__main__":
    main()
