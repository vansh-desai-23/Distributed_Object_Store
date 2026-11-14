"""Reproducible Docker demonstration; only mutates the object created by this run."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import time

import httpx


def docker(*args):
    subprocess.run(["docker", *args], check=True)


def wait_for(predicate, description, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            complete = predicate()
        except httpx.HTTPError:
            complete = False
        if complete:
            print(f"PASS: {description}", flush=True)
            return
        time.sleep(2)
    raise RuntimeError(f"Timed out: {description}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size-mb", type=int, default=100)
    parser.add_argument("--faults", action="store_true", help="Stop/restart a worker and corrupt a demo chunk")
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    if args.size_mb < 1:
        parser.error("--size-mb must be positive")
    identity = None
    stopped = None
    with tempfile.TemporaryDirectory(prefix="object-store-demo-") as directory, httpx.Client(base_url=args.url, timeout=300, trust_env=False) as client:
        def metadata():
            response = client.get(f"/api/v1/objects/{identity}/metadata")
            response.raise_for_status()
            return response.json()

        def replicated():
            return all(sum(r["available"] and r["state"] == "healthy" for r in c["replicas"]) >= 2 for c in metadata()["chunks"])

        def verify_download(expected, size):
            digest = hashlib.sha256()
            received = 0
            with client.stream("GET", f"/api/v1/objects/{identity}") as response:
                response.raise_for_status()
                for part in response.iter_bytes(1_000_000):
                    received += len(part)
                    digest.update(part)
            assert received == size and digest.hexdigest() == expected
            print(f"PASS: downloaded {received:,} bytes with matching SHA-256", flush=True)

        try:
            wait_for(lambda: len([w for w in client.get('/api/v1/status').json()['workers'] if w['available']]) == 4, "four workers registered")
            path = Path(directory) / "demo.bin"
            digest = hashlib.sha256()
            with path.open("wb") as stream:
                for i in range(args.size_mb):
                    block = (hashlib.sha256(str(i).encode()).digest() * 31250)
                    stream.write(block)
                    digest.update(block)
            with path.open("rb") as stream:
                response = client.post("/api/v1/objects", files={"file": (path.name, stream, "application/octet-stream")},
                                       data={"metadata": json.dumps({"purpose": "submission-demo"})})
            response.raise_for_status()
            result = response.json()
            identity = result["object_id"]
            print(f"Created {identity}: {result['chunk_count']} chunks", flush=True)
            assert result["checksum"] == digest.hexdigest()
            assert result["chunk_count"] == args.size_mb
            assert all(len(c['replicas']) == 2 for c in result['chunks'])
            workers = {r['worker_id'] for c in result['chunks'] for r in c['replicas']}
            assert len(workers) == 4 if args.size_mb > 1 else len(workers) == 2
            print("PASS: chunk distribution and two distinct replicas", sorted(workers), flush=True)
            verify_download(digest.hexdigest(), path.stat().st_size)
            response = client.patch(f"/api/v1/objects/{identity}/metadata", json={"purpose": "updated-without-upload"})
            response.raise_for_status()
            assert response.json()['metadata']['purpose'] == 'updated-without-upload'
            assert response.json()['checksum'] == digest.hexdigest()
            print("PASS: metadata update without re-upload", flush=True)
            if args.faults:
                stopped = "worker2"
                docker("stop", stopped)
                wait_for(lambda: not next(w['available'] for w in client.get('/api/v1/status').json()['workers'] if w['worker_id'] == stopped), "worker failure detected")
                verify_download(digest.hexdigest(), path.stat().st_size)
                wait_for(replicated, "two available replicas restored")
                docker("start", stopped)
                stopped = None
                wait_for(lambda: all(w['available'] for w in client.get('/api/v1/status').json()['workers']), "worker returned")
                wait_for(lambda: all(len(c['replicas']) == 2 for c in metadata()['chunks']), "surplus copies reconciled")
                chunk = metadata()["chunks"][0]
                target = chunk["replicas"][0]["worker_id"]
                docker("exec", target, "python", "-c",
                       "import pathlib,sys; pathlib.Path('/data', sys.argv[1]+'.chunk').write_bytes(b'intentionally corrupted demo data')", chunk["chunk_id"])
                verify_download(digest.hexdigest(), path.stat().st_size)
                wait_for(replicated, "corruption repaired")
                # Inventory reads and hashes the actual files rather than trusting metadata alone.
                def actual_replicas_valid():
                    for replica in metadata()['chunks'][0]['replicas']:
                        result = subprocess.run(['docker', 'exec', replica['worker_id'], 'python', '-c',
                            'import urllib.request,sys; urllib.request.urlopen("http://localhost:8000/internal/chunks/"+sys.argv[1]).read()', chunk['chunk_id']], capture_output=True)
                        if result.returncode:
                            return False
                    return True
                wait_for(actual_replicas_valid, "repaired bytes verified on disk")
                docker("compose", "restart", "controller")
                wait_for(lambda: client.get('/health').status_code == 200, "controller restarted")
                verify_download(digest.hexdigest(), path.stat().st_size)
                print("PASS: metadata and chunks persisted across restart", flush=True)
            print("PASS: demonstration complete", flush=True)
        finally:
            if stopped:
                docker("start", stopped)
            if identity:
                response = client.delete(f"/api/v1/objects/{identity}")
                response.raise_for_status()
                wait_for(lambda: client.get('/api/v1/status').json()['pending_cleanup_objects'] == 0, "demo data cleaned up")


if __name__ == "__main__":
    main()
