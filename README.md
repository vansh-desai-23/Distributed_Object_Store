# Mini Distributed Cloud Object Store

Python REST backend for one controller and four persistent storage workers, using PostgreSQL metadata and Docker Compose. Objects are divided into decimal 1 MB chunks with two replicas on different workers. No frontend or cloud account is needed.

## Submission guide

See [docs/submission.md](docs/submission.md) for the presentation sequence, requirement-to-evidence mapping, fresh-deployment verification, packaging instructions and known limitations. The required backend covers Phases 1–7; the optional custom frontend remains deferred.

To create the source submission archive after validation:

```bash
python3 scripts/build_submission.py
```

Output: `dist/mini-distributed-object-store.zip`, with source checksums inside and an archive checksum alongside it. No Git commit or push is performed.

## Start

Prerequisites: Docker Engine with Compose; Python 3.12+ for local tests/demo.

```bash
docker compose up -d --build --wait
docker compose ps
curl http://127.0.0.1:8000/api/v1/status
```

Wait for all four workers to appear as available. The API is at http://127.0.0.1:8000 and generated API documentation at http://127.0.0.1:8000/docs. PostgreSQL migrations run automatically before the controller starts. Port 8000 and container names worker1..worker4 must be free.

Six containers run locally. Each worker has a separate named volume; PostgreSQL and upload spool storage also have volumes. Worker and database ports are not published. These are local-development credentials/network settings, not an Internet deployment.

## Phase 2 foundation check

After startup, run this read-only check (Python standard library and Docker only):

```bash
python3 scripts/check_foundation.py
```

It verifies the six healthy services, isolated writable named volumes, PostgreSQL migration/schema consistency, worker DNS/identities, registration, and advancing heartbeats. It does not restart containers or modify stored objects. This checks the infrastructure foundation independently of the larger storage/failure demonstrations below.

## Phase 3 storage acceptance

```bash
.venv/bin/python scripts/check_storage.py
# Optional larger-file acceptance run:
.venv/bin/python scripts/check_storage.py --size-mb 500
```

Requires the running local Compose stack and the Python dependencies installed below. This exercises empty, one-byte, exact 1 MB, 1 MB + 1 byte, 2.5 MB, and 100 MB files by default. It verifies chunk ordering/sizes/hashes, two distinct replicas, downloads, listing, duplicate filenames, and metadata GET/PATCH. Worker inventory APIs independently verify the actual stored replicas. It creates and cleans up only its own test objects, and does not stop containers or inject corruption. Run with all four workers available and without simultaneous fault demonstrations.

## Phase 4 lifecycle acceptance

```bash
.venv/bin/pytest -q tests/test_lifecycle.py
.venv/bin/python scripts/check_lifecycle.py
```

The live check creates a small private test object, stops one worker, deletes the object, and restarts the controller while cleanup is pending. It verifies that the deletion record survives, restarts the worker, checks physical cleanup, and confirms late chunk PUTs are rejected on every worker. It restores the stopped worker and cleans up its object in a finally block. Run when no other demonstration or upload is active, since the controller restart interrupts other requests.

Final deletion writes a small persistent `.deleted` marker on every registered worker. These markers contain no object bytes and prevent delayed requests from recreating deleted chunks, including after restart. They are intentionally retained; there is no automatic marker garbage collection. A worker/disk outage keeps cleanup pending until the deletion marker can be safely persisted.

## Phase 5 worker-failure acceptance

```bash
.venv/bin/pytest -q tests/test_recovery.py
.venv/bin/python scripts/check_recovery.py
# Optional larger object:
.venv/bin/python scripts/check_recovery.py --size-mb 100
```

The live check uses 10 MB by default and runs two successive worker stop/start cycles. It checks downloads immediately after stopping a worker, heartbeat expiry, automatic restoration of two live replicas, independent on-disk checksum verification, and reconciliation to exactly two copies when the worker returns. It restores stopped workers and deletes only its own test object. Run separately from other fault demonstrations; worker stops temporarily affect the whole local cluster.

A worker explicitly registers on startup. A return after heartbeat expiry (or explicit re-registration) makes old healthy replicas pending until verified, rather than treating a heartbeat as proof that data survived. Network/server errors mean unverified availability; missing or checksum-invalid data is marked bad. Repair waits and retries when no valid source or suitable destination is available.

## Phase 6 integrity acceptance

```bash
.venv/bin/pytest -q tests/test_integrity.py
.venv/bin/python scripts/check_integrity.py
# Optional larger object:
.venv/bin/python scripts/check_integrity.py --size-mb 100
```

The live check uses 10 MB by default. It corrupts bytes, damages a checksum manifest, and alters both bytes and their worker-local checksum in its own test object. The controller is briefly paused during each injection so the script can capture the worker response before repair runs, then immediately resumed. It checks full download hashes and exactly two repaired copies on disk; the manifest case relies on background scrubbing without triggering a download first. Run when no other work is using the local controller. The script restores the controller and deletes its own object in finally blocks.

Workers validate manifest structure, an integer size within 1..1,000,000 bytes, and a 64-character SHA-256 value, reading at most 4,097 manifest bytes to enforce a 4,096-byte limit. The controller independently compares chunk bytes with its PostgreSQL checksum; a locally consistent but altered worker file is still rejected. If every copy is invalid, the system preserves the records and fails rather than propagating corrupt data. Checksums detect accidental corruption; they are not authentication against an attacker who can modify controller metadata too.

## Use the API

```bash
curl -sS -X POST http://127.0.0.1:8000/api/v1/objects \
  -F 'file=@/path/to/file.bin;type=application/octet-stream' \
  -F 'metadata={"course":"cloud","owner":"student"}'
```

Copy `object_id` from the response and substitute it below:

```bash
OBJECT_ID='replace-with-returned-uuid'
curl -sS http://127.0.0.1:8000/api/v1/objects
curl -sS "http://127.0.0.1:8000/api/v1/objects/$OBJECT_ID/metadata"
curl -sS -X PATCH "http://127.0.0.1:8000/api/v1/objects/$OBJECT_ID/metadata" \
  -H 'Content-Type: application/json' -d '{"owner":"updated"}'
curl --fail -o downloaded.bin "http://127.0.0.1:8000/api/v1/objects/$OBJECT_ID"
sha256sum downloaded.bin
curl -i -X DELETE "http://127.0.0.1:8000/api/v1/objects/$OBJECT_ID"
```

Compare `sha256sum` to the metadata's whole-object checksum. Metadata also shows chunk ordering, size, checksum, worker locations, last-known replica state, and heartbeat availability. DELETE returns 204 after logical deletion; physical cleanup can continue while a worker is offline. PATCH accepts only custom string key-value pairs.

## Tests and submission demonstration

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/pytest -q
.venv/bin/python scripts/demo.py --size-mb 100 --faults
.venv/bin/python scripts/demo.py --size-mb 500
```

The fault demo creates its own object, verifies chunking/distribution/checksums and metadata PATCH, stops worker2, downloads during failure, waits for repair, restarts the worker, verifies surplus cleanup, deliberately corrupts one chunk of its own object, verifies fallback and repaired files, and restarts the controller to demonstrate persistence. It restores the stopped worker in a finally block and deletes its demo object at the end. `--faults` deliberately changes container availability; run when no other demonstration is in progress. Run from this repository directory.

Demo files are generated and deleted automatically. Downloads are hashed in memory one chunk at a time and do not create another full file. The 100 MB run is the main demonstration; 500 MB is a larger-file check. Larger objects are supported as resources permit. Avoid filling the roughly 5 GB budget with multiple retained test objects; 500 MB requires approximately 1 GB of replica storage alone.

Manual inspection/failure commands:

```bash
docker stop worker2
curl http://127.0.0.1:8000/api/v1/status
docker start worker2
docker compose logs --tail=50 controller
# Worker inventory, including on-disk checksum verification:
docker exec worker1 python -c "import urllib.request; print(urllib.request.urlopen('http://localhost:8000/internal/workers/worker1/chunks').read().decode())"
```

## Stop and restart

```bash
docker compose stop
docker compose start
# Or remove containers/network while retaining named volumes:
docker compose down
docker compose up -d --wait
```

`docker compose down -v` permanently deletes all project volumes and stored data; it is not needed for normal shutdown. No reset/volume deletion is part of the demonstration.

## Design and boundaries

See [design.md](docs/design.md) for Phase 1 decisions, schema, complete REST contracts, state transitions, recovery policies, and acceptance criteria. See [validation.md](docs/validation.md) for actual verification results. [AGENT.md](AGENT.md) records prompt-driven changes and exact code-file responsibilities.

- One controller process only. Do not add Uvicorn workers or controller replicas: lifecycle locks are intentionally process-local.
- Replication protects against a worker process failure; a single-host system cannot tolerate loss of its physical disk or metadata database.
- Checksums are verified before each chunk is returned. If all copies of a later chunk fail after download headers were sent, the stream terminates; clients must verify size/checksum.
- Downloads already in progress hold an object lock; deletion waits for those downloads before committing logical removal.
- PostgreSQL owns metadata; persistent filesystem volumes own binary data. No Kafka, gRPC, Kubernetes, or leader election is needed for this topology.
- Authentication, TLS, high availability of the controller/database, backups, and resumable transfers are outside this local coursework implementation. The optional frontend remains deferred.
