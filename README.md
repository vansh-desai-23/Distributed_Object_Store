# Mini Distributed Cloud Object Store

Python REST backend for one controller and four persistent storage workers, using PostgreSQL metadata and Docker Compose. Objects are divided into decimal 1 MB chunks with two replicas on different workers. No frontend or cloud account is needed.

## Command-line client

Use `./store` for a clean terminal interface (the launcher uses your project `.venv`):

```bash
./store upload report.pdf --meta owner=student --idempotency-key report-upload-001
./store list
./store metadata OBJECT_ID
./store placements OBJECT_ID
./store download OBJECT_ID -o downloaded.pdf
./store patch OBJECT_ID --meta owner=updated
./store status
./store scans
./store repairs
./store delete OBJECT_ID
```

Downloads are size/SHA-256 verified before saving the final file. Existing destinations require `--force`. Add `--json` for scripts, `--url` for another controller, or `--help` for command options. See [docs/cli.md](docs/cli.md) for the complete guide.

## Start

Prerequisites: Docker Engine with Compose; Python 3.12+ for local tests/demo.

```bash
docker compose up -d --build --wait
docker compose ps
curl http://127.0.0.1:8000/api/v1/status
```

Wait for all four workers to appear as available. The API is at http://127.0.0.1:8000 and generated API documentation at http://127.0.0.1:8000/docs. PostgreSQL migrations run automatically before the controller starts. Port 8000 and container names worker1..worker4 must be free.

Six containers run locally. Each worker has a separate named volume; PostgreSQL and upload spool storage also have volumes. Worker and database ports are not published. These are local-development credentials/network settings, not an Internet deployment.

## Guided demonstration

Start Docker Compose first, then run the single presentation client:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python final_demo/demo.py
# Same checks without presentation pauses:
.venv/bin/python final_demo/demo.py --unattended
```

The opening checklist is printed once. Seven stages cover deployment, binary round-trip,
chunk order/distribution and physical replicas, metadata/list/errors, worker failure and
offline deletion, proactive corruption detection/report delivery/repair, restart-safe
upload idempotency, and final deletion. Stages pause for Enter; Docker fault commands
run automatically and are announced. Use a second terminal for logs or inspection,
not simultaneous fault injection. The operational CLI is shown during metadata inspection.

The default main object is 100 MB plus a 19-byte final chunk. `--size-mb 2` provides a
quick rehearsal; values up to 500 are supported. Downloads are streamed and hashed.
The script uses the local controller at port 8000 and the current repository's Compose
services. Run on an otherwise idle local deployment: service interruptions affect the
whole cluster, although only this run's objects are corrupted/deleted. It restores
containers and removes its data even after an error or Ctrl+C, reports cleanup failures,
and exits nonzero on failure. No Docker volumes are removed.

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

## Automated tests

```bash
.venv/bin/python -m pytest -q
```

These tests retain chunk boundaries, stale operations, interrupted writes, concurrency,
worker membership, integrity, lifecycle, CLI and upload-idempotency coverage independently
of the presentation.

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

See [design.md](docs/design.md) for the architecture, schema, REST contracts, state transitions, recovery policies, and acceptance criteria.

- One controller process only. Do not add Uvicorn workers or controller replicas: lifecycle locks are intentionally process-local.
- Replication protects against a worker process failure; a single-host system cannot tolerate loss of its physical disk or metadata database.
- Checksums are verified before each chunk is returned. If all copies of a later chunk fail after download headers were sent, the stream terminates; clients must verify size/checksum.
- Downloads already in progress hold an object lock; deletion waits for those downloads before committing logical removal.
- PostgreSQL owns metadata; persistent filesystem volumes own binary data. No Kafka, gRPC, Kubernetes, or leader election is needed for this topology.
- Authentication, TLS, high availability of the controller/database, backups, and resumable transfers are outside this local coursework implementation.

## Durable integrity and repair

Workers use Python's built-in SQLite module for a small `catalog.sqlite` file inside their existing data volume. PostgreSQL remains the controller database, and the deployment still has six containers. This adds durable local metadata, not another database service.

- A fixed-order local scan checks up to 16 cataloged chunks per pass (default every 3 seconds). Its cursor, cycle ceiling, completed-cycle count and checked count survive restart. Checks use trusted expected size/hash from PUT or controller reconciliation, never a recomputed replacement hash.
- Damage reports have durable event IDs and repeat in heartbeats until acknowledged. The controller commits deduplication and repair intent before acknowledgment; an old report cannot invalidate a newer replacement operation.
- The controller coalesces read failures, availability changes, inventory findings and damage reports into one persistent repair job per chunk. One serial loop handles up to 32 jobs per pass, verifies replacement bytes and checks current object/repair intent before publication. Unresolved jobs retain attempts/error details and retry.
- Mutations carry a durable increasing operation sequence. Workers record the sequence and intent before changing files, retain completion receipts, reject stale/conflicting requests, and recover interrupted publication/deletion on startup. Permanent deletion markers still forbid resurrection at any sequence.

`GET /api/v1/status` includes each worker's last reported `scan_progress` and up to 100 pending `repair_jobs` (reason, revision, attempts and last error). Worker `GET /internal/scan` exposes local progress and up to 100 unacknowledged reports on the internal network. The CLI exposes these inspection APIs through `store status`, `store scans` and `store repairs`.

Catalogs, operation high-water marks, deduplication receipts and permanent deletion markers are retained rather than garbage-collected. Back up controller metadata and worker volumes together; restoring an older controller database against newer worker sequence records is not supported. The system continues to require one controller process and one worker process per volume.

## Retry uploads safely

Choose a unique key for each intended object and reuse it if the response is lost:

```bash
./store upload report.pdf --meta owner=student --idempotency-key report-upload-001
# Retry the identical command with the same key.
```

The API accepts the optional `Idempotency-Key` header on `POST /api/v1/objects`. Matching requests return the saved original 201 response and object ID, with `Idempotency-Replayed: true` on a replay (`false` on the first success). The key binds the file bytes, normalized filename, content type and custom metadata. Changed requests return 409. Matching retries after deletion return 410; use a new key to intentionally create another object.

Keys are case-sensitive, globally scoped in this local service, and contain 1–128 ASCII letters/digits or `. _ : -` (without spaces). Receipts persist in PostgreSQL across controller restarts and are retained after object cleanup. A failed, unpublished attempt can retry with the same key and identical payload; its old partial data remains subject to cleanup. Requests still in progress can return 409 and should be retried later. Replays return the original response snapshot; use metadata/status APIs for current metadata and replica health.

Uploads without a key remain independent. The CLI does not retry automatically or generate a key implicitly. Keyed retries resend the complete request, with a bounded extra hashing pass over the existing upload spool; this is not resumable upload. The guided demo verifies keyed replay, conflicts, controller-restart persistence and rejection after physical deletion.
