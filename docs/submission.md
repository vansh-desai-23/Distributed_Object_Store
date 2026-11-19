# Submission and demonstration guide

## Project scope and completion

The required project is a local distributed object-storage backend. Phases 1–7 cover design, infrastructure, normal storage, lifecycle correctness, worker failures, integrity, and submission. The optional Phase 8 custom frontend remains a post-submission extension. Swagger API documentation is available now; it is not a custom frontend.

The system stores arbitrary bytes in decimal 1 MB chunks, with two replicas per chunk on different workers. The controller provides the required REST API and owns a relational metadata database. Four worker containers independently persist chunk files. Python is used for both services and the demonstration client.

## Submission contents

- `object_store/`: controller, worker, coordinator, metadata models, shared validation and operational CLI.
- `store`: executable CLI launcher; `docs/cli.md`: command reference and download safety contract.
- `migrations/`: versioned PostgreSQL schema migrations and Alembic runner.
- `Dockerfile`, `compose.yaml`, locked dependencies: reproducible local environment.
- `tests/`: automated behavioral tests, including failure/concurrency/integrity checks.
- `final_demo/`: single guided demo, isolated fresh-deployment verification and packaging.
- `docs/design.md`: architecture diagram, schema, API contracts, lifecycle and recovery algorithms.
- `docs/validation.md`: actual observed verification results and their limits.
- `docs/openapi.json`: generated client/controller API specification.
- `README.md`: setup and commands; `AGENT.md`: decisions and precise file inventory.
- Supplied project brief, unchanged.

The ZIP excludes `.git`, virtual environments, credentials/environment files, caches, test binaries, Docker volumes and build output. Git commits are made only on explicit user instruction; no push is performed automatically. `MANIFEST.sha256` inside the ZIP lists each source file's checksum; `dist/SHA256SUMS` checks the ZIP itself.

## Set up and present

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
docker compose up --build -d --wait
docker compose ps
.venv/bin/python final_demo/demo.py
```

The demo is a client of the running backend, not another server. Keep a second terminal
for Docker logs. Press Enter at each stage to execute the announced actions; worker
stop/start, controller pause/restart and corruption injection are automated. Do not run
another fault demo concurrently. The script only corrupts/deletes its own generated
objects, restores service availability on exit and reports any incomplete cleanup.

Use `--unattended` for automatic rehearsal and `--size-mb 2` for a quick small run.
The presentation default is 100 MB plus 19 bytes, exercising a partial final chunk.
The required scenarios always run; there is no separate `--faults` switch.

| Requirement / extension | Live evidence |
| --- | --- |
| Docker topology/persistence | Six healthy services and six isolated writable named volumes |
| Binary object storage | Original and downloaded byte counts/SHA-256 match |
| Chunking/distribution | All positions, sizes and hashes verified; per-worker totals and sample placements displayed |
| Replication | Every chunk independently verified through actual worker inventories |
| Metadata/list/API | System fields, PATCH preserving layout/bytes, paginated listing, 200/201/204 and JSON 400/404/409/410 |
| Worker failure | Download while stopped; two live physical copies restored before return |
| Identity and rejoining | Same storage UUID, different boot UUID, 503 readiness before registration, eligibility after reconciliation |
| Offline deletion | Immediate 404 while worker is down, physical cleanup and no resurrection after return |
| Integrity and reports | Corrupt own replica with controller paused; local scan advances and retains damage report; valid retrieval, physical repair and acknowledgment |
| Upload idempotency | Identical original response on retry, conflict rejection, receipt persistence and 410 after physical deletion |
| Restart persistence | Main object bytes/custom metadata and keyed receipt survive controller restart |

The final stage deletes the main object and verifies physical cleanup. Results show each
stage's PASS/FAIL and duration; failure returns a nonzero process exit code. Boundary and
race-condition tests remain separately runnable with `.venv/bin/python -m pytest -q`.
This presentation does not claim to reproduce every interleaving covered by that suite.

## Fresh-deployment verification

Build the current local image, then run:

```bash
docker compose build controller
.venv/bin/python final_demo/verify_fresh.py --large
```

This creates a separate random Compose project, network, six fresh volumes, and an ephemeral loopback port. It removes fixed worker container names in its temporary configuration, so it can coexist with the normal deployment. It re-resolves the ephemeral controller port after a restart and verifies initial migrations/registration, 100 MB plus a partial chunk, worker failure/repair, corruption recovery, controller restart, deletion, and optionally 500 MB plus a partial chunk. The physical-cleanup deadline scales with chunk count (at least 180 seconds, otherwise two seconds per chunk) because every chunk is durably fenced on all four workers. Its finally block removes only that temporary project's containers, network and volumes. Normal project data is preserved. It reuses the image just built; this is fresh-data deployment verification, not proof of a cold registry download.

## Package the final source

```bash
python3 final_demo/build_submission.py
```

The checksum file uses the archive basename. Verify with:

```bash
(cd dist && sha256sum -c SHA256SUMS)
```

Deliver `dist/mini-distributed-object-store.zip`. Regenerate the ZIP after any source or documentation change. Do not include generated test objects or `.venv` in a manual submission.

## Questions to be ready to answer

- **Why PostgreSQL?** Durable relational metadata and transactions for object visibility and cleanup intents; binary files belong on the workers.
- **Why REST rather than gRPC?** The brief specifies HTTP interfaces, and binary HTTP transfers meet this scale's needs.
- **Why no Kafka or leader election?** A single controller and durable metadata-driven maintenance suffice; there is no controller election requirement.
- **What does replication protect against?** A storage-worker process failure when another valid copy exists. All containers still share one physical host.
- **What happens to a failed upload?** It remains invisible; recorded destinations are cleaned up, including after restart/database recovery.
- **Can deletion be delayed?** Logical deletion commits before 204; physical cleanup waits for offline/full workers. Tiny permanent markers reject delayed writes.
- **What if both copies are corrupt?** The system fails safely, preserves records, and cannot reconstruct missing information without a valid source.
- **What if failure occurs halfway through a download?** Only verified chunks are emitted; the connection terminates and the client rejects the incomplete size/checksum.

## Known limits

One controller process and one metadata database remain availability dependencies. Per-object locks can delay deletion/repair behind active downloads. Metadata transactions are synchronous and intended for a small local workload. No authentication/TLS, backup service, resumable transfer, range requests, versioning or multi-host failure guarantee is included. Permanent deletion markers and in-memory lock entries are retained. Client POST retries without Idempotency-Key can create a second complete object after an ambiguous response; keyed retries replay the original response. Upload receipts are retained without expiry. The optional frontend is deferred; no claim of production readiness is made.

## Reliability upgrades after the original submission baseline

Upgrade Phases 1–2 add persistent storage/boot identity, inventory-gated readiness, local SQLite catalogs, ordered mutation receipts, persisted worker scan progress, acknowledged damage reports and a durable serial repair queue. PostgreSQL remains the controller database; no containers were added. `final_demo/demo.py` proves worker-local report retention with the controller paused; automated tests retain broader corruption variants. Status exposes progress and pending repairs. See design.md for protocol/recovery details and validation.md for current evidence.

Upgrade Phase 3 includes the operational CLI, complete chunk-boundary matrix and refreshed archive. Regenerate the archive after source/documentation changes and verify `dist/SHA256SUMS` before delivery. See validation.md for the actual final checks.

## Demonstrating upload idempotency

Use `./store upload FILE --idempotency-key demo-upload-001` twice with identical arguments: both return the same object ID. Changing file content or metadata with that key returns 409. Deleting the object and retrying the original keyed upload returns 410. A controller restart preserves completed receipts. The idempotency stage in `final_demo/demo.py` automates these checks with a small private object, actual controller restart and physical cleanup; see validation.md for observed results.
