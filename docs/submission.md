# Submission and demonstration guide

## Project scope and completion

The required project is a local distributed object-storage backend. Phases 1–7 cover design, infrastructure, normal storage, lifecycle correctness, worker failures, integrity, and submission. The optional Phase 8 custom frontend remains a post-submission extension. Swagger API documentation is available now; it is not a custom frontend.

The system stores arbitrary bytes in decimal 1 MB chunks, with two replicas per chunk on different workers. The controller provides the required REST API and owns a relational metadata database. Four worker containers independently persist chunk files. Python is used for both services and the demonstration client.

## Submission contents

- `object_store/`: controller, worker, coordinator, metadata models and shared validation.
- `migrations/`: initial PostgreSQL schema and Alembic runner.
- `Dockerfile`, `compose.yaml`, locked dependencies: reproducible local environment.
- `tests/`: automated behavioral tests, including failure/concurrency/integrity checks.
- `scripts/`: phase acceptance checks, combined demo, isolated fresh-deployment verification and packaging.
- `docs/design.md`: architecture diagram, schema, API contracts, lifecycle and recovery algorithms.
- `docs/validation.md`: actual observed verification results and their limits.
- `docs/openapi.json`: generated client/controller API specification.
- `README.md`: setup and commands; `AGENT.md`: decisions and precise file inventory.
- Supplied project brief, unchanged.

The ZIP excludes `.git`, virtual environments, credentials/environment files, caches, test binaries, Docker volumes and build output. The Git repository remains local and uncommitted until the owner explicitly requests a commit. `MANIFEST.sha256` inside the ZIP lists each source file's checksum; `dist/SHA256SUMS` checks the ZIP itself.

## Set up from an extracted submission

From the extracted `mini-distributed-object-store` directory:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
docker compose up -d --build --wait
python3 scripts/check_foundation.py
.venv/bin/pytest -q
```

Use Python 3.12+ and Docker Engine with Compose. Internet access is needed for the initial image/package downloads. Reserve roughly 5 GB for this project, and run the demonstrations sequentially. Six services should be healthy, with all four workers registered.

## Recommended presentation sequence

1. **Explain the architecture.** Open the diagram in `docs/design.md`: one controller, PostgreSQL metadata and four workers with separate volumes. Show `docker compose ps`.
2. **Show the API.** Open `http://127.0.0.1:8000/docs`. Upload a small file, inspect its metadata/chunk locations, download it, PATCH custom metadata, and list objects. Swagger now exposes the multipart file and metadata fields. Use the README curl commands if preferred.
3. **Run the main 100 MB demonstration.**

   ```bash
   .venv/bin/python scripts/demo.py --size-mb 100 --faults
   ```

   Explain the 100 decimal chunks, two distinct replicas, distribution across workers, matching SHA-256, metadata update, worker2 stop/start, repair, corruption handling and controller restart. The script prints PASS checkpoints and cleans only its own object.
4. **Show deeper integrity evidence if requested.**

   ```bash
   .venv/bin/python scripts/check_integrity.py
   ```

   This proves detection and recovery for bytes, manifests, and a locally consistent but incorrect worker checksum. It briefly pauses the controller during controlled injection and restores it automatically.
5. **Show automated test and acceptance evidence.** Explain the difference between controlled ASGI fault tests, real PostgreSQL/Docker acceptance and large-file verification in `docs/validation.md`.

The 500 MB check is optional during the presentation; run `.venv/bin/python scripts/demo.py --size-mb 500` if time and free disk permit. Stop fault demonstrations before starting another one.

## Requirement-to-evidence mapping

| Brief requirement | Implementation/evidence |
|---|---|
| One controller, four workers, local Docker, persistent worker storage | Compose topology, separate named volumes, foundation checker |
| Arbitrary binary objects and unique IDs | Byte-preserving upload/download, UUID4 identity, boundary and content-type tests |
| 1 MB chunks and ordered reconstruction | Decimal 1,000,000-byte chunks, unique positions, final-partial-chunk tests |
| Two replicas on different workers, distribution and repair | Placement algorithm, metadata locations, worker inventory verification, recovery checker |
| Object/system/custom metadata | Relational tables, metadata GET/PATCH, immutable content/checksum tests |
| Required client methods/paths/statuses | Controller routes, generated OpenAPI, API and behavioral tests |
| Registration/heartbeat and internal chunk/inventory APIs | Worker/controller routes and foundation/recovery checks |
| Worker stop/restart demonstration | Combined demo and two-cycle recovery checker |
| Intentional corruption and valid-copy recovery | Integrity checker and corruption tests |
| Runnable instructions and one or more clients | README, Python demo/checkers, curl and Swagger |
| Clean initial deployment | Isolated fresh verifier with new PostgreSQL/worker volumes |

## Fresh-deployment verification

Build the current local image, then run:

```bash
docker compose build controller
.venv/bin/python scripts/verify_fresh.py --large
```

This creates a separate random Compose project, network, six fresh volumes, and an ephemeral loopback port. It removes fixed worker container names in its temporary configuration, so it can coexist with the normal deployment. It re-resolves the ephemeral controller port after a restart and verifies initial migrations/registration, 100 MB plus a partial chunk, worker failure/repair, corruption recovery, controller restart, deletion, and optionally 500 MB plus a partial chunk. Its finally block removes only that temporary project's containers, network and volumes. Normal project data is preserved. It reuses the image just built; this is fresh-data deployment verification, not proof of a cold registry download.

## Package the final source

```bash
python3 scripts/build_submission.py
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

One controller process and one metadata database remain availability dependencies. Per-object locks can delay deletion/repair behind active downloads. Metadata transactions are synchronous and intended for a small local workload. No authentication/TLS, backup service, resumable transfer, range requests, versioning or multi-host failure guarantee is included. Permanent deletion markers and in-memory lock entries are retained. Client POST retries can create a second complete object after an ambiguous response. The optional frontend is deferred; no claim of production readiness is made.

## Reliability upgrades after the original submission baseline

Upgrade Phases 1–2 add persistent storage/boot identity, inventory-gated readiness, local SQLite catalogs, ordered mutation receipts, persisted worker scan progress, acknowledged damage reports and a durable serial repair queue. PostgreSQL remains the controller database; no containers were added. `scripts/check_integrity.py` now proves worker-local report retention with the controller paused, including catalog rejection of consistently altered bytes/manifest. Status exposes progress and pending repairs. See design.md for protocol/recovery details and validation.md for current evidence.

The existing dist archive predates these upgrades. The operational CLI, expanded boundary matrix and refreshed archive are upgrade Phase 3; regenerate the archive only after final validation.
