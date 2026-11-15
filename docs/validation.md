# Backend verification

Run on 2026-11-13 using Python 3.12, Docker Engine 29.5.3, Docker Compose v5.1.4 and the pinned dependency files.

## Completed checks

- `python -m compileall -q object_store migrations scripts tests`: passed.
- `.venv/bin/pytest -q`: **25 passed in 3.81 seconds** after recovery improvements. Tests use real controller/worker ASGI apps and a SQLite metadata database with foreign-key enforcement, with controllable transport failures. They do not substitute for PostgreSQL deployment testing.
- `docker compose up -d --build --wait`: built the application and started one PostgreSQL, one controller, and four worker containers; all six health checks passed. Alembic migration ran successfully against PostgreSQL.
- `.venv/bin/python scripts/demo.py --size-mb 100 --faults`: passed upload, 100 chunks distributed across four workers with two replicas each, checksum-identical downloads, metadata PATCH, worker2 stop/failure detection, download during failure, repair to two live replicas, worker return, surplus reconciliation, intentional corruption fallback, actual repaired-file verification, and demo-data cleanup. This run preceded adding the controller-restart step to the script.

## Larger-file verification

`.venv/bin/python scripts/demo.py --size-mb 500 --faults`: **passed** with 500,000,000-byte objects and 500 chunks. Verified:

1. Upload and distribution across four workers, two distinct replicas per chunk.
2. Complete streamed download with matching SHA-256 and size.
3. Metadata update without re-uploading bytes.
4. `docker stop worker2`, heartbeat expiry, and correct download during failure.
5. Automatic restoration to two available replicas.
6. `docker start worker2` and reconciliation of surplus copies.
7. Deliberate corruption of one demonstration chunk, successful download via a valid replica, and verification of repaired bytes on worker disks.
8. Controller container restart, followed by another complete checksum-matching download using persisted metadata/chunks.
9. Deletion and complete cleanup of the demonstration object.

The live PostgreSQL schema was compared with SQLAlchemy metadata using Alembic's `compare_metadata`; the difference list was empty (`[]`).

## Phase 2 foundation review — 2026-11-13

`python3 scripts/check_foundation.py`: **passed** against the running Docker deployment.

- Compose configuration is valid; exactly six expected services are running and healthy.
- All four workers have separate writable named volumes mounted at `/data`; PostgreSQL and controller spool volumes are also distinct and writable.
- PostgreSQL is reachable, its migration revision matches Alembic head, and its reflected schema matches SQLAlchemy models.
- Controller can resolve and reach every worker by Compose DNS; health responses match configured identities.
- Exactly four workers are registered, available, and actively advancing their heartbeat timestamps.

This is a read-only foundation check. It does not newly prove physical durability by restarting containers; the earlier worker and controller restart demonstrations provide that evidence. No application behavior, stored objects, or container configuration changed during this phase review.

## Phase 3 storage review — 2026-11-13

- `.venv/bin/pytest -q`: **28 passed in 4.29 seconds**, including three new checks for duplicate filenames/pagination, preservation of chunk layout and creation time across metadata updates, and content-type/Unicode attachment headers.
- `.venv/bin/python scripts/check_storage.py`: **passed** against live Docker workers and PostgreSQL for 0, 1, 1,000,000, 1,000,001, 2,500,000 and 100,000,000 bytes.
- Each case verified 201 upload, independent object IDs despite identical filenames, exact object/checksum/chunk metadata, ordered chunk sizes and hashes, two distinct available replicas per chunk, metadata GET/PATCH, and checksum-identical streamed downloads with expected headers.
- The 100 MB object was distributed across all four workers. List results contained every test object and the required summary fields.
- Worker inventory APIs independently read and checksum-verified every test replica; actual worker locations matched controller metadata exactly.
- The script deleted only its own six objects and confirmed their chunk files were absent from every worker. No worker stops, restarts, or corruption were introduced.

Phase 3 normal storage acceptance is complete. The optional 500 MB invocation of this new script was not run; the earlier full 500 MB demonstration remains separate recorded evidence.

## Phase 4 lifecycle correctness — 2026-11-13

Two new regression checks initially failed against the previous implementation: an abandoned `uploading` record was not reclaimed without controller restart, and a delayed chunk PUT recreated bytes after object deletion. Both now pass following the lifecycle fixes.

- `.venv/bin/pytest -q`: **37 passed in 6.31 seconds**, including nine Phase 4 tests. Coverage includes active upload invisibility, abandoned intent recovery, late PUT rejection, lost DELETE acknowledgments, permanent deletion across worker restart, repair/deletion serialization, download disconnect lock release, and preserving the original upload cancellation during a cleanup database outage.
- The disk-full test now verifies that cleanup remains pending when a durable deletion marker cannot be written, then finishes after storage recovers.
- `docker compose up -d --build --wait`: passed with the updated controller and worker images and six healthy services.
- `.venv/bin/python scripts/check_lifecycle.py`: **passed** against live PostgreSQL and Docker. The check uploaded its own small object, stopped a worker, committed logical deletion, verified repeated DELETE and metadata PATCH cannot revive it, restarted the controller, verified the persisted cleanup record, restored the worker, and confirmed physical cleanup on all workers.
- Subsequent internal PUT retries for the deleted chunk returned 409 on every worker, and inventory checks showed no recreated bytes. The script restored the worker and cleaned its own object.

Permanent empty `.deleted` markers remain by design; “physical cleanup” refers to object/chunk bytes and manifests, not removal of the markers needed to reject delayed writes. No schema migration was required. The large-file demonstrations above were not repeated for this change; the full behavioral suite and targeted live lifecycle check were run.

## Phase 5 live recovery completion — 2026-11-14

The Phase 5 code changes passed 46 tests before this live run. `scripts/check_recovery.py` then **passed** with a 10,000,000-byte object over two successive worker failures (worker2, then worker1): checksum-identical downloads immediately after stopping each worker, heartbeat-expiry detection, repair to two live copies while the worker remained stopped, physical checksum/location verification, re-registration and reconciliation to exactly two copies on return, and final cleanup. All workers were restored. This completes the previously pending Phase 5 live acceptance.

## Phase 6 integrity acceptance — 2026-11-14

- Reproduced three worker validation failures before fixing them: oversized claimed chunk size, boolean size, and oversized manifest acceptance.
- `.venv/bin/pytest -q`: **60 passed in 10.94 seconds**, including 14 integrity cases. The suite verifies bounded manifest validation, byte/manifest corruption variants, authoritative controller checksums, rotating background scrubbing without a download, safe failure when both replicas are invalid, and no emission of corrupted bytes when a later chunk fails mid-stream.
- `docker compose up -d --build --wait`: passed with the updated worker/controller code and six healthy services.
- `.venv/bin/python scripts/check_integrity.py`: **passed** with a 10,000,000-byte object. The script captured worker 409 responses for a bit flip and malformed manifest. It also demonstrated a locally consistent altered file/manifest whose checksum differs from authoritative controller metadata.
- After each injection, full downloads matched the original size/SHA-256 and independent worker inventories confirmed exactly two physically valid original copies. Manifest repair was observed through background maintenance without initiating a download first.
- The controller was briefly paused only during corruption injection/evidence capture and resumed in finally blocks. The script deleted its own test object and confirmed its chunk bytes were gone. No unrelated object was deliberately altered.

This targeted live check used 10 MB; the earlier 100/500 MB demonstrations remain separate evidence. Both-copies-unusable and mid-stream failure cases were verified in controlled ASGI tests, not by damaging unrelated live data.

## Phase 7 final submission verification — 2026-11-14

- Audited all required topology, object metadata, chunk/replica, REST, persistent storage, failure and integrity behaviors against the supplied brief.
- Final `.venv/bin/pytest -q`: **62 passed in 11.84 seconds**, including generated OpenAPI request-body/binary-response and 400-validation-contract checks.
- The source ZIP was extracted to a separate temporary directory; the same **62 tests passed in 10.93 seconds** from that extracted source using the installed pinned test environment. This verifies archive completeness, not a separate dependency installation.
- Parsed all 23 Python source files successfully. Exported and checked `docs/openapi.json` against final multipart/validation contracts.
- Rebuilt the normal deployment with final documentation changes; the read-only foundation check passed for six healthy services, isolated volumes, matching migration/schema and advancing worker heartbeats.
- `scripts/verify_fresh.py --large` created a separate random Compose project with new PostgreSQL/worker volumes and an ephemeral loopback port. Fresh migrations, empty initial state, registration and health checks passed.
- The fresh **100,000,019-byte** object passed chunking (including the final partial chunk), distribution, exactly two replicas, listing, metadata PATCH, checksum-identical downloads, worker2 failure detection and automatic repair, return reconciliation, intentional corruption recovery, physical verification of every replica and controller restart persistence.
- The fresh **500,000,019-byte** object also passed upload/chunking/distribution/metadata/listing and a complete checksum-identical download. Both objects completed durable cleanup; the verifier exited successfully and removed only its temporary containers/network/volumes, preserving the normal project.
- An initial verifier run exposed a test-harness assumption that Docker's ephemeral host port remains fixed across restart. The controller was healthy; the verifier was updated to re-resolve the published port and use a bounded readiness request, then restarted from fresh volumes. The rerun passed the restart check.
- `scripts/build_submission.py` verified all 39 ZIP members (38 selected source/document files plus the internal manifest), CRCs and byte equality, and generated `dist/SHA256SUMS`. The delivered archive was regenerated after recording the completed verification outcome and verified again.

No Git commit or push has been made. Phases 1–7 constitute the required backend submission; the optional Phase 8 custom frontend remains deferred. Known limitations in the design/submission guide are unchanged.

## Scope and limitations of evidence

- Disk-full behavior is simulated with ENOSPC in tests; the host disk was not deliberately filled.
- No arbitrary multi-worker-loss or physical-host durability guarantee is claimed.
- Concurrency checks cover metadata merge and deletion lock ordering; this is not a load/throughput benchmark.
- Mid-stream unrecoverable data terminates a download; clients must verify the final size/checksum.
- Local sandbox restrictions blocked localhost access and thread-backed tests; approved executions outside that sandbox completed the checks.
- Generated demonstration objects are deleted by the script, and stopped workers are restored. No user object is deliberately corrupted or deleted.

## 2026-11-15 — Reliability upgrade Phase 1

- Safety baseline committed as `b9fcf1e` at the user's request. The reliability upgrade changes remain uncommitted; nothing has been pushed.
- `.venv/bin/pytest -q`: **74 passed** in 19.48 seconds. Two Alembic configuration deprecation warnings concern the existing unspecified `path_separator`; migration assertions passed. New tests cover persistent/fresh identities, changed volume rejection, read/placement eligibility, inventory authority and discovery, orphan cleanup, failed reconciliation retry, boot/re-registration races, request targeting, initial readiness, and the additive migration. After correcting the existing URL-allowlist test to supply required identity fields, its targeted rerun passed (1 test).
- `docker compose up -d --build --wait`: rebuilt the local services and applied migration 0002 without deleting existing volumes. All six services became healthy. The final build uses worker `/ready` health checks.
- `scripts/check_foundation.py`: PostgreSQL migration head and model/schema agreement, six separate volumes, worker DNS/identities and advancing heartbeats passed after the schema upgrade.
- `scripts/check_recovery.py`: two real Docker worker stop/start cycles with a 10,000,000-byte object passed. Each cycle verified checksum-correct downloads, heartbeat expiry, restoration of two live physical replicas, matching persistent storage UUID and changed boot UUID on return, reconciled eligibility, and convergence to exactly two copies. Stopped workers were restored and test chunks cleaned up.
- No 100/500 MB rerun or refreshed ZIP is claimed for this upgrade phase. Previous size evidence remains historical; final upgrade packaging and broader boundary verification belong to upgrade Phase 3.

## 2026-11-15 — Reliability upgrade Phase 2

- `.venv/bin/pytest -q --tb=short`: **86 passed** in 60.29 seconds. The two warnings are the pre-existing Alembic `path_separator` deprecation warnings. Twelve new protocol tests cover completed receipt replay/reordering, interrupted PUT/DELETE recovery, persistent scan cursor/outbox, fixed scan boundaries, trusted catalog checks, damage acknowledgment/deduplication, obsolete reports, superseded repair intents, queued deletion, sequence preservation and legacy-catalog adoption. Existing integrity tests now exercise worker scans/reporting rather than the removed routine controller scrub.
- `docker compose up -d --build --wait`: all six services rebuilt/healthy, migration 0003 applied while retaining existing volumes. Worker catalogs use SQLite inside the existing worker volumes; the controller remains PostgreSQL. No new container was added.
- `scripts/check_foundation.py`: PostgreSQL migration head/model-schema consistency, volume isolation, service health, identities and heartbeat advancement passed.
- `scripts/check_integrity.py` (10 MB): byte corruption, malformed manifest and consistently altered bytes/manifest were detected and repaired to exactly two original copies. With the controller paused, a worker-local scan produced and retained the manifest damage event; after unpausing, repair completed without a download trigger. Test data was cleaned up and the controller restored.
- `scripts/check_recovery.py` (10 MB): two real worker stop/start cycles passed heartbeat expiry, live replica restoration, physical checksum verification, persistent storage identity/fresh boot, return reconciliation and final cleanup.
- After the final current-intent/stale-expectation guards, the rebuilt deployment passed `scripts/check_storage.py`: 0, 1, 1,000,000, 1,000,001, 2,500,000 and **100,000,000** bytes, upload/download checksums, two physical replicas, list/metadata/PATCH, and cleanup.
- No new Git commit or push was performed. The baseline remains `b9fcf1e`; both reliability upgrades are working-tree changes. The existing ZIP has not been regenerated: general CLI, expanded boundary coverage and final archive refresh remain upgrade Phase 3.

- Final `scripts/check_lifecycle.py`: pending offline deletion survived a controller restart, worker return completed physical cleanup, late PUTs were rejected on every worker, and test-object cleanup was confirmed.

## 2026-11-15 — Reliability upgrade Phase 3

- Added the operational CLI and executable `store` launcher. `scripts/check_cli.py` passed against the normal Docker deployment: upload, all-page list, metadata, PATCH, placements, independently verified download, status, scan/repair inspection and deletion of its own object. Human output was reviewed using `./store status`, `scans` and `repairs`; it uses aligned plain text without ANSI decorations.
- The final packaged source passed **110 tests** from an extracted ZIP in 94.14 seconds. Two warnings remain from the existing Alembic path_separator configuration. The package's executable launcher was preserved by unzip and its help command ran successfully with the installed Python environment. The 21 CLI tests cover contracts, pagination, error/JSON output, interrupted/corrupt downloads, overwrite safety, empty files, maximum-length filenames and invalid controller URLs.
- The expanded real storage checker passed sizes **0, 1, 999,999, 1,000,000, 1,000,001, 2,000,000, 2,000,001 and 2,500,000 bytes**, plus its selected 2 MB main case. It independently verified physical replica hashes/locations, metadata/list/PATCH/download behavior and cleanup of its objects.
- The first isolated `verify_fresh.py --large` run passed the 100 MB fault/restart sequence and the 500 MB round trip, but hit the fixed 180-second cleanup deadline for 501 chunks. Cleanup was observed progressing; the verifier removed its own temporary deployment in finally. Changed only the verification deadline to max(180 seconds, 2 seconds per chunk), retaining a finite bound and all durability assertions. This explicitly accommodates the four-worker permanent deletion work for large objects.
- No Phase 3 Git commit or push has been performed. Generated submission archives remain ignored by Git. The original normal deployment was checked separately: four available workers, no pending cleanup and no repair jobs.

- The corrected final `scripts/verify_fresh.py --large` run **passed completely** on new isolated PostgreSQL/worker volumes: 100,000,019-byte upload/download, failure/repair/rejoin, corruption recovery, independent physical replica checks, controller restart, 500,000,019-byte round trip, and completed physical deletion for both objects. All temporary containers/network/volumes were removed; the existing deployment was preserved. Large-file cleanup is asynchronous and can take several minutes on this local setup.
- Rebuilt the final source ZIP after recording the validation outcome. The builder verified all **51 members** (50 source/document files plus MANIFEST.sha256), CRCs and byte equality, and generated SHA256SUMS. The extracted source had already passed the final 110-test suite; subsequent changes were validation documentation and the live-verified cleanup deadline. Launcher executable permissions were verified. No user objects were altered by the acceptance tests.
