# Project collaboration record

## Prompt-driven changes and working agreements

### Working agreements established on 2026-11-13

- Discuss phases, architecture, and technology choices before implementing the project.
- Python is required. Prefer a relational database if a database is needed.
- Use class concepts where useful; identify additions beyond the supplied syllabus.
- Keep a custom frontend until the very end, potentially after submission.
- Use Git. Commit only when the user explicitly requests a commit. Do not push without authorization. Remind the user when changes have not been pushed.
- Update this file for each change-producing prompt with all changes made, their validation, and any unresolved decisions. Maintain an exact inventory of every code file: its purpose, functions/classes, inputs/outputs, dependencies, and important behavior. Separate the change history and code inventory with a dashed line.
- Record finalized decisions here; keep tentative discussion out of this file. The finalized baseline below supersedes the initial proposal. Phase 1 is specified in docs/design.md; the authorized backend implementation follows it.

### 2026-11-13 — Review the brief and propose project phases

User request: inspect the supplied project brief; discuss distinct implementation phases and tools before building; create this record and use Git without automatic commits.

Changes made:
- Initialized an empty Git repository on branch `main`. No commit or push was made; no remote was configured.
- Created `AGENT.md` with collaboration agreements, this change record, the proposed plan, and the initial code inventory.
- No application code, dependency installation, or implementation scaffolding was created. The supplied PDF was not modified.

Review and validation:
- Read the complete extracted text of `Mini_Distributed_Cloud_Object_Store.pdf`.
- Verified that the folder initially contained the PDF and was not a Git repository.
- Consulted official FastAPI and Docker documentation for multipart uploads and persistent Compose volumes.
- Git initialization required sandbox approval because `.git` is read-only under the normal sandbox.

Requirements from the brief:
- One controller, four workers, one or more clients; local Docker deployment, with Compose permitted and Kubernetes prohibited for worker orchestration.
- Arbitrary binary objects split into 1 MB chunks; preserve chunk order and maintain two copies on different workers.
- Persistent worker storage; worker registration, heartbeats, inventory, chunk storage/retrieval/deletion APIs.
- Client REST APIs for upload, download, metadata retrieval/update, deletion, and listing, including the specified paths and status codes.
- Object/system metadata, custom key-value metadata, checksums, failure detection, replica restoration, and corruption detection/recovery where a valid copy remains.
- Demonstrate storage, chunk distribution, replication, metadata updates, worker stop/restart, and intentional data corruption.

Finalized technology baseline:
- Python, FastAPI/Uvicorn for controller and worker REST APIs, HTTPX for internal requests.
- PostgreSQL for controller-owned metadata; SQLAlchemy and Alembic for schema access and migrations. Worker chunks remain files on separate persistent Docker volumes.
- Docker Compose for one controller, four instances of one worker implementation, and one supporting PostgreSQL service.
- Python hashlib/SHA-256 and UUIDs for integrity and identifiers; asyncio for periodic heartbeat and repair tasks.
- pytest and HTTPX for tests; curl or a small Python client for demonstrations. Custom frontend deferred.
- No gRPC, broker, Kubernetes, ZooKeeper, or Raft in the agreed baseline. The single controller remains an acknowledged availability limitation.

Agreed phases and completion criteria (phase-oriented, no day-by-day schedule):
1. Design agreement: settle API schemas, relational model, chunk-size byte interpretation, upload visibility, deletion semantics, and failure/recovery policies.
2. Foundation: controller and worker skeletons, metadata migrations, Docker Compose, isolated persistent volumes, health checks, registration, and heartbeats; all required services start locally.
3. Storage happy path: chunking, distribution, two distinct worker replicas, upload/download/list/metadata operations; multi-chunk binary objects round-trip with matching checksums.
4. Lifecycle correctness: deletion, partial-upload cleanup, retries/idempotency, concurrency, and durable pending cleanup; incomplete objects stay hidden and deleted objects do not reappear.
5. Failure recovery: timeouts, heartbeat expiry, alternate-replica reads, repair, worker return reconciliation; stop/restart demonstration succeeds when sufficient copies remain.
6. Integrity: validate chunk hashes, alternate-copy recovery, periodic checks and repair; intentional corruption is detected and repaired when possible. Checksums begin in phase 3.
7. Submission: automated integration tests, reproducible demonstration scripts/data, documentation, architecture explanation, and known limitations; every required demonstration works from a clean deployment.
8. Optional post-submission frontend consuming the established REST API.

### Finalized scope and behavior from the planning discussion

- Deployment entirely on the local machine is sufficient. Supporting containers are allowed when justified; retain all mandatory controller/worker requirements. PostgreSQL is the supporting metadata database container.
- Use independently running controller and worker services; four workers are instances of the same worker implementation. Do not split metadata, placement, or repair into additional services for the baseline.
- Use REST for both client and internal APIs. gRPC and Kafka are omitted because they do not solve a baseline requirement, not because additional technologies are prohibited. Persist pending work in PostgreSQL and process it with controller background tasks.
- Tools beyond the supplied syllabus include FastAPI/Uvicorn, HTTPX, SQLAlchemy/Alembic, asyncio, checksum-based integrity checks, and pytest; explain these as introduced.
- Publish an object and return upload success only after every chunk has two confirmed durable replicas on distinct workers. Keep incomplete uploads hidden and clean up partial data. If insufficient workers/storage prevent the guarantee, fail the upload rather than silently accepting one copy; use 503 for required storage unavailability.
- Detect unavailable workers and automatically restore replication when a valid source replica and a suitable destination are available. Downloads can continue if every chunk has a valid reachable copy.
- On worker return, reconcile its inventory and safely remove surplus replicas or data pending deletion. Never remove a needed valid copy before verifying replacement durability.
- Delete objects logically immediately and persist physical cleanup for unavailable workers. Deleted objects must not reappear when workers return.
- Use bounded-memory chunk processing, bounded transfer concurrency, and chunk-verified streaming downloads. Avoid unnecessary full-object temporary copies. Account for multipart upload spooling and disk exhaustion; remove partial uploads safely.
- Verify each chunk before returning its bytes. If a later chunk becomes unrecoverable after a response begins, terminate the incomplete transfer; a new JSON error/status cannot replace an already-started response. Clients should verify the completed download's size and checksum.
- Support arbitrary binary file types and larger files as resources permit; do not impose a 100 MB limit merely because it is the primary demonstration size.
- Target 100 MB for the main demonstration and up to 500 MB for larger-file tests. Use smaller boundary tests as well. With approximately 5 GB free disk and roughly 8 GB or more RAM reported by the user, do not plan 1–2 GB demonstrations. Two replicas of a 2 GB object alone require about 4 GB, excluding source/download copies, temporary files, images, database, and logs. Delete test objects between runs as needed.
- Replication factor two protects against a single worker loss when replicas are healthy; arbitrary simultaneous loss of two workers may lose both copies of a chunk. The controller and metadata database remain availability limitations.
- Work is planned in phases, with an expected overall effort of 3–4 days but no daily schedule. Frontend remains last, potentially after submission.

### 2026-11-13 — Finalize planning decisions and begin Phase 1 discussion

User request: update this file with decisions reached in discussion, accept the 100–500 MB demonstration range, and explain what Phase 1 must lay out before coding.

Changes made:
- Updated the original proposed stack and phase labels to reflect the agreed baseline.
- Recorded finalized local deployment scope, service boundaries, REST-only baseline, upload guarantees, automatic recovery, returning-worker reconciliation, deletion semantics, streaming behavior, resource constraints, and demonstration sizes.
- Replaced tentative design alternatives with finalized decisions. Detailed API contracts, database schema, state transitions, placement/concurrency rules, timing defaults, exact chunk-size byte interpretation, and acceptance scenarios remain Phase 1 work; no tentative values are recorded as final decisions.
- Retained the dashed separator and exact code-file inventory. No application code or additional project files were created or changed by this prompt.

Validation: reviewed the existing record against the planning conversation and inspected the resulting Git diff/status. No application tests apply to this documentation-only change. No commit or push was made.


### 2026-11-13 — Complete Phase 1 and implement the backend

User request: handle Phase 1 independently and “code it all up.” After clarification, the user explicitly selected proceeding through backend implementation too. Custom frontend remains deferred.

Finalized implementation details:
- Decimal chunk size of 1,000,000 bytes; UUID4 object/chunk identities; SHA-256 at chunk and object level; empty objects use zero chunks.
- Four relational tables: objects, workers, chunks, replicas. Lifecycle/replica states are durable recovery intents; no separate broker or task service.
- Worker heartbeats every 2 seconds, availability expires after 10 seconds, maintenance sleeps 3 seconds between passes, and rotating scrubbing selects up to 16 chunks per pass.
- Worker selection uses lowest recorded replica count and stable worker-ID tie breaking. Successful upload requires two durable acknowledgments per chunk.
- Two simultaneous upload ingestions, four active downloads, 1 MB byte buffers, disk-spooled multipart uploads, free-space guards and explicit ENOSPC handling.
- One controller process only. Per-object locks serialize downloads, PATCH, deletion, and repair. Deletion waits for an existing download before committing invisibility; after 204, new reads return 404. This refines the prior shorthand “immediate” deletion.
- Metadata is a map of up to 100 string pairs; keys 1–128 characters and values at most 2048. PATCH merges keys. Duplicate object names are supported; bytes cannot be overwritten through the client API.
- Worker data and manifests are fsynced and atomically renamed. Idempotent internal writes/deletes support retries. Pending destinations are stored before HTTP calls, enabling cleanup after lost acknowledgments.
- Controller restart retires incomplete uploads, resumes durable cleanup, and preserves available objects. Returning-worker reconciliation uses retained replica records, verification, and safe removal of extras.
- Upload retries after an ambiguous client response may create another object (no client idempotency key). Mid-download unrecoverable chunks terminate the stream. Single-host/controller/database availability limits are documented.

Changes made:
- Created Python controller, shared utilities, database models/session factory, object-storage coordinator, and worker service.
- Added the initial Alembic migration and migration runner configuration.
- Added Dockerfile, six-service Compose deployment with health checks and persistent volumes, locked runtime/development dependencies, Python test configuration, and Git/Docker ignore files.
- Added behavioral tests using actual ASGI controller/worker apps and controlled network failures; SQLite is used only in these fast tests. The actual Docker deployment uses PostgreSQL.
- Added a repeatable Python client/demo for 100–500 MB uploads, checksum-verified downloads, distribution/replication, metadata changes, worker stop/start, intentional corruption of its own object, physical repair verification, controller restart, and cleanup.
- Added docs/validation.md with actual verification evidence, and README.md and docs/design.md covering startup, API use, design decisions, schema, request/lifecycle flows, failure and concurrency behavior, resource limits, and acceptance criteria.
- Created a local .venv (ignored), installed pinned dependencies, built the Docker image, and started project containers/volumes for verification. These are local development actions; no external deployment occurred.
- Updated this record and the exact source-file inventory below. Supplied PDF remains unchanged. No frontend was built.

Validation: 25 behavioral tests passed in 3.81 seconds; Python sources compiled; Docker build/start succeeded with all six services healthy; real 100 MB and 500 MB fault demonstrations passed including worker failure, replica restoration, return reconciliation, corruption fallback/repair, and cleanup. The 500 MB demonstration additionally restarted the controller and verified a complete checksum-matching download afterward. Alembic comparison of the live PostgreSQL schema against models returned no differences. Full results are recorded in docs/validation.md. Sandboxed localhost/thread operations required approved execution outside the sandbox. No commit or push was made.



### 2026-11-13 — Review and complete Phase 2 foundation

User request: start working on Phase 2. The previously authorized backend implementation already included the Phase 2 infrastructure; this prompt reviews that foundation explicitly and adds a reproducible acceptance check rather than duplicating services.

Changes made:
- Added `scripts/check_foundation.py`, a read-only Docker/PostgreSQL/worker foundation check using only the host Python standard library and the dependencies already in the controller container.
- Updated README.md with the Phase 2 check command, its coverage, and its read-only scope.
- Updated docs/validation.md with observed Phase 2 results and the distinction between mounted-volume inspection and the earlier persistence demonstrations.
- Updated this prompt log and code-file inventory. No application source, schema, dependencies, Docker configuration, stored objects, or running container state was modified.

Validation: `docker compose config --quiet` passed. The new check passed for all six healthy services, six separate writable named volumes, PostgreSQL connectivity/current Alembic revision/schema consistency, all four worker DNS/identity checks, registration, availability, and advancing heartbeat timestamps. Phase 2 foundation is complete. Existing 25-test and 100/500 MB demonstration results remain historical evidence; they were not rerun for this inspection-script/documentation change. No commit or push was made.


### 2026-11-13 — Review Phase 3 normal storage operations

User request: proceed to Phase 3. The authorized backend already contains the storage implementation; this prompt adds targeted acceptance coverage and checks that implementation against Phase 3's completion criteria.

Changes made:
- Added `scripts/check_storage.py` for repeatable normal-operation acceptance against the local Docker/PostgreSQL deployment. It creates empty, one-byte, exact-chunk, partial-final-chunk and 100 MB objects, checks chunk ordering/size/checksum and two distinct replicas, verifies metadata GET/PATCH and streamed downloads, exercises duplicate filenames and listing, and compares controller records to checksum-verified worker inventory APIs.
- The new script optionally accepts a main test size of 1–500 MB, generates/hashes files in bounded chunks, removes only the objects it creates, and waits for their chunk files to disappear. It does not stop containers or corrupt data.
- Added three behavioral tests for independent duplicate filenames/pagination, metadata PATCH preserving chunk layout/creation time, and content-type/Unicode attachment-filename headers.
- Updated README.md with the Phase 3 acceptance command, prerequisites, coverage and cleanup behavior; updated validation evidence and this inventory.
- No application implementation, database schema, runtime dependencies or Docker configuration changes were needed for the observed normal-operation checks.

Validation: the expanded suite passed all 28 tests in 4.29 seconds. The live Docker/PostgreSQL acceptance script passed all six sizes (0 bytes through 100 MB), independent replica inventory verification, listing and test-data cleanup. Phase 3 normal storage acceptance is complete; results are recorded in docs/validation.md. No commit or push was made.


### 2026-11-13 — Complete Phase 4 lifecycle correctness

User request: proceed to Phase 4 (interrupted uploads, retries, deletion, concurrent operations and durable cleanup).

Findings and finalized changes:
- Regression tests exposed two bugs: abandoned `uploading` intents could remain until a controller restart, and delayed PUTs could recreate physical chunk files after final deletion.
- Updated `object_store/service.py` so maintenance re-reads state under the object lock and converts inactive `uploading` intents to cleanup work. Active uploads remain protected by their lock. Upload exception handling logs a failed database cleanup update without masking the original failure/cancellation; maintenance recovers after the database becomes available.
- Final object cleanup now creates destination intents for every registered worker and sends `X-Delete-Permanent: true`. This also covers previously removed surplus locations, preventing late retries from creating untracked copies. Ordinary repair/surplus deletion remains non-permanent.
- Updated `object_store/worker.py` to atomically persist an empty `.deleted` marker before final removal, reject PUTs for marked chunks with 409 and GETs with 404, preserve markers across restart, and finish interrupted marked deletions at startup. Markers are retained indefinitely; they contain no object data but consume filesystem metadata/inodes. If a marker cannot be persisted because storage is full, cleanup stays pending until storage recovers.
- Added nine regression/concurrency checks in `tests/test_lifecycle.py`; updated the disk-full check in `tests/test_store.py` to require durable pending cleanup followed by recovery when writes become possible.
- Added `scripts/check_lifecycle.py` for live offline deletion/controller-restart/worker-return/late-PUT validation. It modifies only its own test object, restores the worker and confirms cleanup in a finally block; container restarts temporarily affect service availability.
- Updated README.md, docs/design.md and docs/validation.md with lifecycle commands, revised recovery behavior, permanent-delete header, marker retention and actual evidence. Updated this record and inventory.
- Rebuilt/restarted the local application containers with the fixes; PostgreSQL schema and dependency files were unchanged. No frontend changes.

Validation: the two initial regression failures were reproduced before fixing them. The full suite then passed 37 tests in 6.31 seconds. Docker rebuild/start passed with six healthy services. The live lifecycle acceptance passed: deletion survived controller restart while a worker was offline, cleanup finished after its return, and all four workers rejected delayed PUT retries with 409 without recreating bytes. The test object was cleaned up and the stopped worker restored. Phase 4 is complete. No commit or push was made.


### 2026-11-13 — Phase 5 worker failure and recovery

User request: proceed to Phase 5 (failure detection, alternate-replica reads, repair and returning-worker reconciliation).

Findings and changes:
- Reproduced three failing regression checks: post-expiry heartbeat and explicit re-registration left stale replicas healthy, and failed network reads were classified as corruption.
- Updated `object_store/controller.py`: on existing-worker registration or a heartbeat after expiry, reset only previously healthy replica rows to pending in the registration transaction. Ordinary heartbeats retain verified state; already-bad copies remain bad.
- Updated `object_store/worker.py`: explicitly register on every worker-process startup, then send heartbeats; retain the unknown-worker heartbeat fallback to registration. This covers restarts shorter than the heartbeat timeout.
- Updated `object_store/service.py`: scrubbing/repair labels network/server failures as pending verification rather than proven corruption, preserving prior bad evidence. Successful reads used to select retained surplus copies now mark those replicas healthy.
- Added nine tests in `tests/test_recovery.py`: stale-heartbeat return, explicit re-registration, network-versus-corruption distinction, ordinary heartbeat behavior, fallback/upload placement before timeout detection, unavailable destination capacity followed by repair, no available source followed by recovery, missing data on a returning worker outside the scrub batch, and the exact heartbeat-expiry boundary.
- Added `scripts/check_recovery.py`: bounded file generation and streamed verification, two worker stop/start cycles, heartbeat detection, repair to two live replicas, physical checksum/location checks on live workers, return reconciliation to exactly two replicas, and cleanup/restoration in a finally block. Defaults to 10 MB; accepts 2–500 MB. It does not inject corruption.
- Updated README.md and docs/design.md with the Phase 5 acceptance command and return/replica-state semantics. Updated this record and code inventory; final live evidence is recorded in docs/validation.md.
- Rebuilt local application containers to deploy the changes for verification. No database schema/dependency/frontend changes and no Git commit or push.

Validation: initial regression failures were reproduced before fixes; the expanded full suite passed 46 tests in 7.99 seconds. The pending live check completed on 2026-11-14: a 10 MB object survived two successive worker stop/start cycles with matching downloads, physical verification of two live replicas after repair, return reconciliation, and cleanup. Phase 5 is complete; details are recorded in docs/validation.md.


### 2026-11-14 — Phase 6 data integrity and pending Phase 5 verification

User request: proceed to Phase 6. Completed the pending Phase 5 live recovery run before the Phase 6 live demonstration; both phases' implementation work remains part of the authorized backend scope.

Changes made:
- Reproduced worker validation gaps for a manifest claiming a chunk over 1 MB, a boolean size value, and an oversized manifest read.
- Added `ChunkDisk.load_manifest` in `object_store/worker.py` and used it in PUT/GET. It bounds manifest reads to 4,097 bytes, rejects manifests over 4,096 bytes, requires a dictionary, a non-boolean integer size in 1..1,000,000, and a lowercase 64-hex SHA-256. Malformed/incomplete worker data continues to produce 409 so the controller can use another copy and repair it.
- Added object/chunk/worker diagnostic logging in `object_store/service.py` when foreground reads or background checks prove a replica missing or invalid. Network failures retain the Phase 5 pending/bad distinction.
- Added `tests/test_integrity.py` with 14 parameterized cases: strict manifest/chunk bounds, bit flips, truncation, appended bytes, missing/malformed/checksum-damaged manifests, consistently altered worker data plus its local checksum, rotating scrubbing beyond the first batch without downloads, preserving both unusable copies without propagating them, terminating a later-chunk failure without emitting corrupt bytes while releasing the lock, and rejecting an incoming checksum mismatch.
- Added `scripts/check_integrity.py`: defaults to a 10 MB private test object (optional 2–500 MB), briefly pauses the controller during each controlled corruption injection, resumes it, verifies original download hashes and exact physical replicas, and cleans up its own object. Demonstrates byte corruption, invalid manifest, and worker-local checksum agreement with incorrect bytes; the manifest case waits for background repair before any download.
- Updated README.md, docs/design.md, docs/validation.md and this log/inventory with commands, bounds, evidence, and scope. Completed the pending Phase 5 live run and recorded its two successful worker-failure cycles and cleanup.
- Rebuilt the local application containers for Phase 6 live verification. No database schema, dependency or frontend changes. No commit or push was made.

Validation: all 60 automated tests passed in 10.94 seconds. Phase 5 live recovery passed and restored all workers/cleaned its test object. The Phase 6 live check passed on a 10 MB object: worker-detected byte/manifest corruption, controller-authoritative handling of consistently altered bytes/local checksum, background manifest repair without a download, and exactly two physically verified original copies after each injection. The controller was resumed and the test object cleaned up. Phase 6 is complete; evidence is in docs/validation.md. No commit or push was made.


### 2026-11-14 — Phase 7 submission completion

User request: move to Phase 7 and complete the entire project. Scope is the required backend submission (Phases 1–7); the previously deferred optional Phase 8 custom frontend remains outside the submission.

Changes made:
- Audited the implementation against every section of the supplied PDF, including topology, chunking, metadata, client/internal APIs, persistent storage, failure handling, integrity and required demonstrations.
- Added explicit OpenAPI multipart upload, metadata PATCH and binary-download contracts in `object_store/controller.py`, and binary PUT request documentation in `object_store/worker.py`, making the generated API UI usable for those operations.
- Updated `object_store/common.py` so generated validation-response documentation uses the implemented JSON 400 response instead of FastAPI's default 422. Runtime validation semantics remain unchanged.
- Added `tests/test_api_docs.py` with two API-specification checks, including an actual invalid-list-request status check. Full suite now contains 62 cases.
- Changed `scripts/demo.py` physical-read verification to call the worker HTTP API, avoiding construction of ChunkDisk (whose startup cleanup is mutating) merely to inspect a live worker.
- Added `scripts/verify_fresh.py`: builds a random isolated Compose configuration from the existing service definitions, removes fixed container names, assigns unique volume/network names and an ephemeral loopback port, verifies fresh startup/migrations and 100 MB plus final-partial-chunk storage, failure/repair, corruption, controller restart and deletion, and optionally repeats a 500 MB plus tail round trip. Re-resolves the ephemeral published port after controller restart and bounds readiness requests to five seconds. Finally removes only its own generated project's containers/network/volumes. The normal deployment is preserved. Uses the already-built application image.
- Added `scripts/build_submission.py`: explicitly selects source/docs/brief files, creates a deterministic ZIP with an internal SHA-256 manifest and external archive checksum, then verifies every archived byte. Excludes Git history, virtual environments, environment files, caches, test objects and volumes.
- Added `docs/submission.md` with submission contents, setup, presentation sequence, requirement mapping, packaging/fresh verification commands, expected questions and limitations. Added the generated controller specification at `docs/openapi.json`.
- Updated README.md, validation evidence and this log/inventory. Updated .gitignore/.dockerignore for dist output and nested Python caches. No dependency/schema/custom frontend changes; supplied PDF unchanged.
- Rebuilt the normal local deployment and ran the isolated fresh-deployment verification. Packaged source is a local artifact, not a remote publication. No Git commit or push was made.

Validation: final full suite passed 62 tests in 11.84 seconds, and all 62 passed again from an extracted source ZIP in 10.93 seconds. The packager verified all 39 archive members. The first isolated run passed storage/failure/corruption checks but exposed a verifier assumption about an ephemeral host port remaining stable after restart; fixed port re-resolution and bounded readiness requests, then reran from fresh volumes. The clean rerun passed all fresh-deployment checks, including 100,000,019-byte failure/corruption/restart verification and a 500,000,019-byte round trip, completed object cleanup and removed only its temporary project. Final ZIP was regenerated with completed evidence and all 39 members verified. Phases 1–7 are complete and the source submission is ready; optional Phase 8 remains deferred. Final results are recorded in docs/validation.md. No commit or push was made.


### 2026-11-15 — Reliability upgrade Phase 1: worker identity and safe rejoining

- User authorized a safety checkpoint, created as `b9fcf1e`, before upgrades. This prompt authorizes only new Phase 1; no additional commit or push was made.
- Followed the selected other-team A2/B3/A5 design for persistent storage UUID, per-start boot UUID, registration/readiness, inventory-based eligibility, authoritative checksum comparison and safe excess-copy handling. Retained Python, PostgreSQL and Docker Compose.
- Added additive migration 0002. Existing metadata survives; legacy membership and replicas require reconciliation after upgrade.
- Added controller membership registry, durable eligibility/version fields, actual-process identity probes, old-boot rejection, changed-volume rejection, inventory reconciliation/retries, missing/damaged copy classification, discovery of extra known copies, and permanent cleanup of unknown UUIDs. Inventories cannot overwrite trusted hashes or publish deleting objects.
- Returning/expired workers cannot supply reads or accept new placements until reconciled. Controller requests name the expected storage/boot incarnation; mismatched targets and replies from superseded memberships are rejected. Full operation sequencing/receipts remain Phase 2.
- Workers persist service-bound identity outside chunk manifests, expose storage/boot IDs in health/inventory, report both IDs, and expose readiness based on controller-confirmed eligibility. Compose worker health now uses readiness.
- Added migration, identity, inventory, failure, eligibility and race tests; adapted existing heartbeat fixtures and checks; kept the arbitrary-URL test exercising the allowlist with otherwise-valid identity fields. Extended the real recovery checker to assert stable storage identity and fresh boot identity across restart.
- Validation: 74 tests passed (19.48s; two existing Alembic path_separator deprecation warnings), corrected allowlist test passed separately, PostgreSQL foundation/schema checks passed, and two real 10 MB Docker failure/rejoin cycles passed with stable-storage/fresh-boot assertions. All stopped workers were restored and test chunks cleaned up.
- Updated README/design/API export and validation evidence. Upgrade Phase 2 remains local scans/progress, damage reporting, serial/coalesced repair, durable write records and full stale-operation protection; Phase 3 remains operational CLI, broader boundary coverage and submission refresh. The previously generated archive is not refreshed in this phase.


### 2026-11-15 — Complete reliability upgrade Phase 2

- Implemented selected TEAM-PLAN B2/B4/A5 concepts: durable worker catalog/write receipts, worker-local fixed-order integrity scans, persistent progress, acknowledged/repeated damage reports, coalesced persistent serial repair scheduling, and operation/current-intent protection. Python's built-in SQLite provides each worker's local catalog inside its existing volume; PostgreSQL remains controller authority and no container was added. User was informed of this technology choice.
- Excluded worker SQLite catalogs and journal/WAL sidecars from Git and Docker build context.
- Added migration 0003 for operation sequencing/latest intents, repair jobs, damage deduplication receipts and worker scan progress. Existing objects survive; workers reconcile trusted expectations before becoming eligible after upgrade.
- Worker catalog records pending mutations before data changes and completion receipts afterward. Lower/conflicting sequences are rejected, interrupted publication/deletion is recovered, and tombstones forbid resurrection at every sequence. Trusted catalog hashes are preserved even when bytes and manifest are consistently altered.
- Scans persist fixed-cycle boundaries and cursor/check counts, report damage before advancing, and retain events until acknowledgment. Controller acknowledgments follow durable deduplication/queue commits; obsolete operation reports cannot invalidate replacement health.
- Replaced routine controller checksum scrubbing with worker-local scans. Controller coalesces repair triggers by chunk, processes at most 32 jobs serially, retains retry/error state, independently verifies replacement bytes, and checks object lifecycle and repair revision before mutation/publication. Explicit full_scrub remains an administrative/test verification mode.
- Added controller/worker inspection fields and APIs for scan progress, outstanding damage and queued repair. Updated live integrity checks for catalog-based consistent-corruption rejection and proof of worker-local damage retention while the controller is paused. Added fault/restart/race tests and adapted existing integrity/migration tests to the stronger contract.
- Validation: 86 tests passed (60.29s, two existing Alembic configuration warnings); Docker/PostgreSQL migration/schema checks, worker-local damage/corruption repair, two real worker failure/rejoin cycles, a 100 MB storage round trip with physical-replica checks, and controller-restart/offline-deletion cleanup with late-PUT rejection passed. Test objects were cleaned up and stopped/paused services restored.
- Updated README, design, API export, submission guidance and validation evidence. General operational CLI, broader boundary matrix and regenerated submission archive remain upgrade Phase 3. No commit or push was authorized or performed in this phase.


### 2026-11-15 — Commit completed reliability upgrades

- User authorized committing the completed reliability upgrade Phases 1–2 together, including migrations, worker identity/catalog/scanning, durable repair protocol, tests and documentation.
- Recorded this authorization and checked the diff for whitespace errors before committing. No application code changed in this prompt; the previously recorded 86-test and Docker validation results apply.
- Phase 3 is explicitly deferred. No push was requested or performed.


### 2026-11-15 — Complete reliability upgrade Phase 3

- User requested the remaining Phase 3 with a clean, attractive CLI rather than heavy separators/decorations. Added a plain-table terminal client using standard-library argparse and existing HTTPX; no additional dependencies, frontend or containers.
- Added upload, streamed verified download, paginated list/all-pages, metadata, custom metadata PATCH, DELETE, cluster status, chunk placements, scan progress and repair inspection. Added JSON automation output, controller URL/environment configuration, bounded connection/transfer timeouts, useful errors/exit codes and TTY-only download progress. Transfers report elapsed time.
- Downloads verify size/SHA-256 against metadata in an adjacent temporary file before atomic publication; default refuses existing or concurrently created destinations, --force replaces only after verification, and failures/cancellation remove temporary files. Uploads are not automatically retried because server upload idempotency remains a separate enhancement.
- Added executable `store` launcher that selects the project virtual environment without changing the caller's directory, plus a detailed CLI guide and a live checker covering every command.
- Expanded automated and live storage boundaries to 0, 1, C−1, C, C+1, 2C, 2C+1 (C=1,000,000), retaining the 2.5 MB case. Added 21 CLI tests covering wire contracts, metadata, inspection, pagination, JSON/errors, sanitization, corruption/interruption handling, overwrite safety, empty files, maximum-length destination filenames and malformed controller URLs.
- The first final fresh-deployment run passed 100/500 MB data checks but hit the verifier's fixed 180-second physical-cleanup deadline on 501 chunks. Made that bounded deadline scale with chunk count (at least 180 seconds, otherwise 2 seconds per chunk); no backend durability behavior was relaxed. Temporary deployment resources were removed by finally.
- Validation: final packaged source passed 110 tests (94.14s; two existing Alembic configuration warnings); all CLI commands passed the live checker; the full chunk-boundary matrix passed against real worker files; extracted launcher permissions/help were verified.
- The corrected final fresh-deployment verification passed 100,000,019-byte fault/restart/recovery checks and a 500,000,019-byte round trip, including completed physical cleanup. Its temporary containers/network/volumes were removed and the existing deployment preserved.
- Updated source packaging to include the launcher with executable ZIP permissions. Updated README/design/submission instructions and validation evidence; regenerated submission packaging after verification (51 checked ZIP members: 50 source/document files plus the checksum manifest). No new commit or push was requested or performed.


### 2026-11-15 — Commit completed reliability upgrade Phase 3

- User explicitly authorized committing the completed Phase 3 CLI, expanded boundary coverage, verification harness updates and documentation.
- Recorded the authorization and checked the diff for whitespace errors. No application code changed in this prompt; the recorded 110-test packaged suite and successful fresh 100/500 MB verification remain the validation evidence.
- Refreshed the ignored submission archive to include this documentation entry. No push was requested or performed.

---

## Exact code-file inventory

### Application source

- `object_store/catalog.py`: worker SQLite persistence. `Catalog.__init__` creates chunk expectations, latest operation receipts, damage outbox and singleton scan progress; `connect` provides FULL-synchronous transactional connections; `expected`/`expect`/`state` manage immutable expected content and placement state; `begin`/`finish`/`operation` persist sequence/signature/pending/completed receipts and reject stale/conflicting retries; `damage` coalesces per-chunk/sequence events; `acknowledge` removes acknowledged event UUIDs; `report` returns progress and a bounded outbox batch; `scan_ids` fixes a cycle ceiling and selects the next bounded range; `scanned` and `complete_cycle` persist progress. Uses sqlite3, JSON, UUIDs and HTTPException.
- `object_store/repair_queue.py`: `enqueue` creates/coalesces a job per chunk and optionally increments its revision; `current` validates object lifecycle and a captured repair revision; `needs_repair` checks replica count, health and worker eligibility/freshness. These helpers operate inside caller-owned SQLAlchemy transactions.
- `object_store/cli.py`: operational CLI. `parser` defines ten commands, common URL/timeout/JSON options and command arguments; `identity`, `positive`, `metadata_pairs` validate inputs; `clean`, `size`, `table` format safe readable output; `check` translates HTTP errors; `execute` issues public API requests, streams upload files and paginates lists; `download` streams into an adjacent temporary file, checks authoritative size/SHA-256, fsyncs and publishes with no-overwrite link or explicit atomic replacement, cleaning partials on failure; `render` formats objects, placements, status, scans and repair tables; `main` owns HTTPX lifecycle, TTY progress, output/error channels and exit codes, with injectable transport/streams for tests. Uses argparse/hashlib/json/mimetypes/os/pathlib/sys/tempfile/time/uuid and HTTPX.
- `object_store/__init__.py`: package marker and package docstring; no runtime behavior.
- `object_store/common.py`: decimal `CHUNK_SIZE` and replication constant; `checksum(bytes)` returns SHA-256; `canonical_id` parses/normalizes UUIDs; `validate_metadata` enforces custom string-map constraints; `install_errors` installs FastAPI handlers for HTTP/validation, SQLAlchemy, filesystem, and unexpected exceptions, returning consistent JSON errors. SQLite catalog errors also receive storage-unavailable 503 responses. Its nested openapi_with_validation_errors wrapper documents the actual JSON 400 validation response instead of the generated 422 default. Depends on hashlib, uuid, FastAPI and SQLAlchemy exception types.
- `object_store/membership.py`: `Registry` handles boot-aware registration and heartbeat reports; `matches` compares service/storage/boot identity; `current` checks durable membership version and freshness; `reconcile` retries ineligible fresh workers; `reconcile_one` validates complete inventories against authoritative metadata under object locks, discovers known extra copies, marks absent/damaged copies, cleans unknown UUIDs, and enables only the still-current incarnation. Registration probes the actual worker, rejects/revokes changed-volume membership, and invalidates replica state before rejoining. It also commits damage receipt deduplication/current-operation filtering and repair enqueue before acknowledgment; reconciliation sends bounded trusted expectation batches and enqueues verification jobs. Uses HTTPX, SQLAlchemy, asyncio per-worker locks, UUID parsing and FastAPI errors.
- `object_store/models.py`: `now` produces UTC timestamps; SQLAlchemy `Base`, `StoredObject`, `Worker`, `Chunk`, and `Replica` declare the relational schema, fields, foreign keys, unique chunk positions, composite replica identity, and allowed lifecycle states. Worker includes persistent storage/boot identities, eligibility and a durable membership version; storage UUID is unique. Additional OperationClock, WorkerOperation, RepairJob and DamageReceipt models persist sequencing, latest mutation intents, cascading per-chunk queue rows and damage deduplication; Worker also stores last reported scan_progress JSON. No binary payloads are stored here.
- `object_store/database.py`: `make_database` constructs an engine and non-expiring session factory from DATABASE_URL; PostgreSQL uses psycopg and bounded connection/pool waits. SQLite test engines enable foreign keys and share the in-memory database through StaticPool. Depends on SQLAlchemy and environment configuration.
- `object_store/service.py`: `Store` owns object/download/upload/maintenance locks and semaphores, sessions, HTTPX client and worker timeout. `reachable` checks heartbeat age; `live` additionally requires reconciled eligibility; `available` rejects invisible objects; `request` performs retryable internal HTTP calls with target incarnation/operation headers, persists increasing mutation sequence/intents, and discards superseded-membership or mutation replies; `fetch` verifies/falls back only between eligible fresh replicas; `place` persists destination intents and writes two copies, independently reads replacement bytes, and validates current object/repair revision before publication; `upload` chunks and publishes or schedules cleanup; `metadata`/`summary` serialize object records; `delete_object` commits logical deletion; `cleanup` records all registered worker destinations, persists permanent deletion via worker headers, and removes acknowledged physical copies/rows; `repair` validates and restores replicas, distinguishes pending network failures from proven bad copies, records successful surplus-selection verification, and safely trims extras; `recover_startup` retires interrupted uploads; `maintenance` first retries worker inventory reconciliation, then re-reads lifecycle state under locks, reclaims inactive uploading intents without requiring restart, and coordinates cleanup and persistent serial repair jobs (at most 32 per pass); routine integrity scans now run locally on workers. Upload failures preserve their original exception when writing the cleanup marker also fails due to a database error. Proven missing/invalid foreground and scrubbed replicas are logged with object/chunk/worker identifiers. Inputs are UploadFile/chunk/object IDs and metadata; outputs are metadata, verified bytes, or explicit HTTP errors. Uses models/common helpers, SQLAlchemy, HTTPX, asyncio, hashlib and UUIDs.
- `object_store/controller.py`: `UploadBudget` ASGI middleware gates multipart ingestion and checks spool space; `WorkerReport`, `DamageReport` and `ScanProgress` validate bounded membership/damage/progress reports; `create_app` assembles FastAPI, database, HTTP client, Store, lifespan and maintenance supervision. Nested handlers implement health, allowlisted registration/heartbeats delegated to Registry with required storage/boot UUIDs, status (including scan progress and up to 100 repair jobs), upload, paginated list, metadata GET/PATCH, DELETE and download. Download's `LockedStream` releases locks/slots even on disconnect and streams verified chunks with size/checksum headers. Route decorators also specify multipart upload, JSON PATCH and binary-download OpenAPI contracts. Depends on FastAPI/Pydantic, Starlette multipart/response types, HTTPX, SQLAlchemy, common/database/models/service modules, and environment settings.
- `object_store/worker.py`: `ChunkDisk` creates persistent storage, removes abandoned temporary writes, constructs UUID-only paths, performs atomic/fsynced `put`/`atomic`, uses `load_manifest` for bounded JSON/schema/size/SHA-256 validation in PUT/GET, validates chunk bytes in `get`, performs repeatable durable `delete` with optional permanent markers, and exposes checksum-verified `inventory`. Permanent deletion writes empty `.deleted` files before unlinking data, rejects subsequent PUT/GET, and finishes interrupted marked deletions at startup; ordinary repair deletes permit rewriting the same chunk ID. The DELETE endpoint accepts optional boolean X-Delete-Permanent. `create_app` builds worker FastAPI, serialized threaded filesystem handlers, health, PUT/GET/DELETE/inventory endpoints, and lifespan-managed reporting that registers explicitly at worker startup before heartbeats, with re-registration on an unknown-worker response. `storage_identity` atomically persists and validates the service-bound volume UUID; create_app generates a fresh boot UUID. Reports and inventories include both IDs, `/ready` requires recent controller-confirmed eligibility, and target-incarnation middleware rejects mismatched controller requests. Phase 2 adds a Catalog to ChunkDisk, immutable expected hashes during PUT/GET, `mutate` for sequence/signature/receipt enforcement, `recover_operations` for interrupted publication/deletion, `scan_batch` for durable local scan progress and outbox events, and `reconcile_expected` for controller-provided expectations. The report loop includes retained damage/progress and removes only acknowledged events; a lifespan-managed scanner runs bounded passes. Mutation routes accept operation-sequence headers; POST /internal/chunks/reconcile accepts at most 100 expectations; GET /internal/scan exposes progress/outbox. The PUT route documents its binary request body in OpenAPI. Inputs are raw chunks/checksum headers and UUIDs; outputs are verified bytes, acknowledgments, inventory or errors. Uses filesystem/hash helpers, HTTPX, asyncio and Starlette threadpool support.

### Migrations, demonstration, and tests

- `migrations/env.py`: Alembic environment entry point; obtains the configured engine, binds Base metadata, runs migrations transactionally, and disposes the engine. Depends on Alembic and database/models modules.
- `migrations/versions/0003_repair.py`: additive 0002-to-0003 migration creates operation_clock (seeded at zero), latest worker_operations, cascading repair_jobs, damage_receipts and nullable worker scan progress; invalidates membership/replica health for initial trusted-catalog reconciliation. Downgrade drops the new tables/column.
- `migrations/versions/0002_membership.py`: additive upgrade from 0001 creates nullable storage/boot UUID columns, unique storage identity, eligibility and membership version; demotes legacy replicas to pending. Downgrade removes the added constraint/columns. Batch operations support PostgreSQL and SQLite migration tests.
- `migrations/versions/0001_metadata.py`: immutable initial revision `0001`; `upgrade` explicitly creates four tables with columns, indexes and constraints matching the models; `downgrade` drops replicas, chunks, workers, then objects. Depends on Alembic operations and SQLAlchemy types.
- `scripts/verify_fresh.py`: isolated fresh-deployment verifier. `run` executes bounded subprocesses and surfaces failures; `main` transforms resolved Compose JSON into a unique temporary project with independent volumes/network, no fixed container names and an ephemeral host port. Its nested compose/get_json/metadata/replicated/download helpers build/start/query the deployment, verify 100 MB plus a partial final chunk, failure/repair/corruption/controller restart (re-resolving the ephemeral port) and cleanup, optionally repeat a 500 MB plus tail round trip, use a chunk-count-scaled physical-cleanup deadline, then remove only its own project in finally. Uses argparse/hashlib/json/pathlib/subprocess/sys/tempfile/uuid, HTTPX, check_storage ROOT/require and demo.wait_for.
- `scripts/build_submission.py`: source-only archive builder. ROOT and TOP_LEVEL define the allowed repository inputs including the store launcher; `main` adds allowlisted Python/Markdown/JSON files from source/docs directories, rejects symlink/non-file inputs, constructs an internal MANIFEST.sha256, writes a ZIP with stable timestamps/file permissions to dist, reopens and verifies each member and CRC, then writes dist/SHA256SUMS. Uses only hashlib/pathlib/zipfile; never stages/commits/pushes Git or includes local environments/volumes.
- `scripts/check_integrity.py`: Phase 6 live corruption demonstration. `main` parses a 2–500 MB size (default 10), creates deterministic file data, uploads its own object, pauses/resumes the controller around byte/manifest/consistent-local-hash corruption on a known test chunk, captures catalog-backed worker rejection (including consistently altered bytes/manifest), verifies retained worker-local scan damage while the controller is paused via its nested reported helper, verifies repairs, and deletes its own object in finally. Nested `metadata`, `download` and `repaired` helpers query controller metadata, verify streamed whole-file size/SHA-256, and compare physical worker inventory hashes/sizes/counts. Uses HTTPX, argparse/hashlib/json/pathlib/tempfile, check_lifecycle.docker, check_storage.inventories/require and demo.wait_for; no user objects are intentionally altered.
- `scripts/check_recovery.py`: Phase 5 live acceptance client. `main` parses a 2–500 MB size (default 10), generates deterministic file blocks in bounded memory, uploads and tracks its own object, performs two successive worker stop/start cycles with assertions for stable storage UUID, changed boot UUID and reconciled eligibility, and cleans up in finally. Nested `status`/`metadata` query public APIs; `replicated` checks healthy live counts and optional exact replica count; `verify_download` verifies streamed size/SHA-256; `physical_replicas` queries only live worker inventory APIs through the controller and compares actual checksum/size/locations with metadata. Uses HTTPX, argparse/hashlib/json/pathlib/tempfile, check_lifecycle.docker, check_storage.inventories/require and demo.wait_for. No corruption injection.
- `scripts/check_lifecycle.py`: Phase 4 live acceptance client. `docker` runs bounded Compose commands from the repository; `main` creates a private small binary object, stops a replica holder, checks logical deletion and repeated DELETE/PATCH behavior, inspects its durable state through an embedded read-only SQLAlchemy query, restarts the controller, restores the worker, confirms physical cleanup, and sends late PUTs from the controller to every worker expecting 409. Nested `status`, `absent`, and `pending_state` helpers observe readiness, inventories and database state. Its finally block restores the worker and cleans its own object. Depends on HTTPX, hashlib/subprocess, check_storage ROOT/inventories/require, demo.wait_for, and existing container SQLAlchemy/HTTPX modules.
- `scripts/check_storage.py`: executable Phase 3 storage acceptance client. `require` raises explicit check failures; `inventories` uses bounded Docker subprocess execution to call worker inventory APIs from the controller network; `listed_objects` iterates paginated client listings; `main` selects a 1–500 MB main file size, generates boundary-size and main files with independent expected chunk hashes, checks upload status/metadata/replicas, metadata GET/PATCH, streamed download bytes and headers, list fields and independent IDs, then compares physical inventory locations/hashes and deletes only its own objects in a finally block. Fixed to the local Compose controller so public API and inventory observations refer to the same deployment. Uses HTTPX plus argparse/hashlib/json/pathlib/subprocess/tempfile/time; no fault injection or application configuration writes.
- `scripts/check_cli.py`: live public-API CLI acceptance. Its nested cli helper invokes object_store.cli in subprocesses with JSON output and checks exit status. main creates a temporary binary file spanning two chunks, independently hashes it, exercises upload/list-all/metadata/PATCH/placements/download/status/scans/repairs, and deletes only its object in finally. Uses subprocess, tempfile, pathlib, hashlib, JSON and sys; does not inject faults or stop services.
- `scripts/check_foundation.py`: read-only Phase 2 acceptance command. `docker` runs bounded Docker commands from the repository directory; `require` reports explicit failed conditions; `main` validates Compose service health and distinct writable volumes, then executes an embedded read-only controller check for PostgreSQL dialect, Alembic head, model/schema agreement, worker DNS/identity, registration and heartbeat progress. Embedded `get` performs bounded JSON HTTP reads and embedded `require` validates their results. Exits nonzero with diagnostics on failure. Uses only host json/pathlib/subprocess/sys plus existing container Alembic/SQLAlchemy/urllib/time dependencies; does not change stored objects or restart containers.
- `scripts/demo.py`: executable demonstration/client. `docker` invokes Docker with argument arrays; `wait_for` polls observable conditions with a deadline; `main` parses size/URL/fault flags, creates deterministic binary data in bounded blocks, uploads/patches/streams and verifies downloads, checks distribution, optionally stops/restarts worker2 and corrupts only its own chunk, verifies actual repaired disk data, restarts the controller, and cleans up its own object/temp file. Nested metadata/replication/download/physical-validation helpers support these assertions. Uses HTTPX and standard-library argparse/hashlib/json/pathlib/subprocess/tempfile/time.
- `tests/test_repair_protocol.py`: 12 tests for durable completed receipts, rejected reordered/conflicting operations, permanent tombstones, interrupted PUT/DELETE recovery, restart-resumable fixed-cycle scans, consistent byte/manifest corruption, outbox persistence/deduplication/acknowledgment and cold repair, obsolete damage reports, new repair intent during PUT, deletion of queued repair, legacy expectation adoption, controller sequence preservation and arrivals during a scan cycle. Uses real disks, ASGI worker/controller fixture, SQLite catalog, SQLAlchemy metadata and fault injection.
- `tests/test_membership.py`: 12 tests covering persistent storage/fresh boot identity, volume ownership, unreconciled read/placement exclusion, old boot and changed volume rejection, unknown-copy cleanup and known extra-copy discovery, authoritative hashes despite consistent local corruption, failed inventory retry, restart/re-registration during inventory, target boot request rejection, readiness before registration, and migration preservation of existing worker metadata. Uses real worker disks/ASGI apps, HTTPX failure injection, SQLAlchemy and Alembic.
- `tests/test_cli.py`: 21 tests of the operational CLI using HTTPX MockTransport: object and inspection commands, multipart upload/PATCH contracts, verified downloads, default refusal/explicit replacement, wrong-size/hash and interrupted-stream cleanup, pagination/options placement, input validation, JSON/HTTP/network errors, terminal control sanitization, empty downloads and 255-character destination names. Uses temporary files, in-memory output streams, independent hashes and a failing byte stream.
- `tests/test_api_docs.py`: two checks of generated controller/worker OpenAPI contracts: multipart binary upload and metadata field, string-valued JSON PATCH, binary download rather than JSON, documented/actual 400 validation, and worker binary PUT/checksum header. Uses the existing cluster fixture and actual ASGI OpenAPI endpoint.
- `tests/test_integrity.py`: 14 parameterized integrity cases covering manifest validation/bounded size, six corruption variants and repair, trusted worker-catalog and controller-hash validation against consistently altered bytes/manifest, worker-local scan/report detection without client reads, preservation of unrecoverable copies, safe truncated stream/lock release when a later chunk fails, and incoming wrong-hash rejection. Uses ChunkDisk, actual cluster ASGI apps, pytest/HTTPX, asyncio, JSON/hash helpers, and SQLAlchemy chunk/replica models.
- `tests/test_recovery.py`: nine Phase 5 behavioral tests for return-related replica invalidation, network errors versus corruption, normal heartbeats, pre-expiry failure fallback and placement, repair deferred for capacity, unavailable source recovery, returned missing disk data outside periodic scrub selection, and the ten-second heartbeat boundary. Uses the existing cluster fixture, SQLAlchemy models, datetime timing and monkeypatch-controlled responses/clock.
- `tests/test_lifecycle.py`: nine Phase 4 tests exercising inactive upload recovery, delayed PUT after deletion, active upload invisibility/protection, lost DELETE replies and retries, permanent deletion on previously unlisted workers, persistent markers/startup cleanup, repair-vs-delete lock ordering, download disconnect release, and original cancellation preservation during database outage followed by cleanup recovery. Uses event-controlled requests, actual worker ASGI apps/disk writes, existing cluster fixture, asyncio, pytest, HTTPX and SQLAlchemy models/exceptions.
- `tests/conftest.py`: `Cluster` builds four real worker ASGI apps and an in-memory relational controller; its HTTPX routing transport simulates stopped workers and lost PUT acknowledgments. Identity-bearing report/registration/stop/upload helpers and initial inventory reconciliation and the `cluster` async fixture manage lifespan, sessions and clients. Depends on pytest-asyncio, HTTPX, SQLAlchemy models, controller and worker factories.
- `tests/test_store.py`: behavioral tests for empty/single/exact/final-partial chunks, binary checksums and replica placement, metadata/list/delete, worker loss/repair/return, corrupt chunks and manifests, loss of both copies, offline deletion, insufficient workers, ambiguous acknowledgments, restart/cancel cleanup, invalid metadata/UUIDs, idempotent worker operations/conflicts, concurrent metadata updates, disk-full errors, registration URL rejection, and lock-serialized deletion. Additional Phase 3 tests cover duplicate filenames with pagination, metadata updates preserving chunk layout/creation time, and content-type/Unicode download headers. Expanded chunk-boundary parameterization produces 31 cases in this file; together with test_lifecycle.py, test_recovery.py and test_integrity.py plus test_api_docs.py, the original suite had 62; the membership upgrade brought it to 74; Phase 2 added 12 protocol tests (86 total); Phase 3 adds three boundary cases and 21 CLI cases (110 total). The disk-full case verifies pending cleanup while marker writes fail and completed cleanup after storage recovery. Depends on pytest, HTTPX-backed cluster fixtures, SQLAlchemy models, asyncio, and ChunkDisk.

### Non-Python project files

- `store`: executable POSIX shell launcher; resolves the project directory, selects .venv/bin/python when present (otherwise python3), prepends the project to PYTHONPATH and execs object_store.cli with unchanged arguments and caller working directory.
- `docs/cli.md`: complete CLI command reference, configuration/JSON/exit-code behavior, sample plain output, download safety, upload retry limitation and live verification commands.
- `Dockerfile`: Python 3.12 slim runtime, pinned runtime dependency installation, application/migration copy, worker factory default command. Controller overrides the command in Compose.
- `compose.yaml`: controller, four named workers and PostgreSQL, startup dependencies, worker readiness/controller process health checks, heartbeat/repair defaults, loopback controller port, and six named volumes. No worker/database host port exposure.
- `alembic.ini`: migration directory and repository import path.
- `pyproject.toml`: pytest test discovery, import path and asyncio loop settings.
- `requirements.txt`: fully pinned installed runtime packages, including transitive dependencies.
- `requirements-dev.txt`: fully pinned runtime and test packages.
- `.gitignore`: excludes virtual environment, bytecode/cache, local environment files, databases, temporary output and generated dist archives.
- `.dockerignore`: excludes Git/environment/cache/temp/PDF material, nested Python caches, SQLite catalogs/sidecars and dist artifacts from Docker build context.
- `README.md`: startup/shutdown, curl examples, tests and demo instructions, persistence and limitations.
- `docs/submission.md`: submission contents, setup/presentation commands, requirement-to-evidence mapping, isolated deployment/packaging instructions, explanation prompts and limitations.
- `docs/openapi.json`: exported generated controller OpenAPI specification, including public and registration/heartbeat contracts, upload/PATCH request schemas, binary download and corrected validation status documentation.
- `dist/mini-distributed-object-store.zip` and `dist/SHA256SUMS`: generated source submission and its archive checksum, ignored by Git; ZIP also includes an internal per-source checksum manifest.
- `docs/design.md`: Phase 1 architecture, schema, APIs, lifecycle/recovery/concurrency decisions, resource assumptions and acceptance mapping.
- `docs/validation.md`: actual validation commands/results and limits, updated after the live demonstrations.
- `Mini_Distributed_Cloud_Object_Store.pdf`: supplied project requirements; unchanged.
- `AGENT.md`: collaboration agreements, finalized decisions, prompt change history and this inventory.
