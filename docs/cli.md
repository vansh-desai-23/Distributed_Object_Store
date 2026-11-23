# Command-line client

The `store` client provides object operations and read-only cluster inspection. Start the [Docker deployment](../README.md#quick-start) before using it. All commands call the controller API; clients never connect directly to worker volumes.

## Run the client

From the repository root on Linux or macOS:

```bash
./store --help
```

The shell launcher uses `.venv/bin/python` if present, then falls back to `python3`. If the launcher is not executable, run `.venv/bin/python -m object_store.cli`. On Windows PowerShell, use:

```powershell
.\.venv\Scripts\python.exe -m object_store.cli --help
```

The examples below use `./store`. Replace it with the Python module command on Windows. Create the virtual environment and install `requirements-dev.txt` as shown in the README.

## Object commands

| Command | Purpose |
| --- | --- |
| `upload FILE` | Upload binary data and optional custom metadata |
| `list` | List stored objects |
| `metadata ID` | Show object, chunk, and replica metadata |
| `placements ID` | Show chunk locations and replica health |
| `download ID -o FILE` | Download and verify an object |
| `patch ID --meta KEY=VALUE` | Add or replace custom metadata |
| `delete ID` | Remove an object from the public API |

```bash
./store upload report.pdf --meta owner=student --meta course=cloud
./store list
./store metadata OBJECT_ID
./store placements OBJECT_ID
./store download OBJECT_ID -o downloaded.pdf
./store patch OBJECT_ID --meta owner=updated
./store delete OBJECT_ID
```

Replace `OBJECT_ID` with the UUID returned by upload. Repeat `--meta KEY=VALUE` for multiple fields; values may contain `=`. Upload infers a content type from the filename, and `--type` overrides it. PATCH merges custom fields without changing stored bytes. DELETE makes the object unavailable immediately after its logical deletion commits; cleanup of an offline worker may finish later.

The default `list` page contains up to 100 objects. Use `--limit 1..1000`, `--offset N`, or `--all` for pagination. A multi-page list is a live view, so concurrent changes can alter later pages.

## Cluster inspection

```bash
./store status
./store scans
./store repairs
./store placements OBJECT_ID
```

| Command | What it shows |
| --- | --- |
| `status` | Worker availability, membership, free space, last heartbeat, replication target, and pending cleanup |
| `scans` | Last reported integrity-scan progress and pending damage count per worker |
| `repairs` | Up to 100 queued repair jobs, with reason, revision, attempts, and last error |
| `placements` | Each recorded chunk/worker pair with replica state and availability |

These are status snapshots. A `healthy` replica is last known to be valid; reads and scans provide fresh checksum checks. An empty repair queue does not prove every stored file was checked at that instant.

## Download verification

The client streams a download into a temporary file beside the destination. It checks the exact byte count and SHA-256 against object metadata before publishing the file. A failed or interrupted transfer removes the temporary file. Existing destinations are preserved unless `--force` is given; the client also refuses a destination created by another process during an ordinary download.

```bash
./store download OBJECT_ID -o ./downloads/report.pdf
./store download OBJECT_ID -o ./downloads/report.pdf --force
```

The destination directory must already exist and have enough space for the full object. The server sends `Content-Length` and `X-Object-Checksum`, but a stream can end after HTTP headers have been sent; use the verified CLI or check length and checksum in another client.

## Safe upload retries

An upload is not retried automatically. If the response is lost, retry with the same explicit idempotency key and identical file, name, content type, and metadata:

```bash
./store upload report.pdf --meta owner=student --idempotency-key report-001
# If the response is lost, repeat the same command:
./store upload report.pdf --meta owner=student --idempotency-key report-001
```

The controller returns the original object ID and saved 201 response on a matching retry, including after a controller restart. Changed request data with the same key returns 409. A matching retry after the object was deleted returns 410; use a new key for a new intended object. Keys are case-sensitive, 1–128 characters, and may contain ASCII letters, digits, `.`, `_`, `:`, or `-`. They are retained without expiry in this local system. Retries resend the complete file; this is not resumable upload.

Replayed responses reflect creation-time metadata. Use `metadata`, `placements`, or `status` for current state.

## Configuration and scripts

```bash
./store --url http://127.0.0.1:8000 status
STORE_URL=http://127.0.0.1:8000 ./store status
./store upload report.pdf --timeout 300 --json
./store list --all --json
```

Global options may appear before or after the command. `--url` overrides `STORE_URL`; the default URL is `http://127.0.0.1:8000`. `--timeout` is an inactivity timeout in seconds, not a total transfer deadline; connection setup is capped at five seconds. `--json` writes machine-readable JSON to stdout. Errors go to stderr with exit code 1, invalid arguments use 2, cancellation uses 130, and success uses 0. Run `./store COMMAND --help` for command-specific options.

## Verify the client

```bash
python -m pytest -q tests/test_cli.py
```

Run tests in Linux or a Linux container. The [guided demo](../README.md#run-the-demonstration) also exercises live CLI metadata inspection alongside API and physical-storage checks.
