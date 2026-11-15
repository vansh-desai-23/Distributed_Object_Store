# Operational CLI

Run `./store` from the project directory. The launcher uses the project's `.venv` when present, otherwise `python3`. Install `requirements.txt` first. You can also use `.venv/bin/python -m object_store.cli` (including when a ZIP extractor does not preserve executable permissions).

Output uses plain headings and aligned columns, with no box borders, banners or color dependency. Download progress appears only on an interactive terminal. Transfer summaries include elapsed time. Filenames and metadata are sanitized for human terminal output; JSON preserves the actual values with escaped control characters.

## Object operations

```bash
./store upload './files/report.pdf' --meta owner=student --meta project=cloud
./store list
./store list --all --limit 100
./store metadata OBJECT_ID
./store placements OBJECT_ID
./store patch OBJECT_ID --meta owner=updated --meta 'note=reviewed locally'
./store download OBJECT_ID --output './downloads/report.pdf'
./store delete OBJECT_ID
```

Replace `OBJECT_ID` with the UUID printed by upload. Upload infers a MIME type from the filename; `--type application/octet-stream` overrides it. Repeat `--meta KEY=VALUE` for additional fields; values may contain `=`. PATCH merges custom fields without altering file bytes. Delete immediately removes public visibility; physical cleanup may wait for an offline worker.

Listing returns up to 100 objects by default. Use `--limit 1–1000`, `--offset N`, or `--all` to fetch subsequent pages. Pagination is a live view, not a transactional snapshot during concurrent additions/deletions.

## Inspection

```bash
./store status
./store scans
./store repairs
./store placements OBJECT_ID
```

`status` shows worker availability, membership, free space, heartbeat timestamps, replication factor and pending cleanup. `scans` shows the last reported completed cycles, checked records, pending damage and cursor per worker. `repairs` shows up to 100 pending jobs with reason, attempts, revision and last error. An empty repair queue does not guarantee every file was checked at that instant. `placements` shows each recorded chunk/worker pair and its health/availability.

Example output structure:

```text
Cluster · 4/4 workers available

Worker   Availability  Membership  Free space  Last heartbeat
worker1  ready         ready       ...         ...
worker2  ready         ready       ...         ...
worker3  ready         ready       ...         ...
worker4  ready         ready       ...         ...

Replication  2 copies
Pending cleanup  0 objects
```

## Download safety

Downloads stream to a temporary file beside the destination. The client checks exact size and SHA-256 against object metadata before publishing it. Corrupt or interrupted transfers remove the temporary file and leave an existing destination untouched. Existing files require explicit `--force`; without it, publication also refuses a destination created by another process during the download.

```bash
./store download OBJECT_ID -o report.pdf --force
```

The destination directory must already exist. The client needs temporary disk space for the complete download. Uploads stream from the input file through HTTPX multipart encoding. Upload POSTs are not automatically retried: without server-side idempotency keys, an ambiguous reply can correspond to an already completed upload. Inspect `list` before retrying manually.

## Configuration and automation

```bash
./store --url http://127.0.0.1:8000 status
STORE_URL=http://127.0.0.1:8000 ./store status
./store upload report.pdf --timeout 300 --json
./store list --all --json
```

Global options work before or after the command. `--url` overrides `STORE_URL`; the default is `http://127.0.0.1:8000`. Network inactivity timeout defaults to 300 seconds, with connection establishment limited to at most five seconds; `--timeout` overrides it. This is not a total transfer deadline.

`--json` writes only a JSON result to stdout. Inspection commands return the raw controller status snapshot. Errors use stderr and exit code 1; argument errors use 2; cancellation uses 130. Normal success returns 0. There are no automatic mutation retries and no interactive confirmations. `./store COMMAND --help` lists that command's options.

## Verification

```bash
.venv/bin/pytest -q tests/test_cli.py
.venv/bin/python scripts/check_cli.py
```

The live checker uses the local controller, exercises every command, verifies binary bytes and checksums independently, and deletes only its own temporary object. It does not inject faults or stop containers.
