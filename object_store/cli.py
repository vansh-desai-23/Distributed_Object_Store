"""Clean terminal client for object operations and read-only cluster inspection."""
import argparse
import hashlib
import json
import mimetypes
import os
from pathlib import Path
import sys
import tempfile
import time
from uuid import UUID

import httpx


class ClientError(Exception):
    pass


def clean(value):
    """Keep untrusted filenames/metadata from injecting terminal control sequences."""
    return ''.join(c if c.isprintable() else ' ' for c in str(value))


def size(value):
    number = float(value)
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if number < 1000 or unit == 'TB':
            return f'{number:.0f} {unit}' if unit == 'B' else f'{number:.1f} {unit}'
        number /= 1000


def table(headers, rows):
    rows = [[clean(cell) for cell in row] for row in rows]
    if not rows:
        return 'No entries.'
    widths = [max(len(header), *(len(row[i]) for row in rows)) for i, header in enumerate(headers)]
    return '\n'.join('  '.join(cell.ljust(widths[i]) for i, cell in enumerate(row)).rstrip()
                     for row in [headers, *rows])


def identity(value):
    try:
        return str(UUID(value))
    except ValueError:
        raise argparse.ArgumentTypeError('Expected an object UUID') from None


def positive(value):
    try:
        result = float(value)
        if not 0 < result < float('inf'):
            raise ValueError
        return result
    except ValueError:
        raise argparse.ArgumentTypeError('Expected a positive finite number') from None


def metadata_pairs(values):
    result = {}
    for value in values:
        key, separator, text = value.partition('=')
        if not separator or not key:
            raise ClientError('Metadata must use KEY=VALUE, for example owner=student')
        result[key] = text
    return result


def parser():
    result = argparse.ArgumentParser(prog='store', description='Distributed Object Store',
        epilog='Examples: store upload report.pdf   ·   store status   ·   store download ID -o report.pdf')
    def options(command):
        command.add_argument('--url', default=argparse.SUPPRESS, help='Controller URL (or STORE_URL)')
        command.add_argument('--timeout', type=positive, default=argparse.SUPPRESS, help='Network timeout in seconds (default: 300)')
        command.add_argument('--json', action='store_true', default=argparse.SUPPRESS, help='Machine-readable JSON output')
    options(result)
    result.set_defaults(url=os.environ.get('STORE_URL', 'http://127.0.0.1:8000'), timeout=300, json=False)
    commands = result.add_subparsers(dest='command', required=True, metavar='COMMAND')
    descriptions = {
        'upload': 'Upload a file', 'download': 'Download and verify a file', 'list': 'List stored objects',
        'metadata': 'Show object metadata', 'patch': 'Merge custom metadata', 'delete': 'Delete an object',
        'status': 'Show cluster health', 'placements': 'Inspect chunk replicas',
        'scans': 'Show worker scan progress', 'repairs': 'Show pending repair jobs',
    }
    for name, description in descriptions.items():
        command = commands.add_parser(name, help=description, description=description)
        options(command)
        if name in ('download', 'metadata', 'patch', 'delete', 'placements'):
            command.add_argument('object_id', type=identity, metavar='ID')
        if name in ('upload', 'patch'):
            command.add_argument('--meta', action='append', default=[], metavar='KEY=VALUE', help='Repeat for multiple metadata fields')
        if name == 'upload':
            command.add_argument('file', type=Path)
            command.add_argument('--idempotency-key', help='Reuse this key when retrying the identical upload')
            command.add_argument('--type', dest='content_type', help='Override the MIME type')
        if name == 'download':
            command.add_argument('-o', '--output', type=Path, required=True, help='Destination file')
            command.add_argument('--force', action='store_true', help='Replace an existing destination after verification')
        if name == 'list':
            command.add_argument('--limit', type=int, default=100, help='Page size, 1–1000 (default: 100)')
            command.add_argument('--offset', type=int, default=0)
            command.add_argument('--all', action='store_true', help='Fetch every page')
    return result


def check(response):
    if response.is_error:
        try:
            message = response.json()['error']['message']
        except (ValueError, KeyError, TypeError):
            message = response.reason_phrase
        raise ClientError(f'{response.status_code}: {message}')
    return response


def download(client, object_id, destination, force, progress):
    metadata = check(client.get(f'/api/v1/objects/{object_id}/metadata')).json()
    if destination.exists() and not force:
        raise ClientError(f'Destination exists: {destination}. Use --force to replace it.')
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=destination.parent, prefix='.store-download-', suffix='.part', delete=False) as output:
            temporary = Path(output.name)
            digest, received = hashlib.sha256(), 0
            with client.stream('GET', f'/api/v1/objects/{object_id}') as response:
                if response.is_error:
                    response.read()
                    check(response)
                for block in response.iter_bytes(1_000_000):
                    received += len(block)
                    if received > metadata['size']:
                        raise ClientError('Download exceeded the expected size')
                    output.write(block)
                    digest.update(block)
                    progress(received, metadata['size'])
            if received != metadata['size'] or digest.hexdigest() != metadata['checksum']:
                raise ClientError('Download failed size/SHA-256 verification; destination was not changed')
            output.flush()
            os.fsync(output.fileno())
        if force:
            os.replace(temporary, destination)
        else:
            # Atomic no-overwrite publication, including another process creating the target mid-download.
            os.link(temporary, destination)
            temporary.unlink()
        return {'object_id': object_id, 'path': str(destination), 'size': received,
                'checksum': digest.hexdigest(), 'verified': True}
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def execute(args, client, progress):
    command = args.command
    path = f'/api/v1/objects/{args.object_id}' if hasattr(args, 'object_id') else '/api/v1/objects'
    if command == 'upload':
        if not args.file.is_file():
            raise ClientError(f'Not a regular file: {args.file}')
        content_type = args.content_type or mimetypes.guess_type(args.file.name)[0] or 'application/octet-stream'
        with args.file.open('rb') as stream:
            return check(client.post(path, files={'file': (args.file.name, stream, content_type)},
                                     data={'metadata': json.dumps(metadata_pairs(args.meta))},
                                     headers={'Idempotency-Key': args.idempotency_key} if args.idempotency_key is not None else {})).json()
    if command == 'download':
        return download(client, args.object_id, args.output, args.force, progress)
    if command == 'list':
        if not 1 <= args.limit <= 1000 or args.offset < 0:
            raise ClientError('Use --limit 1–1000 and --offset 0 or greater')
        offset, objects = args.offset, []
        while True:
            page = check(client.get(path, params={'limit': args.limit, 'offset': offset})).json()['objects']
            objects.extend(page)
            if not args.all or len(page) < args.limit:
                return {'objects': objects}
            offset += len(page)
    if command in ('metadata', 'placements'):
        return check(client.get(path + '/metadata')).json()
    if command == 'patch':
        if not args.meta:
            raise ClientError('Provide at least one --meta KEY=VALUE')
        return check(client.patch(path + '/metadata', json=metadata_pairs(args.meta))).json()
    if command == 'delete':
        check(client.delete(path))
        return {'object_id': args.object_id, 'deleted': True, 'physical_cleanup': 'asynchronous'}
    return check(client.get('/api/v1/status')).json()


def render(command, data):
    if command == 'list':
        rows = [[o['object_id'], o['name'], size(o['size'])] for o in data['objects']]
        return f"Objects · {len(rows)}\n\n" + table(['ID', 'Name', 'Size'], rows)
    if command == 'delete':
        return f"Deleted {data['object_id']}\nPhysical cleanup continues in the background."
    if command == 'download':
        return f"Downloaded {clean(data['path'])} · {size(data['size'])}\nSHA-256 verified  {data['checksum']}"
    if command in ('upload', 'metadata', 'patch'):
        title = {'upload': 'Uploaded', 'metadata': 'Object', 'patch': 'Updated'}[command]
        fields = [[key, data[value]] for key, value in [('ID', 'object_id'), ('Name', 'name'), ('Type', 'content_type')]]
        fields += [['Size', size(data['size'])], ['Chunks', data['chunk_count']], ['SHA-256', data['checksum']]]
        fields += [[f'Metadata · {key}', value] for key, value in data.get('metadata', {}).items()]
        return title + '\n\n' + table(['Field', 'Value'], fields)
    if command == 'placements':
        rows = [[c['position'], c['chunk_id'], size(c['size']), r['worker_id'], r['state'], 'yes' if r['available'] else 'no']
                for c in data['chunks'] for r in c['replicas']]
        return f"Placements · {clean(data['name'])}\n\n" + table(['Chunk', 'ID', 'Size', 'Worker', 'Replica', 'Available'], rows)
    if command == 'repairs':
        jobs = data.get('repair_jobs', [])
        rows = [[j['chunk_id'], j['reason'], j['attempts'], j['revision'], j['last_error'] or ''] for j in jobs]
        return f"Pending repairs · {len(jobs)} shown (up to 100)\n\n" + table(['Chunk', 'Reason', 'Attempts', 'Revision', 'Last error'], rows)
    if command == 'scans':
        rows = []
        for worker in data['workers']:
            scan = worker.get('scan_progress')
            rows.append([worker['worker_id'], scan['cycles'] if scan else '—', scan['checked'] if scan else '—',
                         scan['pending_damage'] if scan else '—', scan['cursor'] or 'cycle boundary' if scan else 'not reported'])
        return 'Integrity scans · last reported\n\n' + table(['Worker', 'Cycles', 'Checked', 'Pending damage', 'Cursor'], rows)
    workers = data['workers']
    rows = [[w['worker_id'], 'ready' if w['available'] else 'unavailable', w.get('membership_state', 'unknown'),
             size(w['free_bytes']), w['last_seen']] for w in workers]
    return (f"Cluster · {sum(bool(w['available']) for w in workers)}/{len(workers)} workers available\n\n"
            + table(['Worker', 'Availability', 'Membership', 'Free space', 'Last heartbeat'], rows)
            + f"\n\nReplication  {data['replication_factor']} copies\nPending cleanup  {data['pending_cleanup_objects']} objects")


def main(argv=None, *, transport=None, stdout=None, stderr=None):
    stdout, stderr = stdout or sys.stdout, stderr or sys.stderr
    args = parser().parse_args(argv)
    started, last_update = time.monotonic(), 0.0
    active_progress = False
    def progress(received, total):
        nonlocal last_update, active_progress
        instant = time.monotonic()
        if not args.json and stderr.isatty() and instant - last_update >= 0.2:
            print(f'\rDownloading  {size(received)} / {size(total)}', end='', file=stderr, flush=True)
            last_update, active_progress = instant, True
    try:
        if not args.json and args.command == 'upload':
            print(f'Uploading {clean(args.file.name)}…', file=stderr, flush=True)
        with httpx.Client(base_url=args.url.rstrip('/'), timeout=httpx.Timeout(args.timeout, connect=min(5, args.timeout)), trust_env=False, transport=transport) as client:
            data = execute(args, client, progress)
        if active_progress:
            print(file=stderr)
        if args.json:
            print(json.dumps(data, ensure_ascii=True), file=stdout)
        else:
            print(render(args.command, data), file=stdout)
            if args.command in ('upload', 'download'):
                print(f'Completed in {time.monotonic() - started:.1f}s', file=stdout)
        return 0
    except (ClientError, httpx.HTTPError, httpx.InvalidURL, OSError, ValueError) as error:
        if active_progress:
            print(file=stderr)
        message = str(error) or type(error).__name__
        print(json.dumps({'error': message}) if args.json else f'Error: {clean(message)}', file=stderr)
        return 1
    except KeyboardInterrupt:
        print('\nCancelled.', file=stderr)
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
