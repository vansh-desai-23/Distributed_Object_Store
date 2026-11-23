"""Guided demonstration against the local Compose deployment; only deletes its own objects."""
import argparse
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from uuid import uuid4

import httpx

ROOT = Path(__file__).resolve().parents[1]
C = 1_000_000
WORKERS = {f'worker{i}' for i in range(1, 5)}
CHECKLIST = (
    'Docker topology and isolated persistent worker volumes.',
    'Binary upload/download with matching size and SHA-256.',
    '1 MB chunks, preserved ordering, partial final chunk and worker distribution.',
    'Exactly two independently verified physical replicas on distinct workers.',
    'System metadata and custom metadata updates without re-uploading data.',
    'Object listing, deletion, physical cleanup and subsequent 404 responses.',
    'Worker failure, continued retrieval, replication recovery and safe return.',
    'Intentional corruption, detection, valid-copy retrieval and repair.',
    'Representative success codes and meaningful JSON error responses.',
    'Upload-key replay, conflicting-request rejection and deleted-object protection.',
    'Persistent worker identity, fresh boot identity and reconciliation before readiness.',
    'Proactive local scans and retained damage reports during controller downtime.',
    'Offline deletion without resurrection when the worker returns.',
    'Objects, metadata and upload receipts surviving controller restart.',
)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def docker(*args):
    result = subprocess.run(['docker', *args], cwd=ROOT, capture_output=True, text=True, timeout=180)
    require(result.returncode == 0, result.stderr.strip() or 'Docker command failed')
    return result.stdout


def wait_for(predicate, description, timeout=180):
    start = time.monotonic()
    print(f'Waiting: {description}', flush=True)
    while time.monotonic() - start < timeout:
        try:
            if predicate():
                print(f'PASS: {description} ({time.monotonic() - start:.1f}s)', flush=True)
                return
        except httpx.HTTPError:
            pass
        time.sleep(2)
    raise RuntimeError(f'Timed out: {description}')


def worker_get(worker, path):
    code = '''
import json, sys, urllib.request, urllib.error
try:
    with urllib.request.urlopen('http://localhost:8000' + sys.argv[1], timeout=10) as r:
        print(json.dumps({'status': r.status, 'body': json.load(r)}))
except urllib.error.HTTPError as e:
    print(json.dumps({'status': e.code, 'body': json.load(e)}))
except (urllib.error.URLError, TimeoutError):
    print(json.dumps({'status': 0, 'body': {}}))
'''
    return json.loads(docker('compose', 'exec', '-T', worker, 'python', '-c', code, path))


def inventories(excluded=None):
    return {w: worker_get(w, f'/internal/workers/{w}/chunks')['body']['chunks']
            for w in sorted(WORKERS - {excluded})}


class Demo:
    def __init__(self, args, client, directory):
        self.args, self.client, self.directory = args, client, Path(directory)
        self.created, self.chunk_ids, self.results = [], set(), []
        self.stopped, self.paused = set(), False

    @contextmanager
    def stage(self, title):
        print(f'\n{len(self.results) + 1}. {title}', flush=True)
        if not self.args.unattended:
            input('Press Enter to run this stage (Ctrl+C to stop): ')
        start = time.monotonic()
        try:
            yield
        except BaseException:
            self.results.append((title, 'FAIL', time.monotonic() - start))
            raise
        else:
            self.results.append((title, 'PASS', time.monotonic() - start))
            print(f'PASS: {title}', flush=True)

    def get(self, path):
        response = self.client.get(path)
        require(response.status_code == 200, f'GET {path}: {response.status_code}')
        return response.json()

    def status(self):
        return self.get('/api/v1/status')['workers']

    def ready(self):
        workers = self.status()
        return {w['worker_id'] for w in workers if w['available'] and w['eligible']} == WORKERS

    def metadata(self, identity):
        return self.get(f'/api/v1/objects/{identity}/metadata')

    def error(self, response, expected):
        require(response.status_code == expected, f'Expected HTTP {expected}, got {response.status_code}')
        body = response.json()
        require(isinstance(body.get('error'), dict) and body['error'].get('message'), 'Missing JSON error message')
        print(f'HTTP {expected}: {body["error"]["message"]}')

    def upload(self, path, key=None, metadata=None):
        with path.open('rb') as stream:
            response = self.client.post('/api/v1/objects',
                headers={'Idempotency-Key': key} if key else {},
                files={'file': (path.name, stream, 'application/octet-stream')},
                data={'metadata': json.dumps(metadata or {'purpose': 'guided-demo'})})
        if response.status_code == 201:
            body = response.json()
            if body['object_id'] not in self.created:
                self.created.append(body['object_id'])
                self.chunk_ids.update(c['chunk_id'] for c in body['chunks'])
        return response

    def download(self, identity, expected, size):
        digest, received = hashlib.sha256(), 0
        with self.client.stream('GET', f'/api/v1/objects/{identity}') as response:
            require(response.status_code == 200, f'Download must return 200, got {response.status_code}')
            require(response.headers['content-type'] == 'application/octet-stream', 'Wrong binary content type')
            for block in response.iter_bytes(C):
                digest.update(block)
                received += len(block)
        require(received == size and digest.hexdigest() == expected, 'Downloaded bytes do not match original')
        print(f'Download: {received:,} bytes; SHA-256 {digest.hexdigest()}')

    def physical(self, identity, excluded=None):
        chunks = self.metadata(identity)['chunks']
        inventory = inventories(excluded)
        for chunk in chunks:
            copies = [(w, item) for w, items in inventory.items() for item in items if item['chunk_id'] == chunk['chunk_id']]
            recorded = {r['worker_id'] for r in chunk['replicas'] if r['available'] and r['state'] == 'healthy'}
            if len(copies) != 2 or {w for w, _ in copies} != recorded:
                return False
            if any(not item['valid'] or item['checksum'] != chunk['checksum'] or item['size'] != chunk['size'] for _, item in copies):
                return False
        return True

    def replicated(self, identity):
        return all(sum(r['available'] and r['state'] == 'healthy' for r in c['replicas']) == 2
                   for c in self.metadata(identity)['chunks'])

    def absent(self, ids):
        return not ids.intersection(item['chunk_id'] for items in inventories().values() for item in items)

    def pause(self):
        self.paused = True
        docker('compose', 'pause', 'controller')

    def resume(self):
        docker('compose', 'unpause', 'controller')
        self.paused = False

    def run(self):
        with self.stage('Deployment and persistent storage'):
            ids = docker('compose', 'ps', '--all', '--quiet').split()
            require(len(ids) == 6, 'Start the six services: docker compose up -d --build --wait')
            containers = json.loads(docker('inspect', *ids))
            services = {c['Config']['Labels']['com.docker.compose.service']: c for c in containers}
            require(set(services) == WORKERS | {'controller', 'postgres'}, 'Unexpected topology')
            volumes = []
            for name, container in sorted(services.items()):
                require(container['State']['Running'] and container['State'].get('Health', {}).get('Status') == 'healthy', f'{name} not healthy')
                destination = '/data' if name in WORKERS else '/tmp' if name == 'controller' else '/var/lib/postgresql/data'
                mounts = [m for m in container['Mounts'] if m['Destination'] == destination and m['Type'] == 'volume' and m['RW']]
                require(len(mounts) == 1, f'{name} missing persistent volume')
                volumes.append(mounts[0]['Name'])
                print(f'{name:12} healthy  {mounts[0]["Name"]} → {destination}')
            require(len(set(volumes)) == 6, 'Data volumes must be isolated')
            wait_for(self.ready, 'all four workers eligible')

        with self.stage('Upload, download, chunks and physical replication'):
            path = self.directory / 'demo.bin'
            digest, hashes = hashlib.sha256(), []
            with path.open('wb') as stream:
                for index in range(self.args.size_mb + 1):
                    block = hashlib.sha256(str(index).encode()).digest() * 31250 if index < self.args.size_mb else b'partial final chunk'
                    stream.write(block)
                    digest.update(block)
                    hashes.append(hashlib.sha256(block).hexdigest())
            expected, size = digest.hexdigest(), path.stat().st_size
            print(f'Uploading {size:,} bytes to the running controller…', flush=True)
            response = self.upload(path)
            require(response.status_code == 201, 'Upload must return 201')
            original = response.json()
            identity = original['object_id']
            print(f'Object: {identity}\nSource: {size:,} bytes; SHA-256 {expected}')
            require(original['checksum'] == expected and original['size'] == size, 'Upload metadata mismatch')
            require(original['chunk_count'] == len(original['chunks']) == len(hashes), 'Wrong chunk count')
            counts = dict.fromkeys(sorted(WORKERS), 0)
            for index, chunk in enumerate(original['chunks']):
                require(chunk['position'] == index and chunk['size'] == min(C, size - index * C) and chunk['checksum'] == hashes[index], 'Wrong chunk order/size/checksum')
                require(len(chunk['replicas']) == len({r['worker_id'] for r in chunk['replicas']}) == 2, 'Wrong replica count')
                for replica in chunk['replicas']:
                    counts[replica['worker_id']] += 1
            require(all(counts.values()), 'Object was not distributed across all workers')
            print(f'{len(hashes)} chunks; final chunk {original["chunks"][-1]["size"]} bytes')
            for chunk in original['chunks'][:3] + original['chunks'][-1:]:
                print(f'Chunk {chunk["position"]:3}: {chunk["size"]:9,} bytes  ' + ', '.join(r['worker_id'] for r in chunk['replicas']))
            print('Replica totals:', ', '.join(f'{w}={n}' for w, n in counts.items()))
            require(self.physical(identity), 'Physical replicas do not match metadata')
            print('All chunks independently checked on worker disks, not just the displayed sample.')
            self.download(identity, expected, size)

        with self.stage('Metadata, listing and API errors'):
            before = self.metadata(identity)
            for field in ('name', 'size', 'content_type', 'created_at', 'modified_at', 'checksum', 'chunk_count'):
                print(f'{field}: {before[field]}')
            response = self.client.patch(f'/api/v1/objects/{identity}/metadata', json={'purpose': 'updated-live', 'presenter': 'demo'})
            require(response.status_code == 200, 'PATCH must return 200')
            patched = response.json()
            require(patched['metadata'] == {'purpose': 'updated-live', 'presenter': 'demo'}, 'Custom metadata mismatch')
            require(all(patched[k] == before[k] for k in ('chunks', 'checksum', 'size', 'created_at')), 'PATCH changed object data/layout')
            print('Custom metadata:', patched['metadata'])
            listed, offset = [], 0
            while True:
                page = self.get(f'/api/v1/objects?limit=100&offset={offset}')['objects']
                listed.extend(page)
                if len(page) < 100:
                    break
                offset += len(page)
            require(any(o['object_id'] == identity and all(k in o for k in ('name', 'size', 'content_type')) for o in listed), 'List omitted object/fields')
            self.download(identity, expected, size)
            self.error(self.client.get(f'/api/v1/objects/{uuid4()}/metadata'), 404)
            self.error(self.client.patch(f'/api/v1/objects/{identity}/metadata', json={'invalid': 123}), 400)
            cli = subprocess.run([sys.executable, '-m', 'object_store.cli', 'metadata', identity], cwd=ROOT, text=True, capture_output=True, timeout=30)
            require(cli.returncode == 0, cli.stderr)
            print('Operational CLI:\n' + cli.stdout)

        with self.stage('Worker failure, offline deletion and safe rejoining'):
            small = self.directory / 'offline.bin'
            small.write_bytes(bytes(range(256)) * 4096)
            response = self.upload(small)
            require(response.status_code == 201, 'Small upload failed')
            doomed = response.json()
            doomed_ids = {c['chunk_id'] for c in doomed['chunks']}
            worker = 'worker2'
            inventory = worker_get(worker, f'/internal/workers/{worker}/chunks')['body']['chunks']
            require(any(item['chunk_id'] in doomed_ids and item['valid'] for item in inventory),
                    'worker2 must hold a copy of the offline-deletion object before shutdown')
            before_worker = next(w for w in self.status() if w['worker_id'] == worker)
            print(f'Running: docker stop {worker}')
            self.stopped.add(worker)
            docker('stop', worker)
            self.download(identity, expected, size)
            wait_for(lambda: not next(w['available'] for w in self.status() if w['worker_id'] == worker), 'worker failure detected')
            wait_for(lambda: self.replicated(identity), 'two live replicas restored while worker is offline')
            require(self.physical(identity, excluded=worker), 'Live physical replication failed')
            require(self.client.delete(f'/api/v1/objects/{doomed["object_id"]}').status_code == 204, 'Offline delete failed')
            self.error(self.client.get(f'/api/v1/objects/{doomed["object_id"]}'), 404)
            # Hold registration briefly to make the not-ready state observable.
            self.pause()
            print(f'Running: docker start {worker} (controller briefly paused)')
            docker('start', worker)
            self.stopped.discard(worker)
            wait_for(lambda: worker_get(worker, '/health')['status'] == 200, 'worker process restarted')
            require(worker_get(worker, '/ready')['status'] == 503, 'Unregistered worker became ready')
            after_worker = worker_get(worker, '/health')['body']
            require(after_worker['storage_id'] == before_worker['storage_id'] and after_worker['boot_id'] != before_worker['boot_id'], 'Identity contract failed')
            print(f'Storage ID preserved: {after_worker["storage_id"]}\nBoot ID changed: {before_worker["boot_id"]} → {after_worker["boot_id"]}\nReadiness: 503 until registration and reconciliation')
            self.resume()
            wait_for(self.ready, 'returning worker reconciled and eligible')
            wait_for(lambda: self.absent(doomed_ids), 'offline-deleted chunks removed from every worker')
            self.error(self.client.get(f'/api/v1/objects/{doomed["object_id"]}'), 404)
            wait_for(lambda: self.physical(identity), 'exactly two physical copies after rejoining')
            self.download(identity, expected, size)

        with self.stage('Proactive corruption detection, durable reports and repair'):
            chunk = self.metadata(identity)['chunks'][0]
            worker = chunk['replicas'][0]['worker_id']
            self.pause()
            scan_before = worker_get(worker, '/internal/scan')['body']['scan_progress']['checked']
            print(f'Corrupting only demo chunk {chunk["chunk_id"]} on {worker}; controller paused.')
            docker('compose', 'exec', '-T', worker, 'python', '-c',
                "import pathlib,sys; p=pathlib.Path('/data',sys.argv[1]+'.chunk'); b=bytearray(p.read_bytes()); b[0]^=1; p.write_bytes(b)", chunk['chunk_id'])
            def reported():
                report = worker_get(worker, '/internal/scan')['body']
                return report['scan_progress']['checked'] > scan_before and any(e['chunk_id'] == chunk['chunk_id'] for e in report['damage_reports'])
            wait_for(reported, 'local scan detected corruption without a download and retained its report')
            report = worker_get(worker, '/internal/scan')['body']
            print('Scan progress:', report['scan_progress'])
            require(any(e['chunk_id'] == chunk['chunk_id'] for e in report['damage_reports']), 'Damage report lost during controller downtime')
            # Observe both copies before allowing repair, so a fast repair cannot
            # make the fallback evidence ambiguous. This is after scan detection.
            probe = """
import hashlib, json, sys, urllib.request, urllib.error
try:
    with urllib.request.urlopen('http://localhost:8000/internal/chunks/' + sys.argv[1], timeout=10) as r:
        print(json.dumps({'status': r.status, 'checksum': hashlib.sha256(r.read()).hexdigest()}))
except urllib.error.HTTPError as e:
    print(json.dumps({'status': e.code}))
"""
            for replica in chunk['replicas']:
                holder = replica['worker_id']
                proof = json.loads(docker('compose', 'exec', '-T', holder, 'python', '-c', probe, chunk['chunk_id']))
                require(proof['status'] == (409 if holder == worker else 200), 'Replica corruption/fallback evidence failed')
                if holder != worker:
                    require(proof['checksum'] == chunk['checksum'], 'Surviving replica differs from original')
                print(f'{holder}: chunk GET HTTP {proof["status"]}')
            self.resume()
            wait_for(self.ready, 'worker heartbeats restored after controller downtime')
            self.download(identity, expected, size)
            wait_for(lambda: self.physical(identity), 'two checksum-verified original replicas restored')
            wait_for(lambda: not any(e['chunk_id'] == chunk['chunk_id'] for e in worker_get(worker, '/internal/scan')['body']['damage_reports']), 'damage report acknowledged and removed from worker outbox')

        with self.stage('Upload idempotency and controller-restart persistence'):
            key = 'demo-' + uuid4().hex
            first = self.upload(small, key)
            require(first.status_code == 201 and first.headers.get('Idempotency-Replayed') == 'false', 'Initial key claim failed')
            retry = self.upload(small, key)
            require(retry.status_code == 201 and retry.json() == first.json() and retry.headers.get('Idempotency-Replayed') == 'true', 'Replay differs')
            print('Identical retry returned original object:', first.json()['object_id'])
            self.error(self.upload(small, key, {'purpose': 'conflicting'}), 409)
            print('Running: docker compose restart controller')
            docker('compose', 'restart', 'controller')
            wait_for(lambda: self.client.get('/health', timeout=5).status_code == 200, 'controller restarted')
            wait_for(self.ready, 'workers available after controller restart')
            require(self.metadata(identity)['metadata'] == patched['metadata'], 'Custom metadata lost on restart')
            self.download(identity, expected, size)
            retry = self.upload(small, key)
            require(retry.status_code == 201 and retry.json() == first.json() and retry.headers.get('Idempotency-Replayed') == 'true', 'Receipt lost after restart')
            require(self.client.delete(f'/api/v1/objects/{first.json()["object_id"]}').status_code == 204, 'Keyed deletion failed')
            wait_for(lambda: self.absent({c['chunk_id'] for c in first.json()['chunks']}), 'keyed object physically deleted')
            self.error(self.upload(small, key), 410)

        with self.stage('Final deletion and physical cleanup'):
            require(self.client.delete(f'/api/v1/objects/{identity}').status_code == 204, 'Delete must return 204')
            self.error(self.client.get(f'/api/v1/objects/{identity}'), 404)
            wait_for(lambda: self.absent(self.chunk_ids), 'all demo chunks removed', timeout=max(180, 2 * len(self.chunk_ids)))

    def cleanup(self):
        errors = []
        actions = []
        if self.paused:
            actions.append(self.resume)
        for worker in self.stopped:
            actions.append(lambda w=worker: docker('start', w))
        for action in actions:
            try:
                action()
            except Exception as exc:
                errors.append(str(exc))
        if self.created:
            try:
                wait_for(lambda: self.client.get('/health', timeout=5).status_code == 200, 'controller reachable for cleanup')
                for identity in self.created:
                    require(self.client.delete(f'/api/v1/objects/{identity}').status_code in (204, 404), f'Cleanup failed: {identity}')
                wait_for(self.ready, 'workers restored')
                wait_for(lambda: self.absent(self.chunk_ids), 'only this run’s data cleaned up', timeout=max(180, 2 * len(self.chunk_ids)))
            except Exception as exc:
                errors.append(str(exc))
        require(not errors, '; '.join(errors))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--unattended', action='store_true', help='Run stages without Enter prompts')
    parser.add_argument('--size-mb', type=int, default=100, choices=range(2, 501), metavar='2..500', help='Main object size in decimal MB, plus a 19-byte final chunk (default: 100)')
    args = parser.parse_args()
    if not args.unattended and not sys.stdin.isatty():
        parser.error('Guided mode requires a terminal; use --unattended for automation')
    print('Demo coverage', flush=True)
    for item in CHECKLIST:
        print(f'• {item}')
    print('\nStart beforehand: docker compose up -d --build --wait\nThis demo pauses/restarts local services and cleans up only its own objects.\nWorker commands run automatically after each stage’s Enter prompt.', flush=True)
    failed = False
    with tempfile.TemporaryDirectory(prefix='store-demo-') as directory, httpx.Client(base_url='http://127.0.0.1:8000', timeout=300, trust_env=False) as client:
        demo = Demo(args, client, directory)
        try:
            demo.run()
        except (Exception, KeyboardInterrupt) as exc:
            failed = True
            print(f'\nFAIL: {exc or "Interrupted"}', file=sys.stderr)
        finally:
            try:
                demo.cleanup()
            except (Exception, KeyboardInterrupt) as exc:
                failed = True
                print(f'Cleanup incomplete: {exc}. Demo object IDs: {demo.created}', file=sys.stderr)
            print('\nResults')
            for title, result, seconds in demo.results:
                print(f'{result:4}  {seconds:6.1f}s  {title}')
            print('DEMO FAILED' if failed else 'ALL DEMO STAGES PASSED', flush=True)
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
