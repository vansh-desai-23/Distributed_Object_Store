"""Phase 5: repeat worker failures, verify replica repair, and reconcile returns."""
import argparse
import hashlib
import json
from pathlib import Path
import tempfile

import httpx

from check_lifecycle import docker
from check_storage import inventories, require
from demo import wait_for


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--size-mb', type=int, default=10, choices=range(2, 501), metavar='2..500')
    args = parser.parse_args()
    identity = None
    stopped = None
    chunk_ids = set()
    with tempfile.TemporaryDirectory(prefix='phase5-') as directory, httpx.Client(
            base_url='http://127.0.0.1:8000', timeout=180, trust_env=False) as client:
        def status():
            response = client.get('/api/v1/status')
            response.raise_for_status()
            return response.json()

        def metadata():
            response = client.get(f'/api/v1/objects/{identity}/metadata')
            response.raise_for_status()
            return response.json()

        def replicated(exact=False):
            return all(sum(r['available'] and r['state'] == 'healthy' for r in c['replicas']) == 2
                       and (not exact or len(c['replicas']) == 2) for c in metadata()['chunks'])

        def verify_download():
            digest = hashlib.sha256()
            size = 0
            with client.stream('GET', f'/api/v1/objects/{identity}') as response:
                response.raise_for_status()
                for block in response.iter_bytes(1_000_000):
                    digest.update(block)
                    size += len(block)
            require(size == args.size_mb * 1_000_000 and digest.hexdigest() == expected,
                    'Download lost or changed bytes')
            print(f'PASS: {size:,}-byte download matches original SHA-256', flush=True)

        def physical_replicas(excluded=None):
            # Do not contact the intentionally stopped worker.
            names = [f'worker{i}' for i in range(1, 5) if f'worker{i}' != excluded]
            code = '''
import json, sys, urllib.request
result = {}
for worker in json.loads(sys.argv[1]):
    with urllib.request.urlopen(f"http://{worker}:8000/internal/workers/{worker}/chunks", timeout=30) as response:
        result[worker] = json.load(response)["chunks"]
print(json.dumps(result))
'''
            inventory = json.loads(docker('compose', 'exec', '-T', 'controller', 'python', '-c', code, json.dumps(names)))
            for chunk in metadata()['chunks']:
                holders = set()
                for worker, items in inventory.items():
                    for item in items:
                        if item['chunk_id'] == chunk['chunk_id']:
                            require(item['valid'] and item['checksum'] == chunk['checksum'] and item['size'] == chunk['size'],
                                    'Worker has an invalid replica')
                            holders.add(worker)
                require(len(holders) == 2, f"Chunk {chunk['position']} does not have two physical live copies")
                recorded = {r['worker_id'] for r in chunk['replicas'] if r['available'] and r['state'] == 'healthy'}
                require(holders == recorded, 'Physical locations do not match healthy live metadata')
            print('PASS: two checksum-verified physical replicas per chunk', flush=True)

        try:
            wait_for(lambda: len([w for w in status()['workers'] if w['available']]) == 4, 'four workers available')
            path = Path(directory) / 'phase5.bin'
            digest = hashlib.sha256()
            with path.open('wb') as stream:
                for index in range(args.size_mb):
                    block = hashlib.sha256(str(index).encode()).digest() * 31250
                    stream.write(block)
                    digest.update(block)
            expected = digest.hexdigest()
            with path.open('rb') as stream:
                response = client.post('/api/v1/objects', files={'file': (path.name, stream, 'application/octet-stream')})
            response.raise_for_status()
            identity = response.json()['object_id']
            chunk_ids = {c['chunk_id'] for c in response.json()['chunks']}
            verify_download()
            physical_replicas()
            for cycle in range(2):
                stopped = 'worker2' if cycle == 0 else next(
                    r['worker_id'] for r in metadata()['chunks'][0]['replicas'] if r['worker_id'] != 'worker2')
                before = next(w for w in status()['workers'] if w['worker_id'] == stopped)
                docker('stop', stopped)
                print(f'Cycle {cycle + 1}: stopped {stopped}', flush=True)
                # Exercise fallback before waiting for the heartbeat threshold.
                verify_download()
                wait_for(lambda: not next(w['available'] for w in status()['workers'] if w['worker_id'] == stopped),
                         'heartbeat expiry detected')
                wait_for(replicated, 'two available replicas restored while worker remains stopped')
                physical_replicas(excluded=stopped)
                docker('start', stopped)
                stopped = None
                wait_for(lambda: all(w['available'] for w in status()['workers']), 'worker registered again')
                after = next(w for w in status()['workers'] if w['worker_id'] == before['worker_id'])
                require(after['storage_id'] == before['storage_id'] and after['boot_id'] != before['boot_id'],
                        'Restart must preserve storage identity and change boot identity')
                require(after['eligible'], 'Returning worker was not reconciled')
                print('PASS: persistent storage identity, fresh boot and reconciled eligibility', flush=True)
                wait_for(lambda: replicated(exact=True), 'return reconciled to exactly two healthy replicas')
                physical_replicas()
                verify_download()
            print('Phase 5 recovery acceptance passed.', flush=True)
        finally:
            if stopped:
                docker('start', stopped)
            if identity:
                wait_for(lambda: len([w for w in status()['workers'] if w['available']]) == 4, 'all workers restored')
                response = client.delete(f'/api/v1/objects/{identity}')
                require(response.status_code in (204, 404), 'Could not delete recovery test object')
                wait_for(lambda: not chunk_ids.intersection(
                    item['chunk_id'] for items in inventories().values() for item in items), 'test chunks cleaned up')


if __name__ == '__main__':
    main()
