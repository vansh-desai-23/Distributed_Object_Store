"""Phase 6 live demonstration: corrupt only this run's chunks and verify repair."""
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
    paused = False
    chunk_ids = set()
    with tempfile.TemporaryDirectory(prefix='phase6-') as directory, httpx.Client(
            base_url='http://127.0.0.1:8000', timeout=180, trust_env=False) as client:
        def metadata():
            response = client.get(f'/api/v1/objects/{identity}/metadata')
            response.raise_for_status()
            return response.json()

        def download():
            digest = hashlib.sha256()
            received = 0
            with client.stream('GET', f'/api/v1/objects/{identity}') as response:
                response.raise_for_status()
                for part in response.iter_bytes(1_000_000):
                    received += len(part)
                    digest.update(part)
            require(received == args.size_mb * 1_000_000 and digest.hexdigest() == expected,
                    'Downloaded bytes differ from the original')
            print('PASS: complete download matches original size and SHA-256', flush=True)

        def repaired(chunk):
            copies = []
            for worker, items in inventories().items():
                for item in items:
                    if item['chunk_id'] == chunk['chunk_id']:
                        copies.append(item)
            return len(copies) == 2 and all(item['valid'] and item['checksum'] == chunk['checksum'] and
                                          item['size'] == chunk['size'] for item in copies)

        try:
            status = client.get('/api/v1/status')
            status.raise_for_status()
            require(len([w for w in status.json()['workers'] if w['available']]) == 4, 'Four workers must be available')
            path = Path(directory) / 'phase6.bin'
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
            download()
            for mode in ('flip', 'manifest', 'consistent'):
                chunk = metadata()['chunks'][0]
                worker = chunk['replicas'][0]['worker_id']
                # Pause repair only during injection/proof, then resume immediately.
                # This prevents the background loop from repairing before evidence is captured.
                paused = True
                docker('compose', 'pause', 'controller')
                try:
                    code = '''
import hashlib, json, pathlib, sys, urllib.error, urllib.request
identity, mode = sys.argv[1:]
path = pathlib.Path('/data', identity + '.chunk')
if mode == 'manifest':
    path.with_suffix('.json').write_text('{broken manifest')
else:
    data = bytearray(path.read_bytes())
    data[0] ^= 1
    path.write_bytes(data)
    if mode == 'consistent':
        path.with_suffix('.json').write_text(json.dumps({'size': len(data), 'checksum': hashlib.sha256(data).hexdigest()}))
try:
    with urllib.request.urlopen(f'http://localhost:8000/internal/chunks/{identity}', timeout=5) as response:
        print(json.dumps({'status': response.status, 'checksum': hashlib.sha256(response.read()).hexdigest()}))
except urllib.error.HTTPError as exc:
    print(json.dumps({'status': exc.code}))
'''
                    proof = json.loads(docker('exec', worker, 'python', '-c', code, chunk['chunk_id'], mode))
                    if mode == 'consistent':
                        require(proof['status'] == 200 and proof['checksum'] != chunk['checksum'],
                                'Failed to create self-consistent but incorrect worker data')
                        print('PASS: worker accepts altered bytes/manifest; authoritative checksum differs', flush=True)
                    else:
                        require(proof['status'] == 409, 'Worker failed to detect injected corruption')
                        print(f'PASS: worker detected {mode} corruption with 409', flush=True)
                finally:
                    docker('compose', 'unpause', 'controller')
                    paused = False
                if mode != 'manifest':
                    download()
                # Manifest case deliberately relies on background scrubbing without a download.
                wait_for(lambda: repaired(chunk), f'{mode}: exactly two physically verified original copies restored')
                download()
            print('Phase 6 integrity acceptance passed.', flush=True)
        finally:
            if paused:
                docker('compose', 'unpause', 'controller')
            if identity:
                response = client.delete(f'/api/v1/objects/{identity}')
                require(response.status_code in (204, 404), 'Failed to delete integrity test object')
                wait_for(lambda: not chunk_ids.intersection(item['chunk_id'] for items in inventories().values() for item in items),
                         'test object bytes cleaned up')


if __name__ == '__main__':
    main()
