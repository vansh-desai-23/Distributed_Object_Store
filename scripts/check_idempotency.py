"""Verify keyed upload replay and PostgreSQL persistence through a controller restart."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from uuid import uuid4

import httpx

from check_lifecycle import docker
from check_storage import require
from demo import wait_for


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--size-mb', type=int, default=10, choices=range(1, 501), metavar='1..500')
    args = parser.parse_args()
    key, identity = 'verify-' + str(uuid4()), None
    with tempfile.TemporaryDirectory(prefix='keyed-upload-') as directory, httpx.Client(
            base_url='http://127.0.0.1:8000', timeout=300, trust_env=False) as client:
        path = Path(directory) / 'keyed.bin'
        digest = hashlib.sha256()
        with path.open('wb') as stream:
            for index in range(args.size_mb):
                block = hashlib.sha256(str(index).encode()).digest() * 31250
                digest.update(block)
                stream.write(block)
        def upload(metadata='{"purpose":"idempotency-check"}'):
            with path.open('rb') as stream:
                return client.post('/api/v1/objects', headers={'Idempotency-Key': key},
                    files={'file': (path.name, stream, 'application/octet-stream')}, data={'metadata': metadata})
        try:
            first = upload()
            first.raise_for_status()
            identity = first.json()['object_id']
            require(first.status_code == 201 and first.headers['Idempotency-Replayed'] == 'false', 'Initial key claim failed')
            require(first.json()['checksum'] == digest.hexdigest(), 'Original checksum mismatch')
            retry = upload()
            require(retry.status_code == 201 and retry.json() == first.json() and
                    retry.headers['Idempotency-Replayed'] == 'true', 'Retry did not replay the original response')
            print('PASS: same key returns the original object and response', flush=True)
            require(upload('{"purpose":"different"}').status_code == 409, 'Conflicting request was accepted')
            cli = subprocess.run([sys.executable, '-m', 'object_store.cli', 'upload', str(path),
                '--idempotency-key', key, '--meta', 'purpose=idempotency-check', '--json'],
                capture_output=True, text=True, timeout=300)
            require(cli.returncode == 0 and json.loads(cli.stdout) == first.json(), 'CLI retry did not replay original object')
            print('PASS: CLI key reuse and conflicting-request rejection', flush=True)
            docker('compose', 'restart', 'controller')
            wait_for(lambda: client.get('/health', timeout=5).status_code == 200, 'controller restarted')
            retry = upload()
            require(retry.status_code == 201 and retry.json() == first.json() and
                    retry.headers['Idempotency-Replayed'] == 'true', 'Receipt did not survive PostgreSQL restart recovery')
            wait_for(lambda: all(w['available'] for w in client.get('/api/v1/status').json()['workers']),
                     'workers available after restart')
            actual = hashlib.sha256()
            with client.stream('GET', f'/api/v1/objects/{identity}') as response:
                response.raise_for_status()
                for block in response.iter_bytes(1_000_000):
                    actual.update(block)
            require(actual.hexdigest() == digest.hexdigest(), 'Replayed object data changed')
            print('PASS: saved response and file bytes survive controller restart', flush=True)
            require(client.delete(f'/api/v1/objects/{identity}').status_code == 204, 'Delete failed')
            require(upload().status_code == 410, 'Deleted keyed object was recreated')
            code = '''
import sys
from sqlalchemy import select, func
from object_store.database import make_database
from object_store.models import StoredObject, UploadReceipt
engine, sessions = make_database()
with sessions() as session:
    receipt = session.get(UploadReceipt, sys.argv[1])
    print(receipt.state, session.get(StoredObject, receipt.object_id) is None)
engine.dispose()
'''
            def cleaned():
                return docker('compose', 'exec', '-T', 'controller', 'python', '-c', code, key).strip() == 'completed True'
            wait_for(cleaned, 'physical cleanup completed and receipt retained', timeout=max(180, args.size_mb * 2))
            require(upload().status_code == 410, 'Key was recycled after physical cleanup')
            print('PASS: late retries cannot resurrect a deleted object', flush=True)
        finally:
            if identity:
                response = client.delete(f'/api/v1/objects/{identity}')
                require(response.status_code in (204, 404), 'Could not clean test object')


if __name__ == '__main__':
    main()
