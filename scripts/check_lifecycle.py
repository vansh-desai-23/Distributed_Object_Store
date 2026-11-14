"""Phase 4 live check: deletion survives offline workers and controller restart."""
import hashlib
import subprocess

import httpx

from check_storage import ROOT, inventories, require
from demo import wait_for


def docker(*args):
    return subprocess.run(['docker', *args], cwd=ROOT, check=True, timeout=60,
                          capture_output=True, text=True).stdout


def main():
    identity = None
    stopped = None
    chunks = []
    body = b'phase-four\x00\xff' * 100_000
    with httpx.Client(base_url='http://127.0.0.1:8000', timeout=30, trust_env=False) as client:
        def status():
            response = client.get('/api/v1/status')
            response.raise_for_status()
            return response.json()

        def absent():
            ids = {c['chunk_id'] for c in chunks}
            return not ids.intersection(item['chunk_id'] for items in inventories().values() for item in items)

        def pending_state():
            code = '''
import sys
from object_store.database import make_database
from object_store.models import StoredObject
engine, sessions = make_database()
with sessions() as session:
    obj = session.get(StoredObject, sys.argv[1])
    print(obj.state if obj else "absent")
engine.dispose()
'''
            return docker('compose', 'exec', '-T', 'controller', 'python', '-c', code, identity).strip()

        try:
            require(len([w for w in status()['workers'] if w['available']]) == 4, 'Four workers must be available')
            response = client.post('/api/v1/objects', files={'file': ('phase4.bin', body, 'application/octet-stream')},
                                   data={'metadata': '{"phase":"4"}'})
            response.raise_for_status()
            meta = response.json()
            identity, chunks = meta['object_id'], meta['chunks']
            require(meta['checksum'] == hashlib.sha256(body).hexdigest(), 'Upload checksum mismatch')
            stopped = chunks[0]['replicas'][0]['worker_id']
            docker('stop', stopped)
            wait_for(lambda: not next(w['available'] for w in status()['workers'] if w['worker_id'] == stopped), 'worker offline')
            require(client.delete(f'/api/v1/objects/{identity}').status_code == 204, 'DELETE must return 204')
            require(client.get(f'/api/v1/objects/{identity}').status_code == 404, 'Deleted object remained readable')
            require(client.patch(f'/api/v1/objects/{identity}/metadata', json={'phase': 'resurrect'}).status_code == 404,
                    'PATCH must not revive a deleted object')
            require(client.delete(f'/api/v1/objects/{identity}').status_code == 404, 'Repeated DELETE must return 404')
            require(pending_state() == 'deleting', 'Offline deletion must retain a durable cleanup record')
            print('PASS: logical deletion, repeated DELETE, and durable offline cleanup', flush=True)
            docker('compose', 'restart', 'controller')
            wait_for(lambda: client.get('/health').status_code == 200, 'controller restarted')
            require(pending_state() == 'deleting', 'Cleanup intent did not survive restart')
            require(client.get(f'/api/v1/objects/{identity}/metadata').status_code == 404, 'Restart resurrected deleted metadata')
            docker('start', stopped)
            stopped = None
            wait_for(lambda: all(w['available'] for w in status()['workers']), 'worker returned')
            wait_for(lambda: pending_state() == 'absent' and absent(), 'durable cleanup finished on every worker')
            # A stale retry must be rejected on ALL workers, including prior surplus locations.
            code = '''
import hashlib, httpx, sys
identity = sys.argv[1]
# The first original chunk is exactly one decimal MB.
payload = (b"phase-four\\x00\\xff" * 100000)[:1000000]
with httpx.Client(timeout=5) as client:
    for i in range(1, 5):
        response = client.put(f"http://worker{i}:8000/internal/chunks/{identity}", content=payload,
                              headers={"X-Chunk-Checksum": hashlib.sha256(payload).hexdigest()})
        if response.status_code != 409:
            raise RuntimeError(f"Late PUT unexpectedly accepted by worker{i}: {response.status_code}")
print("PASS: late PUT retries rejected on every worker")
'''
            print(docker('compose', 'exec', '-T', 'controller', 'python', '-c', code, chunks[0]['chunk_id']), end='')
            require(absent(), 'A late PUT recreated deleted chunk bytes')
            print('Phase 4 lifecycle acceptance passed.', flush=True)
        finally:
            if stopped:
                docker('start', stopped)
            if identity:
                wait_for(lambda: client.get('/health').status_code == 200, 'controller reachable for cleanup')
                response = client.delete(f'/api/v1/objects/{identity}')
                require(response.status_code in (204, 404), 'Could not clean up lifecycle test object')
                wait_for(lambda: pending_state() == 'absent' and absent(), 'test object cleanup confirmed')


if __name__ == '__main__':
    main()
