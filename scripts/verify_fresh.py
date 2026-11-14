"""Verify an isolated fresh Compose deployment and remove only its temporary resources."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from uuid import uuid4

import httpx

from check_storage import ROOT, require
from demo import wait_for


def run(*args, timeout=240):
    result = subprocess.run(list(args), cwd=ROOT, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"Command failed: {args[0:3]}\n{result.stderr[-6000:]}\n{result.stdout[-2000:]}")
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--large', action='store_true', help='Also verify a 500 MB round trip')
    args = parser.parse_args()
    project = 'store-verify-' + uuid4().hex[:10]
    config = json.loads(run('docker', 'compose', 'config', '--format', 'json'))
    config['name'] = project
    for group in ('volumes', 'networks'):
        for name, value in config[group].items():
            require(not value.get('external'), 'Fresh verification refuses external resources')
            value['name'] = f'{project}_{name}'
    for service in config['services'].values():
        service.pop('container_name', None)
        service.pop('build', None)
        service['restart'] = 'no'
    config['services']['controller']['ports'] = [
        {'target': 8000, 'published': '0', 'host_ip': '127.0.0.1', 'protocol': 'tcp'}]
    with tempfile.TemporaryDirectory(prefix=project + '-') as directory:
        configuration = Path(directory) / 'compose.json'
        configuration.write_text(json.dumps(config))
        def compose(*command, timeout=240):
            return run('docker', 'compose', '-p', project, '-f', str(configuration), *command, timeout=timeout)
        try:
            print(f'Creating isolated fresh deployment: {project}', flush=True)
            compose('up', '-d', '--no-build', '--wait', timeout=240)
            address = compose('port', 'controller', '8000').strip()
            base = f'http://{address}'
            print('PASS: fresh volumes initialized, migrations applied, six services healthy', flush=True)
            with httpx.Client(base_url=base, timeout=300, trust_env=False) as client:
                def get_json(path):
                    response = client.get(path)
                    response.raise_for_status()
                    return response.json()
                wait_for(lambda: len([w for w in get_json('/api/v1/status')['workers'] if w['available']]) == 4,
                         'four fresh workers registered')
                require(get_json('/api/v1/objects')['objects'] == [], 'Fresh deployment was not empty')
                identity = None
                def metadata():
                    return get_json(f'/api/v1/objects/{identity}/metadata')
                def replicated(exact=False):
                    return all(sum(r['available'] and r['state'] == 'healthy' for r in c['replicas']) == 2
                        and (not exact or len(c['replicas']) == 2) for c in metadata()['chunks'])
                def download(expected, size):
                    digest, received = hashlib.sha256(), 0
                    with client.stream('GET', f'/api/v1/objects/{identity}') as response:
                        response.raise_for_status()
                        for block in response.iter_bytes(1_000_000):
                            digest.update(block)
                            received += len(block)
                    require(received == size and digest.hexdigest() == expected, 'Round trip mismatch')
                    print(f'PASS: {size:,}-byte download matches original checksum', flush=True)
                for size_mb in ([100, 500] if args.large else [100]):
                    path = Path(directory) / 'fresh.bin'
                    digest = hashlib.sha256()
                    with path.open('wb') as stream:
                        for i in range(size_mb):
                            block = hashlib.sha256(str(i).encode()).digest() * 31250
                            stream.write(block)
                            digest.update(block)
                        tail = b'partial final chunk'
                        stream.write(tail)
                        digest.update(tail)
                    size, expected = path.stat().st_size, digest.hexdigest()
                    with path.open('rb') as stream:
                        response = client.post('/api/v1/objects', files={'file': ('fresh.bin', stream, 'application/octet-stream')},
                                               data={'metadata': '{"purpose":"fresh-submission-check"}'})
                    response.raise_for_status()
                    meta = response.json()
                    identity = meta['object_id']
                    require(response.status_code == 201 and meta['checksum'] == expected, 'Upload response mismatch')
                    require(meta['chunk_count'] == size_mb + 1 and meta['chunks'][-1]['size'] == len(tail), 'Chunking mismatch')
                    require(replicated(exact=True), 'Replication factor not met')
                    require({r['worker_id'] for c in meta['chunks'] for r in c['replicas']} == {f'worker{i}' for i in range(1, 5)}, 'Distribution mismatch')
                    require(identity in {o['object_id'] for o in get_json('/api/v1/objects')['objects']}, 'List omitted object')
                    response = client.patch(f'/api/v1/objects/{identity}/metadata', json={'purpose': 'patched'})
                    response.raise_for_status()
                    require(response.json()['metadata']['purpose'] == 'patched' and response.json()['checksum'] == expected, 'Metadata PATCH mismatch')
                    download(expected, size)
                    if size_mb == 100:
                        compose('stop', 'worker2')
                        download(expected, size)
                        wait_for(lambda: not next(w['available'] for w in get_json('/api/v1/status')['workers'] if w['worker_id'] == 'worker2'), 'worker2 failure detected')
                        wait_for(replicated, 'replicas restored with worker2 stopped')
                        compose('start', 'worker2')
                        wait_for(lambda: all(w['available'] for w in get_json('/api/v1/status')['workers']), 'worker2 returned')
                        wait_for(lambda: replicated(exact=True), 'surplus copies reconciled')
                        chunk = metadata()['chunks'][0]
                        worker = chunk['replicas'][0]['worker_id']
                        compose('exec', '-T', worker, 'python', '-c',
                            "import pathlib,sys; pathlib.Path('/data',sys.argv[1]+'.chunk').write_bytes(b'fresh-test-corruption')", chunk['chunk_id'])
                        download(expected, size)
                        wait_for(lambda: replicated(exact=True), 'corrupted replica repaired')
                        code = '''
import json, sys, urllib.request
expected = json.loads(sys.argv[1])
found = {identity: [] for identity in expected}
for i in range(1, 5):
    with urllib.request.urlopen(f'http://worker{i}:8000/internal/workers/worker{i}/chunks') as response:
        for item in json.load(response)['chunks']:
            if item['chunk_id'] in found:
                found[item['chunk_id']].append(item)
for identity, items in found.items():
    if len(items) != 2 or any(not item['valid'] or item['checksum'] != expected[identity] for item in items):
        raise RuntimeError('Physical replica verification failed: ' + identity)
print('PASS: every chunk has exactly two independently verified physical copies')
'''
                        print(compose('exec', '-T', 'controller', 'python', '-c', code,
                                      json.dumps({c['chunk_id']: c['checksum'] for c in metadata()['chunks']})), end='', flush=True)
                        compose('restart', 'controller')
                        # Docker may allocate a different ephemeral host port on restart.
                        address = compose('port', 'controller', '8000').strip()
                        client.base_url = f'http://{address}'
                        print(f'Resolved restarted controller at {address}', flush=True)
                        wait_for(lambda: client.get('/health', timeout=5).status_code == 200,
                                 'controller restarted with persisted metadata')
                        download(expected, size)
                    require(client.delete(f'/api/v1/objects/{identity}').status_code == 204, 'Delete failed')
                    wait_for(lambda: get_json('/api/v1/status')['pending_cleanup_objects'] == 0, 'test object cleanup completed')
                    require(client.get(f'/api/v1/objects/{identity}').status_code == 404, 'Deleted object remains visible')
                    identity = None
                    path.unlink()
                print('PASS: isolated fresh-deployment submission verification', flush=True)
        except BaseException:
            print(compose('logs', '--tail', '5'), file=sys.stderr)
            raise
        finally:
            # All names and resources were generated uniquely above, never the normal project.
            compose('down', '--volumes', '--remove-orphans')
            print(f'Removed only temporary deployment {project}; existing project preserved.', flush=True)


if __name__ == '__main__':
    main()
