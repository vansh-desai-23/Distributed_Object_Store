"""Exercise every operational CLI command against the running local controller."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    def cli(*arguments):
        result = subprocess.run([sys.executable, '-m', 'object_store.cli', '--json', *arguments],
                                cwd=ROOT, capture_output=True, text=True, timeout=180)
        if result.returncode:
            raise RuntimeError(result.stderr)
        return json.loads(result.stdout)
    identity = None
    with tempfile.TemporaryDirectory(prefix='store-cli-') as directory:
        source, target = Path(directory) / 'binary file.bin', Path(directory) / 'download.bin'
        source.write_bytes(bytes(range(256)) * 3907)
        expected = hashlib.sha256(source.read_bytes()).hexdigest()
        try:
            uploaded = cli('upload', str(source), '--meta', 'purpose=cli-check')
            identity = uploaded['object_id']
            assert uploaded['checksum'] == expected
            assert identity in {obj['object_id'] for obj in cli('list', '--all', '--limit', '2')['objects']}
            assert cli('metadata', identity)['metadata']['purpose'] == 'cli-check'
            assert cli('patch', identity, '--meta', 'purpose=updated')['metadata']['purpose'] == 'updated'
            assert len(cli('placements', identity)['chunks']) == 2
            assert cli('download', identity, '-o', str(target))['verified']
            assert hashlib.sha256(target.read_bytes()).hexdigest() == expected
            for command in ('status', 'scans', 'repairs'):
                assert len(cli(command)['workers']) == 4
            print('PASS: CLI upload, paginated list, metadata, PATCH, placements and verified download', flush=True)
            print('PASS: CLI status, scan and repair inspection', flush=True)
        finally:
            if identity:
                assert cli('delete', identity)['deleted']
                print('PASS: CLI delete; only this test object removed', flush=True)


if __name__ == '__main__':
    main()
