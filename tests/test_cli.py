"""CLI HTTP contracts, safe verified downloads, pagination and terminal output."""
import hashlib
from io import StringIO
import json
from uuid import uuid4

import httpx
import pytest

from object_store.cli import main, parser, render

OBJECT = str(uuid4())
DATA = b'file bytes\x00\xff'
META = {'object_id': OBJECT, 'name': 'sample.bin', 'size': len(DATA), 'checksum': hashlib.sha256(DATA).hexdigest(),
        'content_type': 'application/octet-stream', 'chunk_count': 1, 'metadata': {'owner': 'student'},
        'chunks': [{'position': 0, 'chunk_id': str(uuid4()), 'size': len(DATA), 'replicas': [
            {'worker_id': 'worker1', 'state': 'healthy', 'available': True}]}]}
STATUS = {'workers': [{'worker_id': 'worker1', 'available': True, 'membership_state': 'ready',
                      'free_bytes': 1000000000, 'last_seen': '2026-11-15T00:00:00Z', 'scan_progress': None}],
          'replication_factor': 2, 'pending_cleanup_objects': 0, 'repair_jobs': []}


def invoke(args, handler):
    output, errors = StringIO(), StringIO()
    code = main(args, transport=httpx.MockTransport(handler), stdout=output, stderr=errors)
    return code, output.getvalue(), errors.getvalue()


def handler(request):
    if request.url.path.endswith('/status'):
        return httpx.Response(200, json=STATUS)
    if request.method == 'DELETE':
        return httpx.Response(204)
    if request.url.path.endswith('/metadata') or request.method == 'POST':
        return httpx.Response(200, json=META)
    if request.url.path.endswith('/objects'):
        return httpx.Response(200, json={'objects': [META]})
    return httpx.Response(200, content=DATA)


@pytest.mark.parametrize('command', ['metadata', 'placements', 'delete'])
def test_object_commands(command):
    seen = []
    def record(request):
        seen.append((request.method, request.url.path))
        return handler(request)
    code, output, errors = invoke([command, OBJECT, '--json'], record)
    assert code == 0 and not errors
    assert json.loads(output)['object_id'] == OBJECT
    assert seen[0][0] == ('DELETE' if command == 'delete' else 'GET')


@pytest.mark.parametrize('command', ['status', 'scans', 'repairs', 'list'])
def test_inspection_commands_are_clean_and_read_only(command):
    def read_only(request):
        assert request.method == 'GET'
        return handler(request)
    code, output, errors = invoke([command], read_only)
    assert code == 0 and output.strip() and not errors
    assert '\x1b' not in output and '---' not in output


def test_upload_and_patch_metadata_contract(tmp_path):
    path = tmp_path / 'a file.bin'
    path.write_bytes(DATA)
    def upload(request):
        assert request.method == 'POST'
        assert 'multipart/form-data' in request.headers['content-type']
        body = request.read()
        assert DATA in body and b'"owner": "a=b"' in body
        return httpx.Response(201, json=META)
    code, output, errors = invoke(['upload', str(path), '--meta', 'owner=a=b', '--json'], upload)
    assert code == 0 and not errors and json.loads(output)['object_id'] == OBJECT
    def patch(request):
        assert request.method == 'PATCH'
        assert json.loads(request.read()) == {'owner': 'updated'}
        return httpx.Response(200, json=META)
    assert invoke(['patch', OBJECT, '--meta', 'owner=updated'], patch)[0] == 0


def test_download_is_verified_and_does_not_overwrite_by_default(tmp_path):
    output = tmp_path / 'saved.bin'
    code, text, errors = invoke(['download', OBJECT, '-o', str(output), '--json'], handler)
    assert code == 0 and not errors and json.loads(text)['verified']
    assert output.read_bytes() == DATA
    code, _, errors = invoke(['download', OBJECT, '-o', str(output)], handler)
    assert code == 1 and '--force' in errors
    assert output.read_bytes() == DATA and not list(tmp_path.glob('*.part'))


@pytest.mark.parametrize('content', [b'bad', b'x' * 30])
def test_bad_download_never_replaces_existing_file(tmp_path, content):
    output = tmp_path / 'saved.bin'
    output.write_bytes(b'keep me')
    def corrupted(request):
        return handler(request) if request.url.path.endswith('/metadata') else httpx.Response(200, content=content)
    code, _, errors = invoke(['download', OBJECT, '-o', str(output), '--force'], corrupted)
    assert code == 1 and errors
    assert output.read_bytes() == b'keep me'
    assert list(tmp_path.iterdir()) == [output]


def test_interrupted_download_removes_temporary_file(tmp_path):
    class Interrupted(httpx.SyncByteStream):
        def __iter__(self):
            yield b'file'
            raise httpx.ReadError('connection lost')
    def broken(request):
        return handler(request) if request.url.path.endswith('/metadata') else httpx.Response(200, stream=Interrupted())
    code, _, errors = invoke(['download', OBJECT, '-o', str(tmp_path / 'new.bin')], broken)
    assert code == 1 and 'connection lost' in errors
    assert list(tmp_path.iterdir()) == []


def test_pagination_and_options_before_or_after_command():
    offsets = []
    def paged(request):
        assert request.url.host == 'example.test'
        offsets.append(int(request.url.params['offset']))
        return httpx.Response(200, json={'objects': [META] if len(offsets) < 3 else []})
    code, output, errors = invoke(['--json', '--url', 'http://example.test', 'list', '--all', '--limit', '1'], paged)
    assert code == 0 and not errors and len(json.loads(output)['objects']) == 2
    assert offsets == [0, 1, 2]
    args = parser().parse_args(['status', '--json', '--url', 'http://example.test'])
    assert args.json and args.url == 'http://example.test'


@pytest.mark.parametrize('args', [['patch', OBJECT], ['patch', OBJECT, '--meta', 'invalid'], ['list', '--limit', '0']])
def test_local_validation_does_not_send_request(args):
    def unexpected(request):
        pytest.fail('Invalid command must not contact server')
    code, output, errors = invoke(args, unexpected)
    assert code == 1 and not output and errors.startswith('Error:')


def test_errors_use_stderr_and_json_mode_has_no_decoration():
    def missing(request):
        return httpx.Response(404, json={'error': {'message': 'Object not found'}})
    code, output, errors = invoke(['metadata', OBJECT, '--json'], missing)
    assert code == 1 and not output and json.loads(errors)['error'] == '404: Object not found'
    def unavailable(request):
        raise httpx.ConnectError('Cannot reach controller')
    assert invoke(['status'], unavailable)[0] == 1


def test_terminal_control_sequences_are_not_rendered():
    text = render('metadata', {**META, 'name': '\x1b[31mfile\nname'})
    assert '\x1b' not in text and 'file\nname' not in text


def test_empty_download_and_long_destination_name(tmp_path):
    destination = tmp_path / ('a' * 251 + '.bin')
    def empty(request):
        if request.url.path.endswith('/metadata'):
            return httpx.Response(200, json={**META, 'size': 0, 'checksum': hashlib.sha256(b'').hexdigest()})
        return httpx.Response(200, content=b'')
    code, _, errors = invoke(['download', OBJECT, '-o', str(destination)], empty)
    assert code == 0 and not errors and destination.read_bytes() == b''


def test_force_replaces_only_with_verified_bytes(tmp_path):
    destination = tmp_path / 'old.bin'
    destination.write_bytes(b'old')
    code, _, errors = invoke(['download', OBJECT, '-o', str(destination), '--force'], handler)
    assert code == 0 and not errors and destination.read_bytes() == DATA
    assert list(tmp_path.iterdir()) == [destination]


def test_invalid_controller_url_is_a_clean_error():
    code, output, errors = invoke(['--url', 'http://localhost:bad', 'status'], handler)
    assert code == 1 and not output and errors.startswith('Error:')
    assert 'Traceback' not in errors
