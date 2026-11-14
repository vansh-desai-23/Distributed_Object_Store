"""Verify generated API docs expose the actual upload/PATCH/download contracts."""

async def test_client_openapi_has_usable_body_contracts(cluster):
    response = await cluster.client.get('/openapi.json')
    assert response.status_code == 200
    paths = response.json()['paths']
    upload = paths['/api/v1/objects']['post']['requestBody']['content']['multipart/form-data']['schema']
    assert upload['required'] == ['file']
    assert upload['properties']['file']['format'] == 'binary'
    assert upload['properties']['metadata']['type'] == 'string'
    patch = paths['/api/v1/objects/{object_id}/metadata']['patch']['requestBody']['content']['application/json']['schema']
    assert patch['additionalProperties']['type'] == 'string'
    download = paths['/api/v1/objects/{object_id}']['get']['responses']['200']['content']
    assert download['application/octet-stream']['schema']['format'] == 'binary'
    assert 'application/json' not in download
    responses = paths['/api/v1/objects']['get']['responses']
    assert '400' in responses and '422' not in responses
    assert (await cluster.client.get('/api/v1/objects?limit=0')).status_code == 400


def test_worker_openapi_has_binary_put_contract(cluster):
    schema = cluster.workers['worker1'].openapi()
    operation = schema['paths']['/internal/chunks/{chunk_id}']['put']
    assert operation['requestBody']['content']['application/octet-stream']['schema']['format'] == 'binary'
    assert any(p['name'] == 'x-chunk-checksum' and p['required'] for p in operation['parameters'])
