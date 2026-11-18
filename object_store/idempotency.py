"""Durable request identity and replay for single-controller multipart uploads."""
import asyncio
from collections import defaultdict
import hashlib
import json
import re

from fastapi import HTTPException
from sqlalchemy import select

from object_store.common import CHUNK_SIZE
from object_store.models import StoredObject, UploadReceipt


def validate_key(key):
    if not isinstance(key, str) or not re.fullmatch(r'[A-Za-z0-9._:-]{1,128}', key):
        raise HTTPException(400, 'Idempotency-Key must contain 1–128 letters, digits, dots, underscores, colons or hyphens')
    return key


def upload_attributes(file):
    name = (file.filename or 'object').replace('\\', '/').split('/')[-1] or 'object'
    content_type = file.content_type or 'application/octet-stream'
    if len(name) > 255 or len(content_type) > 255 or any(ord(c) < 32 or ord(c) > 126 for c in content_type):
        raise HTTPException(400, 'Invalid filename or content type')
    return name, content_type


def fingerprint(name, content_type, metadata, size, checksum):
    body = json.dumps([1, name, content_type, metadata, size, checksum], sort_keys=True,
                      separators=(',', ':'), ensure_ascii=True).encode()
    return hashlib.sha256(body).hexdigest()


class UploadIdempotency:
    def __init__(self, store):
        self.store = store
        self.locks = defaultdict(asyncio.Lock)

    async def upload(self, file, metadata, key):
        key = validate_key(key)
        name, content_type = upload_attributes(file)
        digest, size = hashlib.sha256(), 0
        # UploadFile is already disk-spooled by the multipart parser. Hash in bounded
        # blocks and rewind; no complete object is retained in RAM.
        while block := await file.read(CHUNK_SIZE):
            digest.update(block)
            size += len(block)
        await file.seek(0)
        expected = fingerprint(name, content_type, metadata, size, digest.hexdigest())
        async with self.locks[key]:
            with self.store.sessions() as session:
                receipt = session.get(UploadReceipt, key)
                if receipt and receipt.fingerprint != expected:
                    raise HTTPException(409, 'Idempotency key was already used for a different upload')
            if receipt and receipt.state == 'completed':
                async with self.store.locks[receipt.object_id]:
                    with self.store.sessions() as session:
                        obj = session.get(StoredObject, receipt.object_id)
                        if obj is None or obj.state != 'available':
                            raise HTTPException(410, 'The original upload was deleted; use a new key for a new object')
                    return receipt.response, True
            result = await self.store.upload(file, metadata, idempotency=(key, expected))
            return result, False

    @staticmethod
    def claim(session, key, expected, identity):
        receipt = session.scalar(select(UploadReceipt).where(UploadReceipt.key == key).with_for_update())
        if receipt:
            if receipt.fingerprint != expected:
                raise HTTPException(409, 'Idempotency key was already used for a different upload')
            if receipt.state == 'completed':
                raise HTTPException(409, 'Upload completed concurrently; retry with the same key')
            old = session.get(StoredObject, receipt.object_id)
            if receipt.state == 'pending' and old and old.state == 'uploading':
                raise HTTPException(409, 'Upload is still in progress; retry with the same key later')
            receipt.object_id, receipt.state, receipt.response = identity, 'pending', None
        else:
            session.add(UploadReceipt(key=key, fingerprint=expected, object_id=identity, state='pending'))
