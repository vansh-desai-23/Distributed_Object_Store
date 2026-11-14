"""Durable worker expectations, ordered mutation receipts, scan cursor and outbox."""
from contextlib import contextmanager
import json
import sqlite3
from uuid import uuid4

from fastapi import HTTPException


class Catalog:
    def __init__(self, root):
        self.path = root / 'catalog.sqlite'
        with self.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS chunks (
                    id TEXT PRIMARY KEY, checksum TEXT NOT NULL, size INTEGER NOT NULL,
                    state TEXT NOT NULL, sequence INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS operations (
                    chunk_id TEXT PRIMARY KEY, sequence INTEGER NOT NULL,
                    signature TEXT NOT NULL, state TEXT NOT NULL, receipt TEXT);
                CREATE TABLE IF NOT EXISTS damage (
                    chunk_id TEXT PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
                    sequence INTEGER NOT NULL, reason TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS scan (
                    id INTEGER PRIMARY KEY CHECK(id=1), cursor TEXT NOT NULL DEFAULT '',
                    ceiling TEXT NOT NULL DEFAULT '', cycles INTEGER NOT NULL DEFAULT 0,
                    checked INTEGER NOT NULL DEFAULT 0);
                INSERT OR IGNORE INTO scan(id) VALUES(1);
            ''')

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:
                yield db
        finally:
            db.close()

    def expected(self, identity):
        with self.connect() as db:
            row = db.execute('SELECT * FROM chunks WHERE id=?', (identity,)).fetchone()
            return dict(row) if row else None

    def expect(self, identity, expected, size, sequence=0, state='pending'):
        with self.connect() as db:
            row = db.execute('SELECT * FROM chunks WHERE id=?', (identity,)).fetchone()
            if row and (row['checksum'] != expected or row['size'] != size):
                raise HTTPException(409, 'Chunk conflicts with trusted catalog expectation')
            db.execute('INSERT INTO chunks VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state, sequence=excluded.sequence',
                       (identity, expected, size, state, sequence))

    def state(self, identity, state):
        with self.connect() as db:
            db.execute('UPDATE chunks SET state=? WHERE id=?', (state, identity))

    def begin(self, identity, sequence, signature):
        with self.connect() as db:
            row = db.execute('SELECT * FROM operations WHERE chunk_id=?', (identity,)).fetchone()
            if row and (sequence < row['sequence'] or (sequence == row['sequence'] and signature != row['signature'])):
                raise HTTPException(409, 'Stale or conflicting operation sequence')
            if row and sequence == row['sequence'] and row['state'] == 'done':
                return json.loads(row['receipt'])
            db.execute('INSERT INTO operations VALUES(?,?,?,\'pending\',NULL) ON CONFLICT(chunk_id) DO UPDATE SET sequence=excluded.sequence, signature=excluded.signature, state=\'pending\', receipt=NULL',
                       (identity, sequence, signature))
        return None

    def finish(self, identity, receipt):
        with self.connect() as db:
            db.execute("UPDATE operations SET state='done',receipt=? WHERE chunk_id=?", (json.dumps(receipt), identity))

    def operation(self, identity):
        with self.connect() as db:
            row = db.execute('SELECT * FROM operations WHERE chunk_id=?', (identity,)).fetchone()
            return dict(row) if row else None

    def damage(self, identity, sequence, reason):
        with self.connect() as db:
            row = db.execute('SELECT sequence FROM damage WHERE chunk_id=?', (identity,)).fetchone()
            if row and row['sequence'] == sequence:
                return
            db.execute('INSERT INTO damage VALUES(?,?,?,?) ON CONFLICT(chunk_id) DO UPDATE SET event_id=excluded.event_id,sequence=excluded.sequence,reason=excluded.reason',
                       (identity, str(uuid4()), sequence, reason))

    def acknowledge(self, ids):
        with self.connect() as db:
            db.executemany('DELETE FROM damage WHERE event_id=?', [(identity,) for identity in ids])

    def report(self):
        with self.connect() as db:
            scan = dict(db.execute('SELECT * FROM scan WHERE id=1').fetchone())
            scan.pop('id')
            scan['pending_damage'] = db.execute('SELECT count(*) FROM damage').fetchone()[0]
            events = [dict(row) for row in db.execute('SELECT * FROM damage ORDER BY event_id LIMIT 100')]
            return {'scan_progress': scan, 'damage_reports': events}

    def scan_ids(self, limit):
        with self.connect() as db:
            scan = db.execute('SELECT * FROM scan WHERE id=1').fetchone()
            ceiling = scan['ceiling']
            if not ceiling:
                ceiling = db.execute("SELECT max(id) FROM chunks WHERE state IN ('pending','committed')").fetchone()[0] or ''
                db.execute('UPDATE scan SET ceiling=? WHERE id=1', (ceiling,))
            return [row[0] for row in db.execute("SELECT id FROM chunks WHERE state IN ('pending','committed') AND id>? AND id<=? ORDER BY id LIMIT ?", (scan['cursor'], ceiling, limit))]

    def scanned(self, identity):
        with self.connect() as db:
            db.execute('UPDATE scan SET cursor=?,checked=checked+1 WHERE id=1', (identity,))

    def complete_cycle(self):
        with self.connect() as db:
            scan = db.execute('SELECT * FROM scan WHERE id=1').fetchone()
            remaining = db.execute("SELECT 1 FROM chunks WHERE state IN ('pending','committed') AND id>? AND id<=? LIMIT 1", (scan['cursor'], scan['ceiling'])).fetchone()
            if not remaining:
                db.execute("UPDATE scan SET cursor='',ceiling='',cycles=cycles+1 WHERE id=1")
