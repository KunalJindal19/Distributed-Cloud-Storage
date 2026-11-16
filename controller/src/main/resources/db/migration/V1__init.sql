-- Metadata schema of the controller. Everything the controller knows lives here, so it can be
-- restarted at any time. All timestamps are UTC, stored as milliseconds since the Unix epoch.

-- One row per storage worker that has ever registered.
CREATE TABLE workers (
    worker_id     TEXT    PRIMARY KEY,
    url           TEXT    NOT NULL,
    status        TEXT    NOT NULL CHECK (status IN ('ALIVE', 'DEAD')),
    last_seen     INTEGER NOT NULL,
    registered_at INTEGER NOT NULL
);

-- One row per stored object: the system-maintained metadata.
CREATE TABLE objects (
    object_id    TEXT    PRIMARY KEY,
    name         TEXT    NOT NULL,
    size         INTEGER NOT NULL CHECK (size >= 0),
    content_type TEXT    NOT NULL,
    checksum     TEXT    NOT NULL,
    chunk_count  INTEGER NOT NULL CHECK (chunk_count >= 0),
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
);

CREATE INDEX ix_objects_created_at ON objects (created_at DESC, object_id);

-- User-defined key/value metadata.
CREATE TABLE object_metadata (
    object_id  TEXT NOT NULL REFERENCES objects (object_id) ON DELETE CASCADE,
    meta_key   TEXT NOT NULL,
    meta_value TEXT NOT NULL,
    PRIMARY KEY (object_id, meta_key)
);

-- One row per chunk; idx preserves the order of the chunks inside the object.
CREATE TABLE chunks (
    chunk_id  TEXT    PRIMARY KEY,
    object_id TEXT    NOT NULL REFERENCES objects (object_id) ON DELETE CASCADE,
    idx       INTEGER NOT NULL CHECK (idx >= 0),
    size      INTEGER NOT NULL CHECK (size > 0),
    checksum  TEXT    NOT NULL,
    UNIQUE (object_id, idx)
);

-- One row per physical copy of a chunk: which worker holds it and what state it is in.
-- The primary key makes two copies of one chunk on the same worker impossible.
CREATE TABLE chunk_replicas (
    chunk_id   TEXT    NOT NULL REFERENCES chunks (chunk_id) ON DELETE CASCADE,
    worker_id  TEXT    NOT NULL REFERENCES workers (worker_id),
    status     TEXT    NOT NULL CHECK (status IN ('OK', 'CORRUPT', 'MISSING')),
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (chunk_id, worker_id)
);

CREATE INDEX ix_replicas_worker_id ON chunk_replicas (worker_id);
CREATE INDEX ix_replicas_status ON chunk_replicas (status);
