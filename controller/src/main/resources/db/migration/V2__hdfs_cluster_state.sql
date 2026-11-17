-- Replica locations and worker liveness are no longer persisted: like the HDFS NameNode, the
-- controller keeps them in memory and rebuilds them from the workers' block reports after every
-- start. The database holds only the namespace.
DROP TABLE IF EXISTS chunk_replicas;
DROP TABLE IF EXISTS workers;

-- One row: key = 'cluster_id'. Workers record it in their VERSION file; a worker from another
-- cluster (or one whose controller database was wiped) is refused, which protects its data.
CREATE TABLE cluster_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Idempotency-Key of an upload -> the object it created. A client that retries an upload after
-- a lost response gets the original object back instead of creating a duplicate.
CREATE TABLE idempotency_keys (
    key        TEXT    PRIMARY KEY,
    object_id  TEXT    NOT NULL REFERENCES objects (object_id) ON DELETE CASCADE,
    size       INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX ix_idempotency_object ON idempotency_keys (object_id);
