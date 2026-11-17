-- Orphan chunks the controller can account for. The orphan safety valve refuses to delete large
-- numbers of chunks the controller knows nothing about (a bug, a restored old database), but chunks
-- of these objects are certainly garbage and are always collected:
--
-- upload_intents: written before an upload's first chunk, deleted in the transaction that commits
--   the object. A row that remains belongs to an upload that failed, or was cut short by a crash
--   (like a file left "under construction" in the HDFS edit log).
CREATE TABLE upload_intents (
    object_id  TEXT    PRIMARY KEY,
    started_at INTEGER NOT NULL
);

-- deleted_objects: a tombstone per deleted object, written in the transaction that deletes it.
-- Copies on workers that were unreachable at the time are recognised when they are reported.
CREATE TABLE deleted_objects (
    object_id  TEXT    PRIMARY KEY,
    deleted_at INTEGER NOT NULL
);
