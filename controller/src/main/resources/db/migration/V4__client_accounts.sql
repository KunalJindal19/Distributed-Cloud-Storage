-- Client accounts. They only concern the public API (/api/v1): which client may see which object.
-- The worker protocol, chunk ids, placement and replication know nothing about them.

-- One row per account. Emails are stored lower-case. Exactly one account is the admin (ADMIN_EMAIL,
-- default admin@gmail.com); it is created, or its password updated, at every start-up.
CREATE TABLE users (
    user_id       TEXT    PRIMARY KEY,
    email         TEXT    NOT NULL UNIQUE,
    password_hash TEXT    NOT NULL,
    is_admin      INTEGER NOT NULL DEFAULT 0 CHECK (is_admin IN (0, 1)),
    created_at    INTEGER NOT NULL
);

-- Signed-in sessions. Only the SHA-256 of a session token is stored, never the token itself.
CREATE TABLE sessions (
    token_hash TEXT    PRIMARY KEY,
    user_id    TEXT    NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);

CREATE INDEX ix_sessions_user ON sessions (user_id);

-- The account that uploaded the object. NULL only for objects stored before accounts existed;
-- the start-up gives those to the admin.
ALTER TABLE objects ADD COLUMN owner_id TEXT REFERENCES users (user_id);

CREATE INDEX ix_objects_owner ON objects (owner_id, created_at DESC, object_id);
