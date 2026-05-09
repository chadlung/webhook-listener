CREATE TABLE endpoints (
    id           TEXT PRIMARY KEY,
    label        TEXT NOT NULL,
    description  TEXT NOT NULL DEFAULT '',
    created_at   INTEGER NOT NULL
);

CREATE TABLE webhooks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint_id  TEXT NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    received_at  INTEGER NOT NULL,
    method       TEXT NOT NULL,
    path         TEXT NOT NULL,
    query        TEXT NOT NULL DEFAULT '',
    source_ip    TEXT NOT NULL,
    headers      TEXT NOT NULL,
    body         BLOB NOT NULL,
    body_size    INTEGER NOT NULL
);

CREATE INDEX webhooks_endpoint_received
    ON webhooks(endpoint_id, received_at DESC);
