package storage

import (
	"context"
	"database/sql"
	"fmt"

	"trace/model"
)

// schema creates canonical tables and rebuildable lookup indexes
const schema = `
CREATE TABLE IF NOT EXISTS trace_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS namespace (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS provenance (
    id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_id TEXT,
    source_uri TEXT,
    agent TEXT,
    model TEXT,
    provider TEXT,
    conversation_id TEXT,
    message_id TEXT,
    operation TEXT,
    created_at TEXT NOT NULL,
    parent_provenance_ids TEXT NOT NULL,
    extensions TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    payload_content_type TEXT NOT NULL,
    occurred_at TEXT,
    recorded_at TEXT NOT NULL,
    namespace TEXT NOT NULL REFERENCES namespace(id),
    actor TEXT,
    provenance_id TEXT NOT NULL REFERENCES provenance(id),
    content_hash TEXT NOT NULL,
    extensions TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS memory (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('fact', 'observation', 'context')),
    content TEXT NOT NULL,
    structured_value TEXT NOT NULL,
    subject_entity_id TEXT,
    predicate TEXT,
    object_entity_id TEXT,
    object_value TEXT,
    valid_from TEXT,
    valid_to TEXT,
    recorded_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('active', 'superseded', 'uncertain', 'invalidated', 'redacted')
    ),
    namespace TEXT NOT NULL REFERENCES namespace(id),
    confidence TEXT NOT NULL,
    provenance_id TEXT NOT NULL REFERENCES provenance(id),
    content_hash TEXT NOT NULL,
    extensions TEXT NOT NULL,
    CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to > valid_from),
    CHECK (content <> '' OR structured_value <> '{}')
);

CREATE TABLE IF NOT EXISTS entity (
    id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    aliases TEXT NOT NULL,
    namespace TEXT NOT NULL REFERENCES namespace(id),
    provenance_id TEXT NOT NULL REFERENCES provenance(id),
    content_hash TEXT NOT NULL,
    extensions TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS relationship (
    id TEXT PRIMARY KEY,
    relationship_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    valid_from TEXT,
    valid_to TEXT,
    provenance_id TEXT REFERENCES provenance(id),
    extensions TEXT NOT NULL,
    CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to > valid_from)
);

CREATE TABLE IF NOT EXISTS derivation (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    relation TEXT NOT NULL,
    created_at TEXT NOT NULL,
    actor TEXT,
    extensions TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mutation (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    commit_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    target_id TEXT NOT NULL,
    actor TEXT,
    created_at TEXT NOT NULL,
    request_hash TEXT,
    metadata TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policy (
    id TEXT PRIMARY KEY,
    effect TEXT NOT NULL CHECK (effect IN ('allow', 'deny')),
    principal TEXT NOT NULL,
    operation TEXT NOT NULL,
    resource_selector TEXT NOT NULL,
    conditions TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT
);

CREATE TABLE IF NOT EXISTS policy_binding (
    policy_id TEXT NOT NULL REFERENCES policy(id),
    namespace TEXT NOT NULL REFERENCES namespace(id),
    PRIMARY KEY (policy_id, namespace)
);

CREATE TABLE IF NOT EXISTS deletion (
    id TEXT PRIMARY KEY,
    target_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    actor TEXT NOT NULL,
    created_at TEXT NOT NULL,
    details TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS event_namespace_recorded_at
    ON event (namespace, recorded_at);
CREATE INDEX IF NOT EXISTS memory_namespace_recorded_at
    ON memory (namespace, recorded_at);
CREATE INDEX IF NOT EXISTS memory_valid_interval
    ON memory (valid_from, valid_to);
CREATE INDEX IF NOT EXISTS derivation_source
    ON derivation (source_id);
CREATE INDEX IF NOT EXISTS derivation_target
    ON derivation (target_id);
`

// createSchema applies the initial schema in one transaction
func createSchema(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin schema transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("create trace schema: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		fmt.Sprintf("PRAGMA user_version = %d", model.SchemaVersion),
	); err != nil {
		return fmt.Errorf("set schema version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit trace schema: %w", err)
	}
	return nil
}
