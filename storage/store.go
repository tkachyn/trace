// storage provides the v0.1 SQLite-backed Trace file implementation
package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"trace/model"

	_ "modernc.org/sqlite"
)

// store owns one Trace file connection and its write policy
type Store struct {
	db       *sql.DB
	path     string
	readOnly bool
}

// inspection exposes the manifest and record counts for diagnostics
type Inspection struct {
	Manifest model.Manifest
	Counts   map[string]int
}

// validationReport contains invariant failures found in a Trace file
type ValidationReport struct {
	Valid    bool
	Counts   map[string]int
	Errors   []string
	Warnings []string
}

// init creates a new Trace file without replacing an existing file
func Init(ctx context.Context, path, generator string) error {
	if path == "" {
		return errors.New("trace path is required")
	}
	if info, err := os.Stat(path); err == nil && info.Size() > 0 {
		return fmt.Errorf("trace file already exists: %s", path)
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("check trace path: %w", err)
	}

	if parent := filepath.Dir(path); parent != "." {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			return fmt.Errorf("create trace directory: %w", err)
		}
	}

	db, err := openDatabase(path, false)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := configureWriteDatabase(db); err != nil {
		return err
	}
	if _, err := db.Exec(fmt.Sprintf("PRAGMA application_id = %d", model.ApplicationID)); err != nil {
		return fmt.Errorf("set SQLite application ID: %w", err)
	}
	if err := createSchema(ctx, db); err != nil {
		return err
	}

	fileID, err := model.NewID()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if generator == "" {
		generator = model.DefaultGenerator
	}
	requiredFeatures, err := json.Marshal([]string{})
	if err != nil {
		return fmt.Errorf("encode required features: %w", err)
	}

	metadata := map[string]string{
		"format_name":       model.FormatName,
		"format_version":    model.FormatVersion,
		"schema_version":    fmt.Sprintf("%d", model.SchemaVersion),
		"created_at":        model.FormatTime(now),
		"updated_at":        model.FormatTime(now),
		"generator":         generator,
		"file_id":           fileID,
		"required_features": string(requiredFeatures),
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin manifest transaction: %w", err)
	}
	defer tx.Rollback()
	for key, value := range metadata {
		if _, err := tx.ExecContext(
			ctx,
			"INSERT INTO trace_meta (key, value) VALUES (?, ?)",
			key,
			value,
		); err != nil {
			return fmt.Errorf("write manifest field %q: %w", key, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit trace manifest: %w", err)
	}
	return nil
}

// open verifies the Trace manifest before enabling write settings
func Open(ctx context.Context, path string, readOnly bool) (*Store, error) {
	if path == "" {
		return nil, errors.New("trace path is required")
	}
	if readOnly {
		if _, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("open trace file: %w", err)
		}
	}

	db, err := openDatabase(path, readOnly)
	if err != nil {
		return nil, err
	}
	store := &Store{db: db, path: path, readOnly: readOnly}
	if _, err := store.manifestValue(ctx, "format_name"); err != nil {
		db.Close()
		return nil, fmt.Errorf("open trace manifest: %w", err)
	}
	if !readOnly {
		if err := configureWriteDatabase(db); err != nil {
			db.Close()
			return nil, err
		}
		if err := ensureRetrievalIndex(ctx, db); err != nil {
			db.Close()
			return nil, err
		}
	}
	return store, nil
}

// close releases the file connection
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// addEvent stores an immutable source event and its provenance atomically
func (s *Store) AddEvent(ctx context.Context, input model.EventInput) (model.Event, error) {
	if s.readOnly {
		return model.Event{}, errors.New("trace is read-only")
	}
	if input.EventType == "" {
		return model.Event{}, errors.New("event type is required")
	}
	if input.Namespace == "" {
		return model.Event{}, errors.New("event namespace is required")
	}
	if err := model.ValidateJSON(input.Payload); err != nil {
		return model.Event{}, fmt.Errorf("event payload: %w", err)
	}
	payload, err := model.CanonicalJSON(input.Payload, json.RawMessage(`{}`))
	if err != nil {
		return model.Event{}, fmt.Errorf("event payload: %w", err)
	}

	eventID := input.ID
	if eventID == "" {
		eventID, err = model.NewID()
		if err != nil {
			return model.Event{}, err
		}
	}
	recordedAt := input.RecordedAt
	if recordedAt.IsZero() {
		recordedAt = time.Now().UTC()
	}
	contentType := input.PayloadContentType
	if contentType == "" {
		contentType = "application/json"
	}
	provenance := input.Provenance
	if err := prepareProvenance(&provenance, eventID); err != nil {
		return model.Event{}, err
	}
	event := model.Event{
		ID:                 eventID,
		EventType:          input.EventType,
		Payload:            payload,
		PayloadContentType: contentType,
		OccurredAt:         input.OccurredAt,
		RecordedAt:         recordedAt.UTC(),
		Namespace:          input.Namespace,
		Actor:              input.Actor,
		ProvenanceID:       provenance.ID,
		Extensions:         model.JSONOrEmpty(input.Extensions),
	}
	hash, err := model.HashEvent(event)
	if err != nil {
		return model.Event{}, err
	}
	event.ContentHash = hash

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Event{}, fmt.Errorf("begin event transaction: %w", err)
	}
	defer tx.Rollback()
	if err := insertNamespace(ctx, tx, event.Namespace, event.RecordedAt); err != nil {
		return model.Event{}, err
	}
	if err := insertProvenance(ctx, tx, provenance); err != nil {
		return model.Event{}, err
	}
	var occurredAt any
	if event.OccurredAt != nil {
		occurredAt = model.FormatTime(*event.OccurredAt)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO event (
			id, event_type, payload, payload_content_type, occurred_at,
			recorded_at, namespace, actor, provenance_id, content_hash, extensions
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		event.ID,
		event.EventType,
		string(event.Payload),
		event.PayloadContentType,
		occurredAt,
		model.FormatTime(event.RecordedAt),
		event.Namespace,
		event.Actor,
		event.ProvenanceID,
		event.ContentHash,
		string(event.Extensions),
	); err != nil {
		return model.Event{}, fmt.Errorf("insert event: %w", err)
	}
	if err := insertRetrievalDocument(
		ctx,
		tx,
		event.ID,
		"event",
		event.Namespace,
		string(event.Payload),
		event.ContentHash,
	); err != nil {
		return model.Event{}, err
	}
	if _, err := insertMutation(ctx, tx, "add_event", event.ID, event.Actor, event.ContentHash); err != nil {
		return model.Event{}, err
	}
	if err := updateManifestTimestamp(ctx, tx, time.Now().UTC()); err != nil {
		return model.Event{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Event{}, fmt.Errorf("commit event: %w", err)
	}
	return event, nil
}

// addMemory stores a derived memory and its derivation links atomically
func (s *Store) AddMemory(ctx context.Context, input model.MemoryInput) (model.Memory, error) {
	if s.readOnly {
		return model.Memory{}, errors.New("trace is read-only")
	}
	if err := model.ValidateMemoryKind(input.Kind); err != nil {
		return model.Memory{}, err
	}
	if input.Namespace == "" {
		return model.Memory{}, errors.New("memory namespace is required")
	}
	status := input.Status
	if status == "" {
		status = "active"
	}
	if err := model.ValidateMemoryStatus(status); err != nil {
		return model.Memory{}, err
	}
	if input.Content == "" && len(input.StructuredValue) == 0 {
		return model.Memory{}, errors.New("memory content or structured value is required")
	}
	if err := model.ValidateJSON(input.StructuredValue); err != nil {
		return model.Memory{}, fmt.Errorf("memory structured value: %w", err)
	}
	if err := model.ValidateJSON(input.Confidence); err != nil {
		return model.Memory{}, fmt.Errorf("memory confidence: %w", err)
	}
	structuredValue, err := model.CanonicalJSON(input.StructuredValue, json.RawMessage(`{}`))
	if err != nil {
		return model.Memory{}, fmt.Errorf("memory structured value: %w", err)
	}
	confidence, err := model.CanonicalJSON(input.Confidence, json.RawMessage(`{}`))
	if err != nil {
		return model.Memory{}, fmt.Errorf("memory confidence: %w", err)
	}
	if err := model.ValidateInterval(input.ValidFrom, input.ValidTo); err != nil {
		return model.Memory{}, err
	}

	memoryID := input.ID
	if memoryID == "" {
		memoryID, err = model.NewID()
		if err != nil {
			return model.Memory{}, err
		}
	}
	recordedAt := input.RecordedAt
	if recordedAt.IsZero() {
		recordedAt = time.Now().UTC()
	}
	provenance := input.Provenance
	if err := prepareProvenance(&provenance, memoryID); err != nil {
		return model.Memory{}, err
	}
	memory := model.Memory{
		ID:              memoryID,
		Kind:            input.Kind,
		Content:         input.Content,
		StructuredValue: structuredValue,
		SubjectEntityID: input.SubjectEntityID,
		Predicate:       input.Predicate,
		ObjectEntityID:  input.ObjectEntityID,
		ObjectValue:     input.ObjectValue,
		ValidFrom:       input.ValidFrom,
		ValidTo:         input.ValidTo,
		RecordedAt:      recordedAt.UTC(),
		Status:          status,
		Namespace:       input.Namespace,
		Confidence:      confidence,
		ProvenanceID:    provenance.ID,
		Extensions:      model.JSONOrEmpty(input.Extensions),
	}
	hash, err := model.HashMemory(memory)
	if err != nil {
		return model.Memory{}, err
	}
	memory.ContentHash = hash

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Memory{}, fmt.Errorf("begin memory transaction: %w", err)
	}
	defer tx.Rollback()
	if err := insertNamespace(ctx, tx, memory.Namespace, memory.RecordedAt); err != nil {
		return model.Memory{}, err
	}
	if err := insertProvenance(ctx, tx, provenance); err != nil {
		return model.Memory{}, err
	}
	for _, sourceID := range input.DerivedFrom {
		if sourceID == "" {
			return model.Memory{}, errors.New("derived-from IDs must not be empty")
		}
		exists, err := recordExists(ctx, tx, sourceID)
		if err != nil {
			return model.Memory{}, err
		}
		if !exists {
			return model.Memory{}, fmt.Errorf("derived-from record does not exist: %s", sourceID)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO memory (
			id, kind, content, structured_value, subject_entity_id, predicate,
			object_entity_id, object_value, valid_from, valid_to, recorded_at,
			status, namespace, confidence, provenance_id, content_hash, extensions
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		memory.ID,
		memory.Kind,
		memory.Content,
		string(memory.StructuredValue),
		memory.SubjectEntityID,
		memory.Predicate,
		memory.ObjectEntityID,
		memory.ObjectValue,
		formatOptionalTime(memory.ValidFrom),
		formatOptionalTime(memory.ValidTo),
		model.FormatTime(memory.RecordedAt),
		memory.Status,
		memory.Namespace,
		string(memory.Confidence),
		memory.ProvenanceID,
		memory.ContentHash,
		string(memory.Extensions),
	); err != nil {
		return model.Memory{}, fmt.Errorf("insert memory: %w", err)
	}
	if err := insertRetrievalDocument(
		ctx,
		tx,
		memory.ID,
		"memory",
		memory.Namespace,
		memory.Content+" "+string(memory.StructuredValue),
		memory.ContentHash,
	); err != nil {
		return model.Memory{}, err
	}
	for _, sourceID := range input.DerivedFrom {
		derivationID, err := model.NewID()
		if err != nil {
			return model.Memory{}, err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO derivation (
				id, source_id, target_id, relation, created_at, actor, extensions
			) VALUES (?, ?, ?, ?, ?, ?, ?)
		`,
			derivationID,
			sourceID,
			memory.ID,
			"derived_from",
			model.FormatTime(memory.RecordedAt),
			provenance.Agent,
			`{}`,
		); err != nil {
			return model.Memory{}, fmt.Errorf("insert derivation: %w", err)
		}
	}
	if _, err := insertMutation(ctx, tx, "remember", memory.ID, provenance.Agent, memory.ContentHash); err != nil {
		return model.Memory{}, err
	}
	if err := updateManifestTimestamp(ctx, tx, time.Now().UTC()); err != nil {
		return model.Memory{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Memory{}, fmt.Errorf("commit memory: %w", err)
	}
	return memory, nil
}

// addEntity stores a canonical entity and its provenance atomically
func (s *Store) AddEntity(ctx context.Context, input model.EntityInput) (model.Entity, error) {
	if s.readOnly {
		return model.Entity{}, errors.New("trace is read-only")
	}
	if input.Namespace == "" {
		return model.Entity{}, errors.New("entity namespace is required")
	}
	if input.EntityType == "" || input.CanonicalName == "" {
		return model.Entity{}, errors.New("entity type and canonical name are required")
	}
	if err := model.ValidateJSON(input.Aliases); err != nil {
		return model.Entity{}, fmt.Errorf("entity aliases: %w", err)
	}
	aliases, err := model.CanonicalJSON(input.Aliases, json.RawMessage(`[]`))
	if err != nil {
		return model.Entity{}, fmt.Errorf("entity aliases: %w", err)
	}

	entityID := input.ID
	if entityID == "" {
		entityID, err = model.NewID()
		if err != nil {
			return model.Entity{}, err
		}
	}
	provenance := input.Provenance
	if err := prepareProvenance(&provenance, entityID); err != nil {
		return model.Entity{}, err
	}
	entity := model.Entity{
		ID:            entityID,
		EntityType:    input.EntityType,
		CanonicalName: input.CanonicalName,
		Aliases:       aliases,
		Namespace:     input.Namespace,
		ProvenanceID:  provenance.ID,
		Extensions:    model.JSONOrEmpty(input.Extensions),
	}
	hash, err := model.HashEntity(entity)
	if err != nil {
		return model.Entity{}, err
	}
	entity.ContentHash = hash

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Entity{}, fmt.Errorf("begin entity transaction: %w", err)
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	if err := insertNamespace(ctx, tx, entity.Namespace, now); err != nil {
		return model.Entity{}, err
	}
	if err := insertProvenance(ctx, tx, provenance); err != nil {
		return model.Entity{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity (
			id, entity_type, canonical_name, aliases, namespace, provenance_id,
			content_hash, extensions
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`,
		entity.ID,
		entity.EntityType,
		entity.CanonicalName,
		string(entity.Aliases),
		entity.Namespace,
		entity.ProvenanceID,
		entity.ContentHash,
		string(entity.Extensions),
	); err != nil {
		return model.Entity{}, fmt.Errorf("insert entity: %w", err)
	}
	if _, err := insertMutation(ctx, tx, "add_entity", entity.ID, provenance.Agent, entity.ContentHash); err != nil {
		return model.Entity{}, err
	}
	if err := updateManifestTimestamp(ctx, tx, now); err != nil {
		return model.Entity{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Entity{}, fmt.Errorf("commit entity: %w", err)
	}
	return entity, nil
}

// getEvent reads one source event by stable identifier
func (s *Store) GetEvent(ctx context.Context, id string) (model.Event, error) {
	var event model.Event
	var payload, recordedAt, extensions string
	var occurredAt sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT id, event_type, payload, payload_content_type, occurred_at,
		       recorded_at, namespace, actor, provenance_id, content_hash, extensions
		FROM event
		WHERE id = ?
	`, id).Scan(
		&event.ID,
		&event.EventType,
		&payload,
		&event.PayloadContentType,
		&occurredAt,
		&recordedAt,
		&event.Namespace,
		&event.Actor,
		&event.ProvenanceID,
		&event.ContentHash,
		&extensions,
	)
	if err != nil {
		return model.Event{}, fmt.Errorf("get event %s: %w", id, err)
	}
	event.Payload = json.RawMessage(payload)
	event.Extensions = json.RawMessage(extensions)
	if occurredAt.Valid {
		parsed, err := model.ParseTime(occurredAt.String)
		if err != nil {
			return model.Event{}, err
		}
		event.OccurredAt = &parsed
	}
	parsed, err := model.ParseTime(recordedAt)
	if err != nil {
		return model.Event{}, err
	}
	event.RecordedAt = parsed
	return event, nil
}

// getMemory reads one memory record by stable identifier
func (s *Store) GetMemory(ctx context.Context, id string) (model.Memory, error) {
	var memory model.Memory
	var structuredValue, recordedAt, confidence, extensions string
	var validFrom, validTo sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT id, kind, content, structured_value, subject_entity_id, predicate,
		       object_entity_id, object_value, valid_from, valid_to, recorded_at,
		       status, namespace, confidence, provenance_id, content_hash, extensions
		FROM memory
		WHERE id = ?
	`, id).Scan(
		&memory.ID,
		&memory.Kind,
		&memory.Content,
		&structuredValue,
		&memory.SubjectEntityID,
		&memory.Predicate,
		&memory.ObjectEntityID,
		&memory.ObjectValue,
		&validFrom,
		&validTo,
		&recordedAt,
		&memory.Status,
		&memory.Namespace,
		&confidence,
		&memory.ProvenanceID,
		&memory.ContentHash,
		&extensions,
	)
	if err != nil {
		return model.Memory{}, fmt.Errorf("get memory %s: %w", id, err)
	}
	memory.StructuredValue = json.RawMessage(structuredValue)
	memory.Confidence = json.RawMessage(confidence)
	memory.Extensions = json.RawMessage(extensions)
	if validFrom.Valid {
		parsed, err := model.ParseTime(validFrom.String)
		if err != nil {
			return model.Memory{}, err
		}
		memory.ValidFrom = &parsed
	}
	if validTo.Valid {
		parsed, err := model.ParseTime(validTo.String)
		if err != nil {
			return model.Memory{}, err
		}
		memory.ValidTo = &parsed
	}
	parsed, err := model.ParseTime(recordedAt)
	if err != nil {
		return model.Memory{}, err
	}
	memory.RecordedAt = parsed
	return memory, nil
}

// inspect reads file metadata without changing canonical records
func (s *Store) Inspect(ctx context.Context) (Inspection, error) {
	manifest, err := s.loadManifest(ctx)
	if err != nil {
		return Inspection{}, err
	}
	counts, err := s.counts(ctx)
	if err != nil {
		return Inspection{}, err
	}
	return Inspection{Manifest: manifest, Counts: counts}, nil
}

// validate checks the manifest, references, JSON, intervals, and content hashes
func (s *Store) Validate(ctx context.Context) (ValidationReport, error) {
	report := ValidationReport{
		Counts:   map[string]int{},
		Errors:   []string{},
		Warnings: []string{},
	}
	inspection, err := s.Inspect(ctx)
	if err != nil {
		return report, err
	}
	report.Counts = inspection.Counts
	if inspection.Manifest.FormatName != model.FormatName {
		report.Errors = append(report.Errors, "unexpected format name")
	}
	if inspection.Manifest.FormatVersion != model.FormatVersion {
		report.Errors = append(report.Errors, "unsupported format version")
	}
	if inspection.Manifest.SchemaVersion != model.SchemaVersion {
		report.Errors = append(report.Errors, "unsupported schema version")
	}

	var applicationID int64
	if err := s.db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&applicationID); err != nil {
		return report, fmt.Errorf("read application ID: %w", err)
	}
	if applicationID != model.ApplicationID {
		report.Errors = append(report.Errors, "unexpected SQLite application ID")
	}

	if err := s.validateForeignKeys(ctx, &report); err != nil {
		return report, err
	}
	if err := s.validateEvents(ctx, &report); err != nil {
		return report, err
	}
	if err := s.validateMemories(ctx, &report); err != nil {
		return report, err
	}
	if err := s.validateEntities(ctx, &report); err != nil {
		return report, err
	}
	if err := s.validateDerivations(ctx, &report); err != nil {
		return report, err
	}
	if err := s.validatePolicies(ctx, &report); err != nil {
		return report, err
	}
	if err := s.validateDeletions(ctx, &report); err != nil {
		return report, err
	}

	report.Valid = len(report.Errors) == 0
	return report, nil
}

func (s *Store) manifestValue(ctx context.Context, key string) (string, error) {
	var value string
	if err := s.db.QueryRowContext(
		ctx,
		"SELECT value FROM trace_meta WHERE key = ?",
		key,
	).Scan(&value); err != nil {
		return "", err
	}
	return value, nil
}

func (s *Store) loadManifest(ctx context.Context) (model.Manifest, error) {
	values := make(map[string]string)
	rows, err := s.db.QueryContext(ctx, "SELECT key, value FROM trace_meta")
	if err != nil {
		return model.Manifest{}, fmt.Errorf("read trace manifest: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return model.Manifest{}, fmt.Errorf("scan trace manifest: %w", err)
		}
		values[key] = value
	}
	if err := rows.Err(); err != nil {
		return model.Manifest{}, fmt.Errorf("read trace manifest rows: %w", err)
	}

	schemaVersion, err := parseInt(values, "schema_version")
	if err != nil {
		return model.Manifest{}, err
	}
	createdAt, err := model.ParseTime(values["created_at"])
	if err != nil {
		return model.Manifest{}, err
	}
	updatedAt, err := model.ParseTime(values["updated_at"])
	if err != nil {
		return model.Manifest{}, err
	}
	var features []string
	if err := json.Unmarshal([]byte(values["required_features"]), &features); err != nil {
		return model.Manifest{}, fmt.Errorf("parse required features: %w", err)
	}
	return model.Manifest{
		FormatName:       values["format_name"],
		FormatVersion:    values["format_version"],
		SchemaVersion:    schemaVersion,
		CreatedAt:        createdAt,
		UpdatedAt:        updatedAt,
		Generator:        values["generator"],
		FileID:           values["file_id"],
		RequiredFeatures: features,
	}, nil
}

func (s *Store) counts(ctx context.Context) (map[string]int, error) {
	counts := make(map[string]int)
	for _, table := range []string{
		"event",
		"memory",
		"entity",
		"provenance",
		"derivation",
		"mutation",
		"relationship",
		"policy",
		"deletion",
	} {
		var count int
		if err := s.db.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM "+table,
		).Scan(&count); err != nil {
			return nil, fmt.Errorf("count %s records: %w", table, err)
		}
		counts[table] = count
	}
	return counts, nil
}

func (s *Store) validateForeignKeys(ctx context.Context, report *ValidationReport) error {
	rows, err := s.db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("run foreign key validation: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var table, rowID, parent, foreignKey string
		if err := rows.Scan(&table, &rowID, &parent, &foreignKey); err != nil {
			return fmt.Errorf("scan foreign key validation: %w", err)
		}
		report.Errors = append(report.Errors, fmt.Sprintf(
			"foreign key violation in %s row %s referencing %s",
			table,
			rowID,
			parent,
		))
	}
	return rows.Err()
}

func (s *Store) validateEvents(ctx context.Context, report *ValidationReport) error {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, event_type, payload, payload_content_type, occurred_at,
		       recorded_at, namespace, actor, provenance_id, content_hash, extensions
		FROM event
	`)
	if err != nil {
		return fmt.Errorf("query events for validation: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var event model.Event
		var payload, recordedAt, extensions string
		var occurredAt sql.NullString
		if err := rows.Scan(
			&event.ID,
			&event.EventType,
			&payload,
			&event.PayloadContentType,
			&occurredAt,
			&recordedAt,
			&event.Namespace,
			&event.Actor,
			&event.ProvenanceID,
			&event.ContentHash,
			&extensions,
		); err != nil {
			return fmt.Errorf("scan event for validation: %w", err)
		}
		event.Payload = json.RawMessage(payload)
		event.Extensions = json.RawMessage(extensions)
		if occurredAt.Valid {
			parsed, err := model.ParseTime(occurredAt.String)
			if err != nil {
				report.Errors = append(report.Errors, fmt.Sprintf("event %s has invalid occurred_at", event.ID))
			} else {
				event.OccurredAt = &parsed
			}
		}
		parsed, err := model.ParseTime(recordedAt)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("event %s has invalid recorded_at", event.ID))
		} else {
			event.RecordedAt = parsed
		}
		if err := model.ValidateJSON(event.Payload); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("event %s has invalid payload", event.ID))
		}
		if err := model.ValidateJSON(event.Extensions); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("event %s has invalid extensions", event.ID))
		}
		hash, err := model.HashEvent(event)
		if err != nil || hash != event.ContentHash {
			report.Errors = append(report.Errors, fmt.Sprintf("event %s has an invalid content hash", event.ID))
		}
	}
	return rows.Err()
}

func (s *Store) validateMemories(ctx context.Context, report *ValidationReport) error {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, kind, content, structured_value, subject_entity_id, predicate,
		       object_entity_id, object_value, valid_from, valid_to, recorded_at,
		       status, namespace, confidence, provenance_id, content_hash, extensions
		FROM memory
	`)
	if err != nil {
		return fmt.Errorf("query memories for validation: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var memory model.Memory
		var structuredValue, recordedAt, confidence, extensions string
		var validFrom, validTo sql.NullString
		if err := rows.Scan(
			&memory.ID,
			&memory.Kind,
			&memory.Content,
			&structuredValue,
			&memory.SubjectEntityID,
			&memory.Predicate,
			&memory.ObjectEntityID,
			&memory.ObjectValue,
			&validFrom,
			&validTo,
			&recordedAt,
			&memory.Status,
			&memory.Namespace,
			&confidence,
			&memory.ProvenanceID,
			&memory.ContentHash,
			&extensions,
		); err != nil {
			return fmt.Errorf("scan memory for validation: %w", err)
		}
		memory.StructuredValue = json.RawMessage(structuredValue)
		memory.Confidence = json.RawMessage(confidence)
		memory.Extensions = json.RawMessage(extensions)
		if validFrom.Valid {
			parsed, err := model.ParseTime(validFrom.String)
			if err != nil {
				report.Errors = append(report.Errors, fmt.Sprintf("memory %s has invalid valid_from", memory.ID))
			} else {
				memory.ValidFrom = &parsed
			}
		}
		if validTo.Valid {
			parsed, err := model.ParseTime(validTo.String)
			if err != nil {
				report.Errors = append(report.Errors, fmt.Sprintf("memory %s has invalid valid_to", memory.ID))
			} else {
				memory.ValidTo = &parsed
			}
		}
		parsed, err := model.ParseTime(recordedAt)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("memory %s has invalid recorded_at", memory.ID))
		} else {
			memory.RecordedAt = parsed
		}
		if err := model.ValidateMemoryKind(memory.Kind); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("memory %s has invalid kind", memory.ID))
		}
		if err := model.ValidateMemoryStatus(memory.Status); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("memory %s has invalid status", memory.ID))
		}
		if err := model.ValidateInterval(memory.ValidFrom, memory.ValidTo); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("memory %s has invalid interval", memory.ID))
		}
		if err := model.ValidateJSON(memory.StructuredValue); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("memory %s has invalid structured value", memory.ID))
		}
		if err := model.ValidateJSON(memory.Confidence); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("memory %s has invalid confidence", memory.ID))
		}
		if err := model.ValidateJSON(memory.Extensions); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("memory %s has invalid extensions", memory.ID))
		}
		hash, err := model.HashMemory(memory)
		if err != nil || hash != memory.ContentHash {
			report.Errors = append(report.Errors, fmt.Sprintf("memory %s has an invalid content hash", memory.ID))
		}
	}
	return rows.Err()
}

func (s *Store) validateEntities(ctx context.Context, report *ValidationReport) error {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, entity_type, canonical_name, aliases, namespace,
		       provenance_id, content_hash, extensions
		FROM entity
	`)
	if err != nil {
		return fmt.Errorf("query entities for validation: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var entity model.Entity
		var aliases, extensions string
		if err := rows.Scan(
			&entity.ID,
			&entity.EntityType,
			&entity.CanonicalName,
			&aliases,
			&entity.Namespace,
			&entity.ProvenanceID,
			&entity.ContentHash,
			&extensions,
		); err != nil {
			return fmt.Errorf("scan entity for validation: %w", err)
		}
		entity.Aliases = json.RawMessage(aliases)
		entity.Extensions = json.RawMessage(extensions)
		if err := model.ValidateJSON(entity.Aliases); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("entity %s has invalid aliases", entity.ID))
		}
		if err := model.ValidateJSON(entity.Extensions); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("entity %s has invalid extensions", entity.ID))
		}
		hash, err := model.HashEntity(entity)
		if err != nil || hash != entity.ContentHash {
			report.Errors = append(report.Errors, fmt.Sprintf("entity %s has an invalid content hash", entity.ID))
		}
	}
	return rows.Err()
}

func (s *Store) validateDerivations(ctx context.Context, report *ValidationReport) error {
	// materialize rows before reference checks because the store uses one connection
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, source_id, target_id
		FROM derivation
	`)
	if err != nil {
		return fmt.Errorf("query derivations for validation: %w", err)
	}
	type derivationReference struct {
		id       string
		sourceID string
		targetID string
	}
	var references []derivationReference
	for rows.Next() {
		var id, sourceID, targetID string
		if err := rows.Scan(&id, &sourceID, &targetID); err != nil {
			return fmt.Errorf("scan derivation for validation: %w", err)
		}
		references = append(references, derivationReference{
			id:       id,
			sourceID: sourceID,
			targetID: targetID,
		})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close derivation validation rows: %w", err)
	}
	for _, reference := range references {
		sourceExists, err := recordExists(ctx, s.db, reference.sourceID)
		if err != nil {
			return err
		}
		targetExists, err := recordExists(ctx, s.db, reference.targetID)
		if err != nil {
			return err
		}
		if !sourceExists || !targetExists {
			report.Errors = append(report.Errors, fmt.Sprintf(
				"derivation %s has a missing record",
				reference.id,
			))
		}
	}
	return nil
}

func (s *Store) validatePolicies(ctx context.Context, report *ValidationReport) error {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, effect, principal, operation, resource_selector, conditions,
		       created_at, expires_at
		FROM policy
	`)
	if err != nil {
		return fmt.Errorf("query policies for validation: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, effect, principal, operation, selector, conditions, createdAt string
		var expiresAt sql.NullString
		if err := rows.Scan(
			&id,
			&effect,
			&principal,
			&operation,
			&selector,
			&conditions,
			&createdAt,
			&expiresAt,
		); err != nil {
			return fmt.Errorf("scan policy for validation: %w", err)
		}
		if effect != model.PolicyAllow && effect != model.PolicyDeny {
			report.Errors = append(report.Errors, fmt.Sprintf("policy %s has invalid effect", id))
		}
		if principal == "" || operation == "" {
			report.Errors = append(report.Errors, fmt.Sprintf("policy %s is missing required fields", id))
		}
		if err := model.ValidateJSON(json.RawMessage(selector)); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("policy %s has invalid selector", id))
		}
		if err := model.ValidateJSON(json.RawMessage(conditions)); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("policy %s has invalid conditions", id))
		}
		if _, err := model.ParseTime(createdAt); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("policy %s has invalid created_at", id))
		}
		if expiresAt.Valid {
			if _, err := model.ParseTime(expiresAt.String); err != nil {
				report.Errors = append(report.Errors, fmt.Sprintf("policy %s has invalid expires_at", id))
			}
		}
	}
	return rows.Err()
}

func (s *Store) validateDeletions(ctx context.Context, report *ValidationReport) error {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, target_id, mode, actor, created_at, details
		FROM deletion
	`)
	if err != nil {
		return fmt.Errorf("query deletions for validation: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, targetID, mode, actor, createdAt, details string
		if err := rows.Scan(
			&id,
			&targetID,
			&mode,
			&actor,
			&createdAt,
			&details,
		); err != nil {
			return fmt.Errorf("scan deletion for validation: %w", err)
		}
		if targetID == "" || actor == "" || mode == "" {
			report.Errors = append(report.Errors, fmt.Sprintf("deletion %s has missing fields", id))
		}
		if err := model.ValidateJSON(json.RawMessage(details)); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("deletion %s has invalid details", id))
		}
		if _, err := model.ParseTime(createdAt); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("deletion %s has invalid created_at", id))
		}
	}
	return rows.Err()
}

// openDatabase configures connection-local safety settings only
func openDatabase(path string, readOnly bool) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout = 5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set SQLite busy timeout: %w", err)
	}
	if readOnly {
		if _, err := db.Exec("PRAGMA query_only = ON"); err != nil {
			db.Close()
			return nil, fmt.Errorf("enable read-only mode: %w", err)
		}
	}
	return db, nil
}

// configureWriteDatabase enables the portable rollback-journal profile
func configureWriteDatabase(db *sql.DB) error {
	if _, err := db.Exec("PRAGMA journal_mode = DELETE"); err != nil {
		return fmt.Errorf("set SQLite journal mode: %w", err)
	}
	if _, err := db.Exec("PRAGMA synchronous = FULL"); err != nil {
		return fmt.Errorf("set SQLite synchronous mode: %w", err)
	}
	return nil
}

// updateManifestTimestamp records the latest committed file mutation
func updateManifestTimestamp(ctx context.Context, tx *sql.Tx, updatedAt time.Time) error {
	if _, err := tx.ExecContext(
		ctx,
		"UPDATE trace_meta SET value = ? WHERE key = 'updated_at'",
		model.FormatTime(updatedAt),
	); err != nil {
		return fmt.Errorf("update trace manifest timestamp: %w", err)
	}
	return nil
}

func insertNamespace(ctx context.Context, tx *sql.Tx, namespace string, createdAt time.Time) error {
	if namespace == "" {
		return errors.New("namespace is required")
	}
	if _, err := tx.ExecContext(
		ctx,
		"INSERT OR IGNORE INTO namespace (id, created_at) VALUES (?, ?)",
		namespace,
		model.FormatTime(createdAt),
	); err != nil {
		return fmt.Errorf("insert namespace: %w", err)
	}
	return nil
}

func insertProvenance(ctx context.Context, tx *sql.Tx, provenance model.Provenance) error {
	parents, err := json.Marshal(provenance.ParentProvenanceIDs)
	if err != nil {
		return fmt.Errorf("encode provenance parents: %w", err)
	}
	if err := model.ValidateJSON(provenance.Extensions); err != nil {
		return fmt.Errorf("provenance extensions: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO provenance (
			id, source_type, source_id, source_uri, agent, model, provider,
			conversation_id, message_id, operation, created_at,
			parent_provenance_ids, extensions
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		provenance.ID,
		provenance.SourceType,
		provenance.SourceID,
		provenance.SourceURI,
		provenance.Agent,
		provenance.Model,
		provenance.Provider,
		provenance.ConversationID,
		provenance.MessageID,
		provenance.Operation,
		model.FormatTime(provenance.CreatedAt),
		string(parents),
		string(model.JSONOrEmpty(provenance.Extensions)),
	); err != nil {
		return fmt.Errorf("insert provenance: %w", err)
	}
	return nil
}

func insertMutation(
	ctx context.Context,
	tx *sql.Tx,
	operation, targetID, actor, requestHash string,
) (model.Mutation, error) {
	commitID, err := model.NewID()
	if err != nil {
		return model.Mutation{}, err
	}
	return insertMutationWithCommit(
		ctx,
		tx,
		operation,
		targetID,
		actor,
		requestHash,
		commitID,
	)
}

func insertMutationWithCommit(
	ctx context.Context,
	tx *sql.Tx,
	operation, targetID, actor, requestHash, commitID string,
) (model.Mutation, error) {
	mutationID, err := model.NewID()
	if err != nil {
		return model.Mutation{}, err
	}
	createdAt := time.Now().UTC()
	result, err := tx.ExecContext(ctx, `
		INSERT INTO mutation (
			id, commit_id, operation, target_id, actor, created_at,
			request_hash, metadata
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`,
		mutationID,
		commitID,
		operation,
		targetID,
		actor,
		model.FormatTime(createdAt),
		requestHash,
		`{}`,
	)
	if err != nil {
		return model.Mutation{}, fmt.Errorf("insert mutation: %w", err)
	}
	sequence, err := result.LastInsertId()
	if err != nil {
		return model.Mutation{}, fmt.Errorf("read mutation sequence: %w", err)
	}
	return model.Mutation{
		Sequence:    sequence,
		ID:          mutationID,
		CommitID:    commitID,
		Operation:   operation,
		TargetID:    targetID,
		Actor:       actor,
		CreatedAt:   createdAt,
		RequestHash: requestHash,
		Metadata:    json.RawMessage(`{}`),
	}, nil
}

func prepareProvenance(provenance *model.Provenance, sourceID string) error {
	// local provenance identifies the writer without claiming an external source
	if provenance.ID == "" {
		id, err := model.NewID()
		if err != nil {
			return err
		}
		provenance.ID = id
	}
	if provenance.SourceType == "" {
		provenance.SourceType = "trace.local"
	}
	if provenance.SourceID == "" {
		provenance.SourceID = sourceID
	}
	if provenance.CreatedAt.IsZero() {
		provenance.CreatedAt = time.Now().UTC()
	}
	extensions, err := model.CanonicalJSON(provenance.Extensions, json.RawMessage(`{}`))
	if err != nil {
		return err
	}
	provenance.Extensions = extensions
	return nil
}

func recordExists(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, id string) (bool, error) {
	// records share one ID space across events, memories, and entities
	var exists bool
	err := queryer.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM event WHERE id = ?
			UNION ALL
			SELECT 1 FROM memory WHERE id = ?
			UNION ALL
			SELECT 1 FROM entity WHERE id = ?
		)
	`, id, id, id).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check record %s: %w", id, err)
	}
	return exists, nil
}

func formatOptionalTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return model.FormatTime(*value)
}

func parseInt(values map[string]string, key string) (int, error) {
	value := values[key]
	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil {
		return 0, fmt.Errorf("parse manifest field %q: %w", key, err)
	}
	return parsed, nil
}
