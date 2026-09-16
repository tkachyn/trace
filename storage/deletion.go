package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"trace/model"
)

type forgetRecord struct {
	id        string
	kind      string
	namespace string
	event     *model.Event
	memory    *model.Memory
	entity    *model.Entity
}

// Forget removes or redacts a record and its derived dependency closure
func (s *Store) Forget(
	ctx context.Context,
	request model.ForgetRequest,
) ([]model.Deletion, error) {
	if s.readOnly {
		return nil, errors.New("trace is read-only")
	}
	if request.TargetID == "" {
		return nil, errors.New("forget target ID is required")
	}
	if request.Mode == "" {
		request.Mode = "dependency_closure"
	}
	switch request.Mode {
	case "dependency_closure", "target_only", "dependency_closure_redact", "target_only_redact":
	default:
		return nil, fmt.Errorf("unsupported forget mode %q", request.Mode)
	}
	if request.Actor == "" {
		return nil, errors.New("forget actor is required")
	}
	if existing, err := s.existingDeletion(ctx, request.TargetID); err != nil {
		return nil, err
	} else if existing != nil {
		return []model.Deletion{*existing}, nil
	}
	if request.Access.Principal == "" {
		request.Access.Principal = request.Actor
	}
	if err := s.Authorize(ctx, request.Access, model.OperationForget, request.TargetID); err != nil {
		return nil, err
	}

	records, err := s.collectForgetRecords(ctx, request.TargetID, request.Mode)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if err := s.Authorize(ctx, request.Access, model.OperationForget, record.id); err != nil {
			return nil, err
		}
	}
	redact := strings.Contains(request.Mode, "redact")
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin forget transaction: %w", err)
	}
	defer tx.Rollback()

	closureIDs := make([]string, 0, len(records))
	for _, record := range records {
		closureIDs = append(closureIDs, record.id)
	}
	details, err := json.Marshal(map[string]any{
		"action":       map[bool]string{true: "redact", false: "tombstone"}[redact],
		"target_kind":  records[0].kind,
		"closure_ids":  closureIDs,
		"index_status": "removed",
	})
	if err != nil {
		return nil, fmt.Errorf("encode forget details: %w", err)
	}
	now := time.Now().UTC()
	deletions := make([]model.Deletion, 0, len(records))
	for _, record := range records {
		if redact {
			if err := redactRecord(ctx, tx, record); err != nil {
				return nil, err
			}
		} else {
			if err := tombstoneRecord(ctx, tx, record.id); err != nil {
				return nil, err
			}
		}
		deletionID, err := model.NewID()
		if err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO deletion (id, target_id, mode, actor, created_at, details)
			VALUES (?, ?, ?, ?, ?, ?)
		`,
			deletionID,
			record.id,
			request.Mode,
			request.Actor,
			model.FormatTime(now),
			string(details),
		); err != nil {
			return nil, fmt.Errorf("insert deletion: %w", err)
		}
		deletion := model.Deletion{
			ID:        deletionID,
			TargetID:  record.id,
			Mode:      request.Mode,
			Actor:     request.Actor,
			CreatedAt: now,
			Details:   details,
		}
		deletions = append(deletions, deletion)
		if _, err := insertMutation(ctx, tx, "forget", record.id, request.Actor, ""); err != nil {
			return nil, err
		}
	}
	if err := updateManifestTimestamp(ctx, tx, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit forget transaction: %w", err)
	}
	return deletions, nil
}

func (s *Store) collectForgetRecords(
	ctx context.Context,
	targetID, mode string,
) ([]forgetRecord, error) {
	target, err := s.loadForgetRecord(ctx, targetID)
	if err != nil {
		return nil, err
	}
	if mode == "target_only" || strings.Contains(mode, "target_only_") {
		return []forgetRecord{target}, nil
	}
	queue := []string{targetID}
	seen := map[string]bool{targetID: true}
	records := []forgetRecord{target}
	for len(queue) > 0 {
		sourceID := queue[0]
		queue = queue[1:]
		rows, err := s.db.QueryContext(ctx, `
			SELECT target_id
			FROM derivation
			WHERE source_id = ? AND relation = ?
			ORDER BY target_id ASC
		`, sourceID, model.RelationDerivedFrom)
		if err != nil {
			return nil, fmt.Errorf("find forget dependencies: %w", err)
		}
		var dependentIDs []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan forget dependency: %w", err)
			}
			if !seen[id] {
				seen[id] = true
				dependentIDs = append(dependentIDs, id)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("read forget dependencies: %w", err)
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("close forget dependencies: %w", err)
		}
		for _, id := range dependentIDs {
			record, err := s.loadForgetRecord(ctx, id)
			if err != nil {
				return nil, err
			}
			records = append(records, record)
			queue = append(queue, id)
		}
	}
	return records, nil
}

func (s *Store) loadForgetRecord(ctx context.Context, id string) (forgetRecord, error) {
	kind, namespace, err := s.recordKindNamespace(ctx, id)
	if err != nil {
		return forgetRecord{}, err
	}
	record := forgetRecord{id: id, kind: kind, namespace: namespace}
	switch kind {
	case "event":
		value, err := s.GetEvent(ctx, id)
		if err != nil {
			return forgetRecord{}, err
		}
		record.event = &value
	case "memory":
		value, err := s.GetMemory(ctx, id)
		if err != nil {
			return forgetRecord{}, err
		}
		record.memory = &value
	case "entity":
		value, err := s.getEntity(ctx, id)
		if err != nil {
			return forgetRecord{}, err
		}
		record.entity = &value
	default:
		return forgetRecord{}, fmt.Errorf("unsupported forget record kind %q", kind)
	}
	return record, nil
}

func redactRecord(ctx context.Context, tx *sql.Tx, record forgetRecord) error {
	switch record.kind {
	case "event":
		value := *record.event
		value.Payload = json.RawMessage(`null`)
		hash, err := model.HashEvent(value)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE event SET payload = ?, content_hash = ? WHERE id = ?
		`, string(value.Payload), hash, record.id); err != nil {
			return fmt.Errorf("redact event: %w", err)
		}
		return replaceRetrievalDocument(ctx, tx, record, "", hash)
	case "memory":
		value := *record.memory
		value.Content = ""
		value.StructuredValue = json.RawMessage(`{"redacted":true}`)
		value.Confidence = json.RawMessage(`{}`)
		value.Status = "redacted"
		hash, err := model.HashMemory(value)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE memory
			SET content = '', structured_value = '{"redacted":true}', confidence = '{}',
			    status = 'redacted', content_hash = ?
			WHERE id = ?
		`, hash, record.id); err != nil {
			return fmt.Errorf("redact memory: %w", err)
		}
		return replaceRetrievalDocument(ctx, tx, record, "", hash)
	case "entity":
		value := *record.entity
		value.CanonicalName = "[redacted]"
		value.Aliases = json.RawMessage(`[]`)
		hash, err := model.HashEntity(value)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE entity
			SET canonical_name = '[redacted]', aliases = '[]', content_hash = ?
			WHERE id = ?
		`, hash, record.id); err != nil {
			return fmt.Errorf("redact entity: %w", err)
		}
		return replaceRetrievalDocument(ctx, tx, record, "[redacted]", hash)
	default:
		return fmt.Errorf("unsupported redaction kind %q", record.kind)
	}
}

func tombstoneRecord(ctx context.Context, tx *sql.Tx, id string) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM retrieval_fts WHERE record_id = ?", id); err != nil {
		return fmt.Errorf("remove retrieval document: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM derivation WHERE source_id = ? OR target_id = ?
	`, id, id); err != nil {
		return fmt.Errorf("remove derivations: %w", err)
	}
	for _, table := range []string{"event", "memory", "entity"} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE id = ?", id); err != nil {
			return fmt.Errorf("remove %s record: %w", table, err)
		}
	}
	return nil
}

func replaceRetrievalDocument(
	ctx context.Context,
	tx *sql.Tx,
	record forgetRecord,
	content, contentHash string,
) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM retrieval_fts WHERE record_id = ?", record.id); err != nil {
		return fmt.Errorf("remove retrieval document: %w", err)
	}
	return insertRetrievalDocument(
		ctx,
		tx,
		record.id,
		record.kind,
		record.namespace,
		content,
		contentHash,
	)
}

func (s *Store) existingDeletion(ctx context.Context, targetID string) (*model.Deletion, error) {
	var deletion model.Deletion
	var createdAt, details string
	err := s.db.QueryRowContext(ctx, `
		SELECT id, target_id, mode, actor, created_at, details
		FROM deletion
		WHERE target_id = ?
		ORDER BY created_at ASC, id ASC
		LIMIT 1
	`, targetID).Scan(
		&deletion.ID,
		&deletion.TargetID,
		&deletion.Mode,
		&deletion.Actor,
		&createdAt,
		&details,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find existing deletion: %w", err)
	}
	parsed, err := model.ParseTime(createdAt)
	if err != nil {
		return nil, err
	}
	deletion.CreatedAt = parsed
	deletion.Details = json.RawMessage(details)
	return &deletion, nil
}
