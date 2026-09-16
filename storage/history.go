package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"trace/model"
)

// addRelation records an explicit lifecycle relationship between two records
func (s *Store) AddRelation(ctx context.Context, input model.RelationInput) (model.Derivation, error) {
	if s.readOnly {
		return model.Derivation{}, errors.New("trace is read-only")
	}
	if input.SourceID == "" || input.TargetID == "" {
		return model.Derivation{}, errors.New("relation source and target are required")
	}
	if input.SourceID == input.TargetID {
		return model.Derivation{}, errors.New("relation source and target must differ")
	}
	if err := model.ValidateRelation(input.Relation); err != nil {
		return model.Derivation{}, err
	}
	extensions, err := model.CanonicalJSON(input.Extensions, json.RawMessage(`{}`))
	if err != nil {
		return model.Derivation{}, fmt.Errorf("relation extensions: %w", err)
	}
	relationID := input.ID
	if relationID == "" {
		relationID, err = model.NewID()
		if err != nil {
			return model.Derivation{}, err
		}
	}
	createdAt := time.Now().UTC()
	relation := model.Derivation{
		ID:         relationID,
		SourceID:   input.SourceID,
		TargetID:   input.TargetID,
		Relation:   input.Relation,
		CreatedAt:  createdAt,
		Actor:      input.Actor,
		Extensions: extensions,
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Derivation{}, fmt.Errorf("begin relation transaction: %w", err)
	}
	defer tx.Rollback()
	for _, id := range []string{relation.SourceID, relation.TargetID} {
		exists, err := recordExists(ctx, tx, id)
		if err != nil {
			return model.Derivation{}, err
		}
		if !exists {
			return model.Derivation{}, fmt.Errorf("relation record does not exist: %s", id)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO derivation (
			id, source_id, target_id, relation, created_at, actor, extensions
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`,
		relation.ID,
		relation.SourceID,
		relation.TargetID,
		relation.Relation,
		model.FormatTime(relation.CreatedAt),
		relation.Actor,
		string(relation.Extensions),
	); err != nil {
		return model.Derivation{}, fmt.Errorf("insert relation: %w", err)
	}
	if _, err := insertMutation(ctx, tx, "relate", relation.ID, relation.Actor, ""); err != nil {
		return model.Derivation{}, err
	}
	if err := updateManifestTimestamp(ctx, tx, createdAt); err != nil {
		return model.Derivation{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Derivation{}, fmt.Errorf("commit relation: %w", err)
	}
	return relation, nil
}

// queryMemories applies valid-time, recorded-time, and lifecycle filters
func (s *Store) QueryMemories(ctx context.Context, query model.MemoryQuery) (model.QueryResult, error) {
	if query.Limit < 0 {
		return model.QueryResult{}, errors.New("query limit must not be negative")
	}
	limit := query.Limit
	if limit == 0 {
		limit = 100
	}
	validAt := query.ValidAt
	if validAt == nil {
		now := time.Now().UTC()
		validAt = &now
	}

	var statement strings.Builder
	statement.WriteString(`
		SELECT id, kind, content, structured_value, subject_entity_id, predicate,
		       object_entity_id, object_value, valid_from, valid_to, recorded_at,
		       status, namespace, confidence, provenance_id, content_hash, extensions
		FROM memory
		WHERE valid_from IS NOT NULL
		  AND valid_from <= ?
		  AND (valid_to IS NULL OR valid_to > ?)
	`)
	args := []any{model.FormatTime(*validAt), model.FormatTime(*validAt)}
	if query.Namespace != "" {
		statement.WriteString(" AND namespace = ?")
		args = append(args, query.Namespace)
	}
	if query.RecordedBefore != nil {
		statement.WriteString(" AND recorded_at < ?")
		args = append(args, model.FormatTime(*query.RecordedBefore))
	}
	if query.RecordedAfter != nil {
		statement.WriteString(" AND recorded_at >= ?")
		args = append(args, model.FormatTime(*query.RecordedAfter))
	}

	statuses := []string{"active", "uncertain"}
	if query.ValidAt != nil || query.IncludeSuperseded {
		statuses = append(statuses, "superseded")
	}
	if query.IncludeInvalidated {
		statuses = append(statuses, "invalidated")
	}
	if query.IncludeRedacted {
		statuses = append(statuses, "redacted")
	}
	placeholders := make([]string, len(statuses))
	for index, status := range statuses {
		placeholders[index] = "?"
		args = append(args, status)
	}
	statement.WriteString(" AND status IN (" + strings.Join(placeholders, ", ") + ")")
	statement.WriteString(" ORDER BY recorded_at ASC, id ASC LIMIT ?")
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, statement.String(), args...)
	if err != nil {
		return model.QueryResult{}, fmt.Errorf("query memories: %w", err)
	}
	defer rows.Close()
	memories := make([]model.Memory, 0)
	for rows.Next() {
		memory, err := scanMemory(rows)
		if err != nil {
			return model.QueryResult{}, fmt.Errorf("scan queried memory: %w", err)
		}
		memories = append(memories, memory)
	}
	if err := rows.Err(); err != nil {
		return model.QueryResult{}, fmt.Errorf("read queried memories: %w", err)
	}

	conflicts, err := s.findConflicts(ctx, memories)
	if err != nil {
		return model.QueryResult{}, err
	}
	evidenceState := "supported"
	if len(memories) == 0 {
		evidenceState = "absent"
	}
	for _, conflict := range conflicts {
		if !conflict.Resolved {
			evidenceState = "conflicting"
			break
		}
	}
	return model.QueryResult{
		Memories:      memories,
		Conflicts:     conflicts,
		EvidenceState: evidenceState,
	}, nil
}

// snapshot returns the latest committed mutation sequence
func (s *Store) Snapshot(ctx context.Context) (model.Snapshot, error) {
	var sequence int64
	if err := s.db.QueryRowContext(
		ctx,
		"SELECT COALESCE(MAX(sequence), 0) FROM mutation",
	).Scan(&sequence); err != nil {
		return model.Snapshot{}, fmt.Errorf("read current snapshot: %w", err)
	}
	manifest, err := s.loadManifest(ctx)
	if err != nil {
		return model.Snapshot{}, err
	}
	return model.Snapshot{Sequence: sequence, UpdatedAt: manifest.UpdatedAt}, nil
}

// history returns mutations in stable commit order
func (s *Store) History(ctx context.Context, targetID string) ([]model.Mutation, error) {
	statement := `
		SELECT sequence, id, commit_id, operation, target_id, actor,
		       created_at, request_hash, metadata
		FROM mutation
	`
	args := []any{}
	if targetID != "" {
		statement += " WHERE target_id = ?"
		args = append(args, targetID)
	}
	statement += " ORDER BY sequence ASC"
	return s.queryMutations(ctx, statement, args...)
}

// diff reports mutations and new records between two logical snapshots
func (s *Store) Diff(ctx context.Context, from, to model.Snapshot) (model.DiffResult, error) {
	current, err := s.Snapshot(ctx)
	if err != nil {
		return model.DiffResult{}, err
	}
	if from.Sequence < 0 || to.Sequence < 0 {
		return model.DiffResult{}, errors.New("snapshot sequences must not be negative")
	}
	if to.Sequence == 0 {
		to.Sequence = current.Sequence
	}
	if from.Sequence > to.Sequence {
		return model.DiffResult{}, errors.New("from snapshot must not be after to snapshot")
	}
	if to.Sequence > current.Sequence {
		return model.DiffResult{}, fmt.Errorf(
			"to snapshot %d is newer than current snapshot %d",
			to.Sequence,
			current.Sequence,
		)
	}
	from.UpdatedAt, err = s.snapshotTime(ctx, from.Sequence)
	if err != nil {
		return model.DiffResult{}, err
	}
	to.UpdatedAt, err = s.snapshotTime(ctx, to.Sequence)
	if err != nil {
		return model.DiffResult{}, err
	}
	mutations, err := s.queryMutations(ctx, `
		SELECT sequence, id, commit_id, operation, target_id, actor,
		       created_at, request_hash, metadata
		FROM mutation
		WHERE sequence > ? AND sequence <= ?
		ORDER BY sequence ASC
	`, from.Sequence, to.Sequence)
	if err != nil {
		return model.DiffResult{}, err
	}
	added := make([]model.RecordReference, 0)
	for _, mutation := range mutations {
		if reference, ok := recordReference(mutation); ok {
			added = append(added, reference)
		}
	}
	return model.DiffResult{
		From:      from,
		To:        to,
		Mutations: mutations,
		Added:     added,
	}, nil
}

// explain returns source records, provenance, derivations, and mutations
func (s *Store) Explain(ctx context.Context, targetID string) (model.Explanation, error) {
	kind, err := s.recordKind(ctx, targetID)
	if err != nil {
		return model.Explanation{}, err
	}
	explanation := model.Explanation{TargetID: targetID, TargetKind: kind}
	switch kind {
	case "event":
		event, err := s.GetEvent(ctx, targetID)
		if err != nil {
			return model.Explanation{}, err
		}
		explanation.Event = &event
		if err := s.addProvenance(ctx, &explanation, event.ProvenanceID); err != nil {
			return model.Explanation{}, err
		}
	case "memory":
		memory, err := s.GetMemory(ctx, targetID)
		if err != nil {
			return model.Explanation{}, err
		}
		explanation.Memory = &memory
		if err := s.addProvenance(ctx, &explanation, memory.ProvenanceID); err != nil {
			return model.Explanation{}, err
		}
	case "entity":
		entity, err := s.getEntity(ctx, targetID)
		if err != nil {
			return model.Explanation{}, err
		}
		explanation.Entity = &entity
		if err := s.addProvenance(ctx, &explanation, entity.ProvenanceID); err != nil {
			return model.Explanation{}, err
		}
	}

	derivations, err := s.derivationsFor(ctx, targetID)
	if err != nil {
		return model.Explanation{}, err
	}
	explanation.Derivations = derivations
	for _, derivation := range derivations {
		if derivation.SourceID == targetID {
			continue
		}
		if err := s.addSource(ctx, &explanation, derivation.SourceID); err != nil {
			return model.Explanation{}, err
		}
	}
	mutations, err := s.History(ctx, targetID)
	if err != nil {
		return model.Explanation{}, err
	}
	for _, derivation := range derivations {
		relationMutations, err := s.History(ctx, derivation.ID)
		if err != nil {
			return model.Explanation{}, err
		}
		mutations = append(mutations, relationMutations...)
	}
	sort.Slice(mutations, func(left, right int) bool {
		return mutations[left].Sequence < mutations[right].Sequence
	})
	explanation.Mutations = mutations
	return explanation, nil
}

func (s *Store) findConflicts(ctx context.Context, memories []model.Memory) ([]model.ConflictGroup, error) {
	if len(memories) < 2 {
		return []model.ConflictGroup{}, nil
	}
	ids := make(map[string]bool, len(memories))
	for _, memory := range memories {
		ids[memory.ID] = true
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, source_id, target_id, relation
		FROM derivation
		WHERE relation IN ('contradicts', 'supersedes')
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query conflict relations: %w", err)
	}
	defer rows.Close()
	type relation struct {
		id, sourceID, targetID, relation string
	}
	var relations []relation
	for rows.Next() {
		var value relation
		if err := rows.Scan(&value.id, &value.sourceID, &value.targetID, &value.relation); err != nil {
			return nil, fmt.Errorf("scan conflict relation: %w", err)
		}
		if ids[value.sourceID] && ids[value.targetID] {
			relations = append(relations, value)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read conflict relations: %w", err)
	}
	parent := make(map[string]string, len(ids))
	for id := range ids {
		parent[id] = id
	}
	var find func(string) string
	find = func(id string) string {
		if parent[id] != id {
			parent[id] = find(parent[id])
		}
		return parent[id]
	}
	union := func(left, right string) {
		leftRoot, rightRoot := find(left), find(right)
		if leftRoot != rightRoot {
			parent[rightRoot] = leftRoot
		}
	}
	for _, value := range relations {
		if value.relation == model.RelationContradicts {
			union(value.sourceID, value.targetID)
		}
	}
	groups := make(map[string][]string)
	for id := range ids {
		root := find(id)
		groups[root] = append(groups[root], id)
	}
	result := make([]model.ConflictGroup, 0)
	for _, group := range groups {
		if len(group) < 2 {
			continue
		}
		sort.Strings(group)
		conflict := model.ConflictGroup{MemoryIDs: group}
		groupIDs := make(map[string]bool, len(group))
		for _, id := range group {
			groupIDs[id] = true
		}
		for _, value := range relations {
			if value.relation == model.RelationSupersedes &&
				groupIDs[value.sourceID] && groupIDs[value.targetID] {
				conflict.Resolved = true
				conflict.ResolutionRelationIDs = append(
					conflict.ResolutionRelationIDs,
					value.id,
				)
			}
		}
		sort.Strings(conflict.ResolutionRelationIDs)
		result = append(result, conflict)
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].MemoryIDs[0] < result[right].MemoryIDs[0]
	})
	return result, nil
}

func (s *Store) queryMutations(ctx context.Context, statement string, args ...any) ([]model.Mutation, error) {
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("query mutations: %w", err)
	}
	defer rows.Close()
	mutations := make([]model.Mutation, 0)
	for rows.Next() {
		var mutation model.Mutation
		var createdAt, metadata string
		var requestHash sql.NullString
		if err := rows.Scan(
			&mutation.Sequence,
			&mutation.ID,
			&mutation.CommitID,
			&mutation.Operation,
			&mutation.TargetID,
			&mutation.Actor,
			&createdAt,
			&requestHash,
			&metadata,
		); err != nil {
			return nil, fmt.Errorf("scan mutation: %w", err)
		}
		parsed, err := model.ParseTime(createdAt)
		if err != nil {
			return nil, err
		}
		mutation.CreatedAt = parsed
		mutation.RequestHash = requestHash.String
		mutation.Metadata = json.RawMessage(metadata)
		mutations = append(mutations, mutation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read mutations: %w", err)
	}
	return mutations, nil
}

func (s *Store) snapshotTime(ctx context.Context, sequence int64) (time.Time, error) {
	if sequence == 0 {
		manifest, err := s.loadManifest(ctx)
		if err != nil {
			return time.Time{}, err
		}
		return manifest.CreatedAt, nil
	}
	var value string
	if err := s.db.QueryRowContext(ctx, `
		SELECT created_at
		FROM mutation
		WHERE sequence <= ?
		ORDER BY sequence DESC
		LIMIT 1
	`, sequence).Scan(&value); err != nil {
		return time.Time{}, fmt.Errorf("read snapshot time: %w", err)
	}
	return model.ParseTime(value)
}

func recordReference(mutation model.Mutation) (model.RecordReference, bool) {
	switch mutation.Operation {
	case "add_event":
		return model.RecordReference{ID: mutation.TargetID, Kind: "event"}, true
	case "remember":
		return model.RecordReference{ID: mutation.TargetID, Kind: "memory"}, true
	case "add_entity":
		return model.RecordReference{ID: mutation.TargetID, Kind: "entity"}, true
	default:
		return model.RecordReference{}, false
	}
}

func (s *Store) recordKind(ctx context.Context, id string) (string, error) {
	var kind string
	err := s.db.QueryRowContext(ctx, `
		SELECT kind FROM (
			SELECT 'event' AS kind, id FROM event
			UNION ALL
			SELECT 'memory' AS kind, id FROM memory
			UNION ALL
			SELECT 'entity' AS kind, id FROM entity
		)
		WHERE id = ?
	`, id).Scan(&kind)
	if err != nil {
		return "", fmt.Errorf("find record %s: %w", id, err)
	}
	return kind, nil
}

func (s *Store) derivationsFor(ctx context.Context, id string) ([]model.Derivation, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, source_id, target_id, relation, created_at, actor, extensions
		FROM derivation
		WHERE source_id = ? OR target_id = ?
		ORDER BY created_at ASC, id ASC
	`, id, id)
	if err != nil {
		return nil, fmt.Errorf("query derivations: %w", err)
	}
	defer rows.Close()
	var result []model.Derivation
	for rows.Next() {
		var derivation model.Derivation
		var createdAt, extensions string
		if err := rows.Scan(
			&derivation.ID,
			&derivation.SourceID,
			&derivation.TargetID,
			&derivation.Relation,
			&createdAt,
			&derivation.Actor,
			&extensions,
		); err != nil {
			return nil, fmt.Errorf("scan derivation: %w", err)
		}
		parsed, err := model.ParseTime(createdAt)
		if err != nil {
			return nil, err
		}
		derivation.CreatedAt = parsed
		derivation.Extensions = json.RawMessage(extensions)
		result = append(result, derivation)
	}
	return result, rows.Err()
}

func (s *Store) addSource(ctx context.Context, explanation *model.Explanation, id string) error {
	kind, err := s.recordKind(ctx, id)
	if err != nil {
		return err
	}
	switch kind {
	case "event":
		event, err := s.GetEvent(ctx, id)
		if err != nil {
			return err
		}
		explanation.SourceEvents = append(explanation.SourceEvents, event)
		return s.addProvenance(ctx, explanation, event.ProvenanceID)
	case "memory":
		memory, err := s.GetMemory(ctx, id)
		if err != nil {
			return err
		}
		explanation.SourceMemories = append(explanation.SourceMemories, memory)
		return s.addProvenance(ctx, explanation, memory.ProvenanceID)
	case "entity":
		entity, err := s.getEntity(ctx, id)
		if err != nil {
			return err
		}
		explanation.SourceEntities = append(explanation.SourceEntities, entity)
		return s.addProvenance(ctx, explanation, entity.ProvenanceID)
	default:
		return fmt.Errorf("unsupported explanation record kind %q", kind)
	}
}

func (s *Store) addProvenance(ctx context.Context, explanation *model.Explanation, id string) error {
	for _, existing := range explanation.Provenance {
		if existing.ID == id {
			return nil
		}
	}
	var provenance model.Provenance
	var parents, extensions string
	var createdAt string
	if err := s.db.QueryRowContext(ctx, `
		SELECT id, source_type, source_id, source_uri, agent, model, provider,
		       conversation_id, message_id, operation, created_at,
		       parent_provenance_ids, extensions
		FROM provenance
		WHERE id = ?
	`, id).Scan(
		&provenance.ID,
		&provenance.SourceType,
		&provenance.SourceID,
		&provenance.SourceURI,
		&provenance.Agent,
		&provenance.Model,
		&provenance.Provider,
		&provenance.ConversationID,
		&provenance.MessageID,
		&provenance.Operation,
		&createdAt,
		&parents,
		&extensions,
	); err != nil {
		return fmt.Errorf("get provenance %s: %w", id, err)
	}
	parsed, err := model.ParseTime(createdAt)
	if err != nil {
		return err
	}
	provenance.CreatedAt = parsed
	if err := json.Unmarshal([]byte(parents), &provenance.ParentProvenanceIDs); err != nil {
		return fmt.Errorf("parse provenance parents: %w", err)
	}
	provenance.Extensions = json.RawMessage(extensions)
	explanation.Provenance = append(explanation.Provenance, provenance)
	return nil
}

func (s *Store) getEntity(ctx context.Context, id string) (model.Entity, error) {
	var entity model.Entity
	var aliases, extensions string
	err := s.db.QueryRowContext(ctx, `
		SELECT id, entity_type, canonical_name, aliases, namespace,
		       provenance_id, content_hash, extensions
		FROM entity
		WHERE id = ?
	`, id).Scan(
		&entity.ID,
		&entity.EntityType,
		&entity.CanonicalName,
		&aliases,
		&entity.Namespace,
		&entity.ProvenanceID,
		&entity.ContentHash,
		&extensions,
	)
	if err != nil {
		return model.Entity{}, fmt.Errorf("get entity %s: %w", id, err)
	}
	entity.Aliases = json.RawMessage(aliases)
	entity.Extensions = json.RawMessage(extensions)
	return entity, nil
}

func scanMemory(scanner interface{ Scan(...any) error }) (model.Memory, error) {
	var memory model.Memory
	var structuredValue, recordedAt, confidence, extensions string
	var validFrom, validTo sql.NullString
	if err := scanner.Scan(
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
		return model.Memory{}, err
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
