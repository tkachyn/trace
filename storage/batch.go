package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"trace/format"
	"trace/model"
)

type preparedBatchEvent struct {
	value      model.Event
	provenance model.Provenance
	clientID   string
}

type preparedBatchMemory struct {
	value      model.Memory
	provenance model.Provenance
	sources    []string
	clientID   string
}

type preparedBatchEntity struct {
	value      model.Entity
	provenance model.Provenance
	clientID   string
}

type preparedBatchRelation struct {
	value    model.RelationInput
	clientID string
}

// IngestBatch validates and commits one atomic machine-readable request
func (s *Store) IngestBatch(
	ctx context.Context,
	request format.BatchRequest,
) (format.BatchResult, error) {
	result := format.BatchResult{
		Protocol:  format.BatchProtocol,
		Version:   format.ProtocolVersion,
		RequestID: request.RequestID,
		IDMap:     map[string]string{},
		Errors:    []format.OperationError{},
	}
	if err := request.Validate(); err != nil {
		result.Errors = append(result.Errors, format.OperationError{
			Category: "protocol",
			Message:  err.Error(),
		})
		return result, nil
	}
	ids := make(map[string]string)
	usedIDs := make(map[string]bool)
	registerID := func(clientID, id string) error {
		if clientID == "" {
			return fmt.Errorf("client_id is required")
		}
		if _, exists := ids[clientID]; exists {
			return fmt.Errorf("duplicate client_id %q", clientID)
		}
		if usedIDs[id] {
			return fmt.Errorf("duplicate record ID %q", id)
		}
		ids[clientID] = id
		usedIDs[id] = true
		result.IDMap[clientID] = id
		return nil
	}
	generateID := func(clientID, requestedID string) error {
		id := requestedID
		var err error
		if id == "" {
			id, err = model.NewID()
			if err != nil {
				return err
			}
		}
		return registerID(clientID, id)
	}
	for _, event := range request.Events {
		if err := generateID(event.ClientID, event.ID); err != nil {
			return batchFailure(result, "validation", "events", event.ClientID, err), nil
		}
	}
	for _, memory := range request.Memories {
		if err := generateID(memory.ClientID, memory.ID); err != nil {
			return batchFailure(result, "validation", "memories", memory.ClientID, err), nil
		}
	}
	for _, entity := range request.Entities {
		if err := generateID(entity.ClientID, entity.ID); err != nil {
			return batchFailure(result, "validation", "entities", entity.ClientID, err), nil
		}
	}
	for _, relation := range request.Relations {
		if relation.ClientID != "" {
			if err := generateID(relation.ClientID, relation.ID); err != nil {
				return batchFailure(result, "validation", "relations", relation.ClientID, err), nil
			}
		}
	}

	resolveID := func(value string) string {
		if resolved, ok := ids[value]; ok {
			return resolved
		}
		return value
	}
	preparedEvents := make([]preparedBatchEvent, 0, len(request.Events))
	for _, input := range request.Events {
		event, provenance, err := prepareBatchEvent(input, request.Actor, ids[input.ClientID])
		if err != nil {
			return batchFailure(result, "validation", "events", input.ClientID, err), nil
		}
		preparedEvents = append(preparedEvents, preparedBatchEvent{
			value: event, provenance: provenance, clientID: input.ClientID,
		})
	}
	preparedMemories := make([]preparedBatchMemory, 0, len(request.Memories))
	for _, input := range request.Memories {
		memory, provenance, err := prepareBatchMemory(input, request.Actor, ids[input.ClientID])
		if err != nil {
			return batchFailure(result, "validation", "memories", input.ClientID, err), nil
		}
		sources := make([]string, 0, len(input.DerivedFrom))
		for _, sourceID := range input.DerivedFrom {
			sources = append(sources, resolveID(sourceID))
		}
		preparedMemories = append(preparedMemories, preparedBatchMemory{
			value: memory, provenance: provenance, sources: sources, clientID: input.ClientID,
		})
	}
	preparedEntities := make([]preparedBatchEntity, 0, len(request.Entities))
	for _, input := range request.Entities {
		entity, provenance, err := prepareBatchEntity(input, ids[input.ClientID])
		if err != nil {
			return batchFailure(result, "validation", "entities", input.ClientID, err), nil
		}
		preparedEntities = append(preparedEntities, preparedBatchEntity{
			value: entity, provenance: provenance, clientID: input.ClientID,
		})
	}
	preparedRelations := make([]preparedBatchRelation, 0, len(request.Relations))
	for _, input := range request.Relations {
		id := input.ID
		if input.ClientID != "" {
			id = ids[input.ClientID]
		}
		if id == "" {
			var err error
			id, err = model.NewID()
			if err != nil {
				return batchFailure(result, "validation", "relations", input.ClientID, err), nil
			}
		}
		if err := model.ValidateRelation(input.Relation); err != nil {
			return batchFailure(result, "validation", "relations", input.ClientID, err), nil
		}
		extensions, err := model.CanonicalJSON(input.Extensions, json.RawMessage(`{}`))
		if err != nil {
			return batchFailure(result, "validation", "relations", input.ClientID, err), nil
		}
		preparedRelations = append(preparedRelations, preparedBatchRelation{
			value: model.RelationInput{
				ID:         id,
				SourceID:   resolveID(input.SourceID),
				TargetID:   resolveID(input.TargetID),
				Relation:   input.Relation,
				Actor:      firstNonEmpty(input.Actor, request.Actor),
				Extensions: extensions,
			},
			clientID: input.ClientID,
		})
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin batch transaction: %w", err)
	}
	defer tx.Rollback()
	commitID, err := model.NewID()
	if err != nil {
		return result, fmt.Errorf("generate batch commit ID: %w", err)
	}
	for _, event := range preparedEvents {
		exists, err := recordExists(ctx, tx, event.value.ID)
		if err != nil {
			return batchFailure(result, "storage", "events", event.clientID, err), nil
		}
		if exists {
			return batchFailure(result, "conflict", "events", event.clientID,
				fmt.Errorf("record ID already exists: %s", event.value.ID)), nil
		}
		if err := insertNamespace(ctx, tx, event.value.Namespace, event.value.RecordedAt); err != nil {
			return batchFailure(result, "validation", "events", event.clientID, err), nil
		}
		if err := insertProvenance(ctx, tx, event.provenance); err != nil {
			return batchFailure(result, "storage", "events", event.clientID, err), nil
		}
		var occurredAt any
		if event.value.OccurredAt != nil {
			occurredAt = model.FormatTime(*event.value.OccurredAt)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO event (
				id, event_type, payload, payload_content_type, occurred_at,
				recorded_at, namespace, actor, provenance_id, content_hash, extensions
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`,
			event.value.ID,
			event.value.EventType,
			string(event.value.Payload),
			event.value.PayloadContentType,
			occurredAt,
			model.FormatTime(event.value.RecordedAt),
			event.value.Namespace,
			event.value.Actor,
			event.value.ProvenanceID,
			event.value.ContentHash,
			string(event.value.Extensions),
		); err != nil {
			return batchFailure(result, "storage", "events", event.clientID, err), nil
		}
		if err := insertRetrievalDocument(
			ctx, tx, event.value.ID, "event", event.value.Namespace,
			string(event.value.Payload), event.value.ContentHash,
		); err != nil {
			return batchFailure(result, "storage", "events", event.clientID, err), nil
		}
		if _, err := insertMutationWithCommit(
			ctx, tx, "add_event", event.value.ID, event.value.Actor,
			event.value.ContentHash, commitID,
		); err != nil {
			return batchFailure(result, "storage", "events", event.clientID, err), nil
		}
		result.Accepted = append(result.Accepted, event.value.ID)
	}
	for _, memory := range preparedMemories {
		exists, err := recordExists(ctx, tx, memory.value.ID)
		if err != nil {
			return batchFailure(result, "storage", "memories", memory.clientID, err), nil
		}
		if exists {
			return batchFailure(result, "conflict", "memories", memory.clientID,
				fmt.Errorf("record ID already exists: %s", memory.value.ID)), nil
		}
		if err := insertNamespace(ctx, tx, memory.value.Namespace, memory.value.RecordedAt); err != nil {
			return batchFailure(result, "validation", "memories", memory.clientID, err), nil
		}
		if err := insertProvenance(ctx, tx, memory.provenance); err != nil {
			return batchFailure(result, "storage", "memories", memory.clientID, err), nil
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO memory (
				id, kind, content, structured_value, subject_entity_id, predicate,
				object_entity_id, object_value, valid_from, valid_to, recorded_at,
				status, namespace, confidence, provenance_id, content_hash, extensions
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`,
			memory.value.ID,
			memory.value.Kind,
			memory.value.Content,
			string(memory.value.StructuredValue),
			memory.value.SubjectEntityID,
			memory.value.Predicate,
			memory.value.ObjectEntityID,
			memory.value.ObjectValue,
			formatOptionalTime(memory.value.ValidFrom),
			formatOptionalTime(memory.value.ValidTo),
			model.FormatTime(memory.value.RecordedAt),
			memory.value.Status,
			memory.value.Namespace,
			string(memory.value.Confidence),
			memory.value.ProvenanceID,
			memory.value.ContentHash,
			string(memory.value.Extensions),
		); err != nil {
			return batchFailure(result, "storage", "memories", memory.clientID, err), nil
		}
		if err := insertRetrievalDocument(
			ctx, tx, memory.value.ID, "memory", memory.value.Namespace,
			memory.value.Content+" "+string(memory.value.StructuredValue),
			memory.value.ContentHash,
		); err != nil {
			return batchFailure(result, "storage", "memories", memory.clientID, err), nil
		}
		if _, err := insertMutationWithCommit(
			ctx, tx, "remember", memory.value.ID, memory.provenance.Agent,
			memory.value.ContentHash, commitID,
		); err != nil {
			return batchFailure(result, "storage", "memories", memory.clientID, err), nil
		}
		result.Accepted = append(result.Accepted, memory.value.ID)
	}
	for _, entity := range preparedEntities {
		exists, err := recordExists(ctx, tx, entity.value.ID)
		if err != nil {
			return batchFailure(result, "storage", "entities", entity.clientID, err), nil
		}
		if exists {
			return batchFailure(result, "conflict", "entities", entity.clientID,
				fmt.Errorf("record ID already exists: %s", entity.value.ID)), nil
		}
		if err := insertNamespace(ctx, tx, entity.value.Namespace, time.Now().UTC()); err != nil {
			return batchFailure(result, "validation", "entities", entity.clientID, err), nil
		}
		if err := insertProvenance(ctx, tx, entity.provenance); err != nil {
			return batchFailure(result, "storage", "entities", entity.clientID, err), nil
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO entity (
				id, entity_type, canonical_name, aliases, namespace, provenance_id,
				content_hash, extensions
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`,
			entity.value.ID,
			entity.value.EntityType,
			entity.value.CanonicalName,
			string(entity.value.Aliases),
			entity.value.Namespace,
			entity.value.ProvenanceID,
			entity.value.ContentHash,
			string(entity.value.Extensions),
		); err != nil {
			return batchFailure(result, "storage", "entities", entity.clientID, err), nil
		}
		if _, err := insertMutationWithCommit(
			ctx, tx, "add_entity", entity.value.ID, entity.provenance.Agent,
			entity.value.ContentHash, commitID,
		); err != nil {
			return batchFailure(result, "storage", "entities", entity.clientID, err), nil
		}
		result.Accepted = append(result.Accepted, entity.value.ID)
	}
	for _, memory := range preparedMemories {
		for _, sourceID := range memory.sources {
			exists, err := recordExists(ctx, tx, sourceID)
			if err != nil {
				return batchFailure(result, "storage", "memories", memory.clientID, err), nil
			}
			if !exists {
				return batchFailure(result, "reference", "memories", memory.clientID,
					fmt.Errorf("derived-from record does not exist: %s", sourceID)), nil
			}
			derivationID, err := model.NewID()
			if err != nil {
				return result, fmt.Errorf("generate derivation ID: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO derivation (
					id, source_id, target_id, relation, created_at, actor, extensions
				) VALUES (?, ?, ?, ?, ?, ?, ?)
			`,
				derivationID,
				sourceID,
				memory.value.ID,
				model.RelationDerivedFrom,
				model.FormatTime(memory.value.RecordedAt),
				memory.provenance.Agent,
				`{}`,
			); err != nil {
				return batchFailure(result, "storage", "memories", memory.clientID, err), nil
			}
		}
	}
	for _, relation := range preparedRelations {
		if relation.value.SourceID == "" || relation.value.TargetID == "" ||
			relation.value.SourceID == relation.value.TargetID {
			return batchFailure(result, "validation", "relations", relation.clientID,
				fmt.Errorf("relation source and target must be distinct and non-empty")), nil
		}
		for _, id := range []string{relation.value.SourceID, relation.value.TargetID} {
			exists, err := recordExists(ctx, tx, id)
			if err != nil {
				return batchFailure(result, "storage", "relations", relation.clientID, err), nil
			}
			if !exists {
				return batchFailure(result, "reference", "relations", relation.clientID,
					fmt.Errorf("relation record does not exist: %s", id)), nil
			}
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO derivation (
				id, source_id, target_id, relation, created_at, actor, extensions
			) VALUES (?, ?, ?, ?, ?, ?, ?)
		`,
			relation.value.ID,
			relation.value.SourceID,
			relation.value.TargetID,
			relation.value.Relation,
			model.FormatTime(time.Now().UTC()),
			relation.value.Actor,
			string(relation.value.Extensions),
		); err != nil {
			return batchFailure(result, "storage", "relations", relation.clientID, err), nil
		}
		if _, err := insertMutationWithCommit(
			ctx, tx, "relate", relation.value.ID, relation.value.Actor, "", commitID,
		); err != nil {
			return batchFailure(result, "storage", "relations", relation.clientID, err), nil
		}
		result.Accepted = append(result.Accepted, relation.value.ID)
	}
	if len(result.Accepted) > 0 {
		if err := updateManifestTimestamp(ctx, tx, time.Now().UTC()); err != nil {
			return result, fmt.Errorf("update batch manifest: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit batch transaction: %w", err)
	}
	result.CommitID = commitID
	return result, nil
}

func prepareBatchEvent(
	input format.EventProposal,
	requestActor, id string,
) (model.Event, model.Provenance, error) {
	if input.EventType == "" || input.Namespace == "" {
		return model.Event{}, model.Provenance{}, fmt.Errorf("event type and namespace are required")
	}
	if err := model.ValidateJSON(input.Payload); err != nil {
		return model.Event{}, model.Provenance{}, fmt.Errorf("event payload: %w", err)
	}
	payload, err := model.CanonicalJSON(input.Payload, json.RawMessage(`{}`))
	if err != nil {
		return model.Event{}, model.Provenance{}, err
	}
	recordedAt, err := parseBatchTime(input.RecordedAt)
	if err != nil {
		return model.Event{}, model.Provenance{}, fmt.Errorf("recorded_at: %w", err)
	}
	occurredAt, err := parseBatchOptionalTime(input.OccurredAt)
	if err != nil {
		return model.Event{}, model.Provenance{}, fmt.Errorf("occurred_at: %w", err)
	}
	provenance := input.Provenance.ModelProvenance()
	if provenance.Agent == "" {
		provenance.Agent = firstNonEmpty(input.Actor, requestActor)
	}
	if err := prepareProvenance(&provenance, id); err != nil {
		return model.Event{}, model.Provenance{}, err
	}
	event := model.Event{
		ID:                 id,
		EventType:          input.EventType,
		Payload:            payload,
		PayloadContentType: firstNonEmpty(input.PayloadContentType, "application/json"),
		OccurredAt:         occurredAt,
		RecordedAt:         recordedAt,
		Namespace:          input.Namespace,
		Actor:              firstNonEmpty(input.Actor, requestActor),
		ProvenanceID:       provenance.ID,
		Extensions:         model.JSONOrEmpty(input.Extensions),
	}
	event.ContentHash, err = model.HashEvent(event)
	if err != nil {
		return model.Event{}, model.Provenance{}, err
	}
	return event, provenance, nil
}

func prepareBatchMemory(
	input format.MemoryProposal,
	requestActor, id string,
) (model.Memory, model.Provenance, error) {
	if err := model.ValidateMemoryKind(input.Kind); err != nil {
		return model.Memory{}, model.Provenance{}, err
	}
	if input.Namespace == "" {
		return model.Memory{}, model.Provenance{}, fmt.Errorf("memory namespace is required")
	}
	status := firstNonEmpty(input.Status, "active")
	if err := model.ValidateMemoryStatus(status); err != nil {
		return model.Memory{}, model.Provenance{}, err
	}
	if input.Content == "" && len(input.StructuredValue) == 0 {
		return model.Memory{}, model.Provenance{}, fmt.Errorf("memory content or structured value is required")
	}
	if err := model.ValidateJSON(input.StructuredValue); err != nil {
		return model.Memory{}, model.Provenance{}, err
	}
	if err := model.ValidateJSON(input.Confidence); err != nil {
		return model.Memory{}, model.Provenance{}, err
	}
	structured, err := model.CanonicalJSON(input.StructuredValue, json.RawMessage(`{}`))
	if err != nil {
		return model.Memory{}, model.Provenance{}, err
	}
	confidence, err := model.CanonicalJSON(input.Confidence, json.RawMessage(`{}`))
	if err != nil {
		return model.Memory{}, model.Provenance{}, err
	}
	from, err := parseBatchOptionalTime(input.ValidFrom)
	if err != nil {
		return model.Memory{}, model.Provenance{}, err
	}
	to, err := parseBatchOptionalTime(input.ValidTo)
	if err != nil {
		return model.Memory{}, model.Provenance{}, err
	}
	if err := model.ValidateInterval(from, to); err != nil {
		return model.Memory{}, model.Provenance{}, err
	}
	recordedAt, err := parseBatchTime(input.RecordedAt)
	if err != nil {
		return model.Memory{}, model.Provenance{}, err
	}
	provenance := input.Provenance.ModelProvenance()
	if provenance.Agent == "" {
		provenance.Agent = requestActor
	}
	if err := prepareProvenance(&provenance, id); err != nil {
		return model.Memory{}, model.Provenance{}, err
	}
	memory := model.Memory{
		ID:              id,
		Kind:            input.Kind,
		Content:         input.Content,
		StructuredValue: structured,
		SubjectEntityID: input.SubjectEntityID,
		Predicate:       input.Predicate,
		ObjectEntityID:  input.ObjectEntityID,
		ObjectValue:     input.ObjectValue,
		ValidFrom:       from,
		ValidTo:         to,
		RecordedAt:      recordedAt,
		Status:          status,
		Namespace:       input.Namespace,
		Confidence:      confidence,
		ProvenanceID:    provenance.ID,
		Extensions:      model.JSONOrEmpty(input.Extensions),
	}
	memory.ContentHash, err = model.HashMemory(memory)
	if err != nil {
		return model.Memory{}, model.Provenance{}, err
	}
	return memory, provenance, nil
}

func prepareBatchEntity(
	input format.EntityProposal,
	id string,
) (model.Entity, model.Provenance, error) {
	if input.EntityType == "" || input.CanonicalName == "" || input.Namespace == "" {
		return model.Entity{}, model.Provenance{}, fmt.Errorf("entity type, canonical name, and namespace are required")
	}
	if err := model.ValidateJSON(input.Aliases); err != nil {
		return model.Entity{}, model.Provenance{}, err
	}
	aliases, err := model.CanonicalJSON(input.Aliases, json.RawMessage(`[]`))
	if err != nil {
		return model.Entity{}, model.Provenance{}, err
	}
	provenance := input.Provenance.ModelProvenance()
	if err := prepareProvenance(&provenance, id); err != nil {
		return model.Entity{}, model.Provenance{}, err
	}
	entity := model.Entity{
		ID:            id,
		EntityType:    input.EntityType,
		CanonicalName: input.CanonicalName,
		Aliases:       aliases,
		Namespace:     input.Namespace,
		ProvenanceID:  provenance.ID,
		Extensions:    model.JSONOrEmpty(input.Extensions),
	}
	entity.ContentHash, err = model.HashEntity(entity)
	if err != nil {
		return model.Entity{}, model.Provenance{}, err
	}
	return entity, provenance, nil
}

func parseBatchTime(value string) (time.Time, error) {
	if value == "" {
		return time.Now().UTC(), nil
	}
	return model.ParseTime(value)
}

func parseBatchOptionalTime(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := model.ParseTime(value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func batchFailure(
	result format.BatchResult,
	category, path, clientID string,
	err error,
) format.BatchResult {
	result.CommitID = ""
	result.Accepted = nil
	result.Errors = []format.OperationError{{
		Category: category,
		Message:  err.Error(),
		Path:     path,
		ClientID: clientID,
	}}
	return result
}
