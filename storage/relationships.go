package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"trace/model"
)

// LookupEntities finds entities by canonical name or JSON aliases
func (s *Store) LookupEntities(
	ctx context.Context,
	query model.EntityQuery,
) ([]model.Entity, error) {
	if query.Limit < 0 {
		return nil, errors.New("entity lookup limit must not be negative")
	}
	limit := query.Limit
	if limit == 0 {
		limit = 100
	}
	statement := `
		SELECT DISTINCT e.id, e.entity_type, e.canonical_name, e.aliases,
		       e.namespace, e.provenance_id, e.content_hash, e.extensions
		FROM entity e
	`
	args := []any{}
	if query.Text != "" {
		statement += `
			LEFT JOIN json_each(e.aliases) alias
			WHERE (lower(e.canonical_name) = lower(?) OR lower(alias.value) = lower(?))
		`
		args = append(args, query.Text, query.Text)
	} else {
		statement += " WHERE 1 = 1"
	}
	if query.Namespace != "" {
		statement += " AND e.namespace = ?"
		args = append(args, query.Namespace)
	}
	statement += " ORDER BY e.id ASC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("lookup entities: %w", err)
	}
	entities := make([]model.Entity, 0)
	for rows.Next() {
		entity, err := scanEntity(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		entities = append(entities, entity)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("read entities: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close entity rows: %w", err)
	}
	visible := make([]model.Entity, 0, len(entities))
	for _, entity := range entities {
		if query.Access.Principal != "" &&
			s.Authorize(ctx, query.Access, model.OperationRead, entity.ID) != nil {
			continue
		}
		visible = append(visible, entity)
	}
	return visible, nil
}

// Traverse follows explicit derivations with deterministic breadth-first order
func (s *Store) Traverse(
	ctx context.Context,
	query model.RelationshipQuery,
) (model.RelationshipResult, error) {
	if len(query.StartIDs) == 0 {
		return model.RelationshipResult{}, errors.New("relationship traversal requires a start ID")
	}
	if query.MaxHops < 0 {
		return model.RelationshipResult{}, errors.New("relationship hop limit must not be negative")
	}
	if query.MaxHops == 0 {
		query.MaxHops = 1
	}
	if query.Direction == "" {
		query.Direction = "outgoing"
	}
	if query.Direction != "outgoing" && query.Direction != "incoming" && query.Direction != "both" {
		return model.RelationshipResult{}, fmt.Errorf("unsupported relationship direction %q", query.Direction)
	}
	limit := query.Limit
	if limit == 0 {
		limit = 100
	}
	relationSet := make(map[string]bool, len(query.Relations))
	for _, relation := range query.Relations {
		if err := model.ValidateRelation(relation); err != nil {
			return model.RelationshipResult{}, err
		}
		relationSet[relation] = true
	}
	type node struct {
		id   string
		hops int
	}
	queue := make([]node, 0, len(query.StartIDs))
	visited := make(map[string]bool)
	for _, id := range query.StartIDs {
		if id == "" || visited[id] {
			continue
		}
		if _, _, err := s.recordKindNamespace(ctx, id); err != nil {
			return model.RelationshipResult{}, err
		}
		if query.Access.Principal != "" &&
			s.Authorize(ctx, query.Access, model.OperationRead, id) != nil {
			continue
		}
		visited[id] = true
		queue = append(queue, node{id: id})
	}
	result := model.RelationshipResult{
		Records: []model.RecordReference{},
		Edges:   []model.RelationshipEdge{},
	}
	for len(queue) > 0 && len(result.Records) < limit {
		current := queue[0]
		queue = queue[1:]
		if current.hops >= query.MaxHops {
			continue
		}
		derivations, err := s.traversalDerivations(ctx, current.id, query.Direction)
		if err != nil {
			return model.RelationshipResult{}, err
		}
		for _, derivation := range derivations {
			if len(result.Records) >= limit {
				break
			}
			if len(relationSet) > 0 && !relationSet[derivation.Relation] {
				continue
			}
			nextID := derivation.TargetID
			if query.Direction == "incoming" {
				nextID = derivation.SourceID
			} else if query.Direction == "both" && derivation.SourceID == current.id {
				nextID = derivation.TargetID
			} else if query.Direction == "both" {
				nextID = derivation.SourceID
			}
			kind, namespace, err := s.recordKindNamespace(ctx, nextID)
			if err != nil {
				continue
			}
			if query.Namespace != "" && query.Namespace != namespace {
				continue
			}
			if query.Access.Principal != "" &&
				s.Authorize(ctx, query.Access, model.OperationRead, nextID) != nil {
				continue
			}
			result.Edges = append(result.Edges, model.RelationshipEdge{
				Derivation: derivation,
				Hops:       current.hops + 1,
			})
			if !visited[nextID] {
				visited[nextID] = true
				result.Records = append(result.Records, model.RecordReference{
					ID:   nextID,
					Kind: kind,
				})
				queue = append(queue, node{id: nextID, hops: current.hops + 1})
			}
		}
	}
	sort.Slice(result.Edges, func(left, right int) bool {
		if result.Edges[left].Hops != result.Edges[right].Hops {
			return result.Edges[left].Hops < result.Edges[right].Hops
		}
		return result.Edges[left].Derivation.ID < result.Edges[right].Derivation.ID
	})
	return result, nil
}

func (s *Store) traversalDerivations(
	ctx context.Context,
	id, direction string,
) ([]model.Derivation, error) {
	statement := `
		SELECT id, source_id, target_id, relation, created_at, actor, extensions
		FROM derivation
		WHERE source_id = ? OR target_id = ?
		ORDER BY id ASC
	`
	rows, err := s.db.QueryContext(ctx, statement, id, id)
	if err != nil {
		return nil, fmt.Errorf("query relationship edges: %w", err)
	}
	defer rows.Close()
	result := make([]model.Derivation, 0)
	for rows.Next() {
		var value model.Derivation
		var createdAt, extensions string
		if err := rows.Scan(
			&value.ID,
			&value.SourceID,
			&value.TargetID,
			&value.Relation,
			&createdAt,
			&value.Actor,
			&extensions,
		); err != nil {
			return nil, fmt.Errorf("scan relationship edge: %w", err)
		}
		if direction == "outgoing" && value.SourceID != id {
			continue
		}
		if direction == "incoming" && value.TargetID != id {
			continue
		}
		parsed, err := model.ParseTime(createdAt)
		if err != nil {
			return nil, err
		}
		value.CreatedAt = parsed
		value.Extensions = json.RawMessage(extensions)
		result = append(result, value)
	}
	return result, rows.Err()
}

func scanEntity(scanner interface{ Scan(...any) error }) (model.Entity, error) {
	var entity model.Entity
	var aliases, extensions string
	if err := scanner.Scan(
		&entity.ID,
		&entity.EntityType,
		&entity.CanonicalName,
		&aliases,
		&entity.Namespace,
		&entity.ProvenanceID,
		&entity.ContentHash,
		&extensions,
	); err != nil {
		return model.Entity{}, fmt.Errorf("scan entity: %w", err)
	}
	entity.Aliases = json.RawMessage(aliases)
	entity.Extensions = json.RawMessage(extensions)
	return entity, nil
}
