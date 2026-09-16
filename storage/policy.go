package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"trace/model"
)

type policySelector struct {
	ID        string `json:"id"`
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
}

type policyConditions struct {
	Purpose string `json:"purpose"`
}

// AddPolicy stores an explicit namespace-scoped access rule
func (s *Store) AddPolicy(ctx context.Context, input model.PolicyInput) (model.Policy, error) {
	if s.readOnly {
		return model.Policy{}, errors.New("trace is read-only")
	}
	if input.Effect != model.PolicyAllow && input.Effect != model.PolicyDeny {
		return model.Policy{}, fmt.Errorf("unsupported policy effect %q", input.Effect)
	}
	if input.Principal == "" || input.Operation == "" || input.Namespace == "" {
		return model.Policy{}, errors.New("policy principal, operation, and namespace are required")
	}
	if err := model.ValidateJSON(input.ResourceSelector); err != nil {
		return model.Policy{}, fmt.Errorf("policy resource selector: %w", err)
	}
	if err := model.ValidateJSON(input.Conditions); err != nil {
		return model.Policy{}, fmt.Errorf("policy conditions: %w", err)
	}
	selector, err := model.CanonicalJSON(input.ResourceSelector, json.RawMessage(`{}`))
	if err != nil {
		return model.Policy{}, fmt.Errorf("policy resource selector: %w", err)
	}
	conditions, err := model.CanonicalJSON(input.Conditions, json.RawMessage(`{}`))
	if err != nil {
		return model.Policy{}, fmt.Errorf("policy conditions: %w", err)
	}
	if err := validatePolicySelector(selector); err != nil {
		return model.Policy{}, err
	}
	if err := validatePolicyConditions(conditions); err != nil {
		return model.Policy{}, err
	}
	id := input.ID
	if id == "" {
		id, err = model.NewID()
		if err != nil {
			return model.Policy{}, err
		}
	}
	now := time.Now().UTC()
	policy := model.Policy{
		ID:               id,
		Effect:           input.Effect,
		Principal:        input.Principal,
		Operation:        input.Operation,
		ResourceSelector: selector,
		Conditions:       conditions,
		CreatedAt:        now,
		ExpiresAt:        input.ExpiresAt,
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Policy{}, fmt.Errorf("begin policy transaction: %w", err)
	}
	defer tx.Rollback()
	if err := insertNamespace(ctx, tx, input.Namespace, now); err != nil {
		return model.Policy{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO policy (
			id, effect, principal, operation, resource_selector, conditions,
			created_at, expires_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`,
		policy.ID,
		policy.Effect,
		policy.Principal,
		policy.Operation,
		string(policy.ResourceSelector),
		string(policy.Conditions),
		model.FormatTime(policy.CreatedAt),
		formatOptionalTime(policy.ExpiresAt),
	); err != nil {
		return model.Policy{}, fmt.Errorf("insert policy: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO policy_binding (policy_id, namespace) VALUES (?, ?)
	`, policy.ID, input.Namespace); err != nil {
		return model.Policy{}, fmt.Errorf("insert policy binding: %w", err)
	}
	if _, err := insertMutation(ctx, tx, "add_policy", policy.ID, input.Actor, ""); err != nil {
		return model.Policy{}, err
	}
	if err := updateManifestTimestamp(ctx, tx, now); err != nil {
		return model.Policy{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Policy{}, fmt.Errorf("commit policy: %w", err)
	}
	return policy, nil
}

// Authorize checks a caller against matching namespace policy bindings
func (s *Store) Authorize(
	ctx context.Context,
	access model.AccessContext,
	operation, recordID string,
) error {
	if access.Principal == "" {
		return errors.New("authorization denied: principal is required")
	}
	kind, namespace, err := s.recordKindNamespace(ctx, recordID)
	if err != nil {
		return err
	}
	when := access.At
	if when.IsZero() {
		when = time.Now().UTC()
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT p.effect, p.principal, p.operation, p.resource_selector,
		       p.conditions, p.expires_at, b.namespace
		FROM policy p
		JOIN policy_binding b ON b.policy_id = p.id
		WHERE (p.principal = ? OR p.principal = '*')
		  AND p.operation = ?
		  AND b.namespace = ?
		ORDER BY p.id ASC
	`, access.Principal, operation, namespace)
	if err != nil {
		return fmt.Errorf("query authorization policies: %w", err)
	}
	defer rows.Close()
	allowed := false
	for rows.Next() {
		var effect, principal, policyOperation, selectorJSON, conditionsJSON string
		var expiresAt, bindingNamespace sql.NullString
		if err := rows.Scan(
			&effect,
			&principal,
			&policyOperation,
			&selectorJSON,
			&conditionsJSON,
			&expiresAt,
			&bindingNamespace,
		); err != nil {
			return fmt.Errorf("scan authorization policy: %w", err)
		}
		if expiresAt.Valid {
			expiry, err := model.ParseTime(expiresAt.String)
			if err != nil {
				return fmt.Errorf("parse policy expiry: %w", err)
			}
			if !when.Before(expiry) {
				continue
			}
		}
		var selector policySelector
		if err := json.Unmarshal([]byte(selectorJSON), &selector); err != nil {
			return fmt.Errorf("parse policy selector: %w", err)
		}
		if selector.ID != "" && selector.ID != recordID ||
			selector.Kind != "" && selector.Kind != kind ||
			selector.Namespace != "" && selector.Namespace != namespace {
			continue
		}
		var conditions policyConditions
		if err := json.Unmarshal([]byte(conditionsJSON), &conditions); err != nil {
			return fmt.Errorf("parse policy conditions: %w", err)
		}
		if conditions.Purpose != "" && conditions.Purpose != access.Purpose {
			continue
		}
		if effect == model.PolicyDeny {
			return fmt.Errorf("authorization denied: policy denies %s for %s", operation, recordID)
		}
		if effect == model.PolicyAllow && principal != "" {
			allowed = true
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read authorization policies: %w", err)
	}
	if !allowed {
		return fmt.Errorf("authorization denied: no policy allows %s for %s", operation, recordID)
	}
	return nil
}

// GetEventAuthorized reads an event after evaluating the caller policy
func (s *Store) GetEventAuthorized(
	ctx context.Context,
	id string,
	access model.AccessContext,
) (model.Event, error) {
	if err := s.Authorize(ctx, access, model.OperationRead, id); err != nil {
		return model.Event{}, err
	}
	return s.GetEvent(ctx, id)
}

// GetMemoryAuthorized reads a memory after evaluating the caller policy
func (s *Store) GetMemoryAuthorized(
	ctx context.Context,
	id string,
	access model.AccessContext,
) (model.Memory, error) {
	if err := s.Authorize(ctx, access, model.OperationRead, id); err != nil {
		return model.Memory{}, err
	}
	return s.GetMemory(ctx, id)
}

// QueryMemoriesAuthorized returns only memories allowed by the caller policy
func (s *Store) QueryMemoriesAuthorized(
	ctx context.Context,
	query model.MemoryQuery,
	access model.AccessContext,
) (model.QueryResult, error) {
	result, err := s.QueryMemories(ctx, query)
	if err != nil {
		return model.QueryResult{}, err
	}
	visible := make([]model.Memory, 0, len(result.Memories))
	for _, memory := range result.Memories {
		if err := s.Authorize(ctx, access, model.OperationRead, memory.ID); err == nil {
			visible = append(visible, memory)
		}
	}
	result.Memories = visible
	result.Conflicts = filterConflictGroups(result.Conflicts, visible)
	result.EvidenceState = evidenceStateForMemories(result.Memories, result.Conflicts)
	return result, nil
}

// SearchAuthorized returns ranked evidence after caller policy filtering
func (s *Store) SearchAuthorized(
	ctx context.Context,
	query model.RetrievalQuery,
	access model.AccessContext,
) (model.RetrievalResult, error) {
	result, err := s.Search(ctx, query)
	if err != nil {
		return model.RetrievalResult{}, err
	}
	visible := make([]model.RetrievalHit, 0, len(result.Hits))
	for _, hit := range result.Hits {
		if err := s.Authorize(ctx, access, readOperationForHit(hit), hit.ID); err == nil {
			visible = append(visible, hit)
		}
	}
	result.Hits = visible
	visibleMemories := make([]model.Memory, 0)
	for _, hit := range visible {
		if hit.Memory != nil {
			visibleMemories = append(visibleMemories, *hit.Memory)
		}
	}
	result.Conflicts = filterConflictGroups(result.Conflicts, visibleMemories)
	result.EvidenceState = evidenceStateForHits(result.Hits, result.Conflicts)
	result.Context = compileContext(result.Hits, result.Snapshot, query.ContextByteLimit)
	if query.Strict && (result.EvidenceState == "weak" || result.EvidenceState == "conflicting") {
		result.Context = model.ContextResult{Snapshot: result.Snapshot}
	}
	return result, nil
}

// HistoryAuthorized returns mutation history for records visible to the caller
func (s *Store) HistoryAuthorized(
	ctx context.Context,
	targetID string,
	access model.AccessContext,
) ([]model.Mutation, error) {
	if targetID != "" {
		if err := s.Authorize(ctx, access, model.OperationHistory, targetID); err != nil {
			return nil, err
		}
		return s.History(ctx, targetID)
	}
	history, err := s.History(ctx, "")
	if err != nil {
		return nil, err
	}
	visible := make([]model.Mutation, 0, len(history))
	for _, mutation := range history {
		if _, _, err := s.recordKindNamespace(ctx, mutation.TargetID); err != nil {
			continue
		}
		if err := s.Authorize(ctx, access, model.OperationHistory, mutation.TargetID); err == nil {
			visible = append(visible, mutation)
		}
	}
	return visible, nil
}

// ExplainAuthorized returns only evidence the caller is allowed to inspect
func (s *Store) ExplainAuthorized(
	ctx context.Context,
	targetID string,
	access model.AccessContext,
) (model.Explanation, error) {
	if err := s.Authorize(ctx, access, model.OperationExplain, targetID); err != nil {
		return model.Explanation{}, err
	}
	explanation, err := s.Explain(ctx, targetID)
	if err != nil {
		return model.Explanation{}, err
	}
	events := make([]model.Event, 0, len(explanation.SourceEvents))
	for _, event := range explanation.SourceEvents {
		if s.Authorize(ctx, access, model.OperationRead, event.ID) == nil {
			events = append(events, event)
		}
	}
	memories := make([]model.Memory, 0, len(explanation.SourceMemories))
	for _, memory := range explanation.SourceMemories {
		if s.Authorize(ctx, access, model.OperationRead, memory.ID) == nil {
			memories = append(memories, memory)
		}
	}
	entities := make([]model.Entity, 0, len(explanation.SourceEntities))
	for _, entity := range explanation.SourceEntities {
		if s.Authorize(ctx, access, model.OperationRead, entity.ID) == nil {
			entities = append(entities, entity)
		}
	}
	explanation.SourceEvents = events
	explanation.SourceMemories = memories
	explanation.SourceEntities = entities
	return explanation, nil
}

func readOperationForHit(hit model.RetrievalHit) string {
	if hit.Kind == "event" {
		return model.OperationRead
	}
	return model.OperationSearch
}

func filterConflictGroups(
	groups []model.ConflictGroup,
	visible []model.Memory,
) []model.ConflictGroup {
	ids := make(map[string]bool, len(visible))
	for _, memory := range visible {
		ids[memory.ID] = true
	}
	result := make([]model.ConflictGroup, 0, len(groups))
	for _, group := range groups {
		idsInGroup := make([]string, 0, len(group.MemoryIDs))
		for _, id := range group.MemoryIDs {
			if ids[id] {
				idsInGroup = append(idsInGroup, id)
			}
		}
		if len(idsInGroup) > 1 {
			group.MemoryIDs = idsInGroup
			result = append(result, group)
		}
	}
	return result
}

func evidenceStateForMemories(
	memories []model.Memory,
	groups []model.ConflictGroup,
) string {
	if len(memories) == 0 {
		return "absent"
	}
	for _, group := range groups {
		if !group.Resolved {
			return "conflicting"
		}
	}
	return "supported"
}

func evidenceStateForHits(
	hits []model.RetrievalHit,
	groups []model.ConflictGroup,
) string {
	if len(hits) == 0 {
		return "absent"
	}
	for _, group := range groups {
		if !group.Resolved {
			return "conflicting"
		}
	}
	return "supported"
}

func validatePolicySelector(value json.RawMessage) error {
	var selector policySelector
	if err := json.Unmarshal(value, &selector); err != nil {
		return fmt.Errorf("policy resource selector must be an object: %w", err)
	}
	return nil
}

func validatePolicyConditions(value json.RawMessage) error {
	var conditions policyConditions
	if err := json.Unmarshal(value, &conditions); err != nil {
		return fmt.Errorf("policy conditions must be an object: %w", err)
	}
	return nil
}

func (s *Store) recordKindNamespace(ctx context.Context, id string) (string, string, error) {
	var kind, namespace string
	err := s.db.QueryRowContext(ctx, `
		SELECT kind, namespace FROM (
			SELECT 'event' AS kind, id, namespace FROM event
			UNION ALL
			SELECT 'memory' AS kind, id, namespace FROM memory
			UNION ALL
			SELECT 'entity' AS kind, id, namespace FROM entity
		)
		WHERE id = ?
	`, id).Scan(&kind, &namespace)
	if err != nil {
		return "", "", fmt.Errorf("find record %s: %w", id, err)
	}
	return kind, namespace, nil
}
