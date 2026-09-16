package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"trace/model"
)

// ensureRetrievalIndex creates the rebuildable full-text index for writable files
func ensureRetrievalIndex(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
		CREATE VIRTUAL TABLE IF NOT EXISTS retrieval_fts USING fts5(
			record_id UNINDEXED,
			record_kind UNINDEXED,
			namespace UNINDEXED,
			content,
			content_hash UNINDEXED
		)
	`); err != nil {
		return fmt.Errorf("create retrieval index: %w", err)
	}
	return nil
}

// insertRetrievalDocument adds one canonical record to the rebuildable index
func insertRetrievalDocument(
	ctx context.Context,
	tx *sql.Tx,
	id, kind, namespace, content, contentHash string,
) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO retrieval_fts (
			record_id, record_kind, namespace, content, content_hash
		) VALUES (?, ?, ?, ?, ?)
	`, id, kind, namespace, content, contentHash); err != nil {
		return fmt.Errorf("insert retrieval index document: %w", err)
	}
	return nil
}

// RebuildRetrievalIndex reconstructs full-text entries from canonical records
func (s *Store) RebuildRetrievalIndex(ctx context.Context) error {
	if s.readOnly {
		return errors.New("trace is read-only")
	}
	if err := ensureRetrievalIndex(ctx, s.db); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin retrieval index rebuild: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "DELETE FROM retrieval_fts"); err != nil {
		return fmt.Errorf("clear retrieval index: %w", err)
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT id, event_type, namespace, payload, content_hash
		FROM event
		ORDER BY id ASC
	`)
	if err != nil {
		return fmt.Errorf("read events for retrieval index: %w", err)
	}
	type document struct {
		id, kind, namespace, content, hash string
	}
	documents := make([]document, 0)
	for rows.Next() {
		var value document
		var eventType string
		if err := rows.Scan(
			&value.id,
			&eventType,
			&value.namespace,
			&value.content,
			&value.hash,
		); err != nil {
			rows.Close()
			return fmt.Errorf("scan event for retrieval index: %w", err)
		}
		value.kind = "event"
		documents = append(documents, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("read event retrieval documents: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close event retrieval rows: %w", err)
	}

	rows, err = tx.QueryContext(ctx, `
		SELECT id, namespace, content, structured_value, content_hash
		FROM memory
		ORDER BY id ASC
	`)
	if err != nil {
		return fmt.Errorf("read memories for retrieval index: %w", err)
	}
	for rows.Next() {
		var value document
		var structured string
		if err := rows.Scan(
			&value.id,
			&value.namespace,
			&value.content,
			&structured,
			&value.hash,
		); err != nil {
			rows.Close()
			return fmt.Errorf("scan memory for retrieval index: %w", err)
		}
		value.kind = "memory"
		value.content += " " + structured
		documents = append(documents, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("read memory retrieval documents: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close memory retrieval rows: %w", err)
	}

	sort.Slice(documents, func(left, right int) bool {
		return documents[left].id < documents[right].id
	})
	for _, document := range documents {
		if err := insertRetrievalDocument(
			ctx,
			tx,
			document.id,
			document.kind,
			document.namespace,
			document.content,
			document.hash,
		); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit retrieval index rebuild: %w", err)
	}
	return nil
}

// Search performs deterministic exact, metadata, and full-text retrieval
func (s *Store) Search(ctx context.Context, query model.RetrievalQuery) (model.RetrievalResult, error) {
	if query.Limit < 0 {
		return model.RetrievalResult{}, errors.New("retrieval limit must not be negative")
	}
	if query.MinimumScore < 0 {
		return model.RetrievalResult{}, errors.New("minimum score must not be negative")
	}
	limit := query.Limit
	if limit == 0 {
		limit = 100
	}

	candidates, err := s.retrievalCandidates(ctx, query)
	if err != nil {
		return model.RetrievalResult{}, err
	}
	hits := make([]model.RetrievalHit, 0, len(candidates))
	for _, candidate := range candidates {
		hit, ok, err := s.retrievalHit(ctx, candidate, query)
		if err != nil {
			return model.RetrievalResult{}, err
		}
		if ok {
			hit.Score = initialRetrievalScore(hit, query)
			hits = append(hits, hit)
		}
	}

	conflicts, err := s.retrievalConflicts(ctx, hits)
	if err != nil {
		return model.RetrievalResult{}, err
	}
	conflictingIDs := make(map[string]bool)
	unresolved := false
	for _, conflict := range conflicts {
		for _, id := range conflict.MemoryIDs {
			conflictingIDs[id] = true
		}
		if !conflict.Resolved {
			unresolved = true
		}
	}
	assignRetrievalScores(hits, conflictingIDs)
	sort.SliceStable(hits, func(left, right int) bool {
		if hits[left].Score.Total != hits[right].Score.Total {
			return hits[left].Score.Total > hits[right].Score.Total
		}
		return hits[left].ID < hits[right].ID
	})
	if len(hits) > limit {
		hits = hits[:limit]
	}

	evidenceState := "absent"
	weak := false
	if len(hits) > 0 {
		evidenceState = "supported"
		for _, hit := range hits {
			if query.MinimumScore > 0 && hit.Score.Total < query.MinimumScore {
				weak = true
			}
		}
		if weak {
			evidenceState = "weak"
		}
		if unresolved {
			evidenceState = "conflicting"
		}
	}
	for index := range hits {
		hits[index].EvidenceState = evidenceState
	}

	snapshot, err := s.Snapshot(ctx)
	if err != nil {
		return model.RetrievalResult{}, err
	}
	contextResult := compileContext(hits, snapshot, query.ContextByteLimit)
	if query.Strict && (evidenceState == "weak" || evidenceState == "conflicting") {
		contextResult = model.ContextResult{Snapshot: snapshot}
	}
	warnings := make([]string, 0)
	if query.Text != "" && !s.hasRetrievalIndex(ctx) {
		warnings = append(warnings, "full-text index unavailable; used canonical scan")
	}
	return model.RetrievalResult{
		Hits:          hits,
		Conflicts:     conflicts,
		EvidenceState: evidenceState,
		Snapshot:      snapshot,
		Context:       contextResult,
		Warnings:      warnings,
	}, nil
}

type retrievalCandidate struct {
	id, kind string
}

func (s *Store) retrievalCandidates(
	ctx context.Context,
	query model.RetrievalQuery,
) ([]retrievalCandidate, error) {
	if len(query.ExactIDs) > 0 {
		candidates := make([]retrievalCandidate, 0, len(query.ExactIDs))
		for _, id := range query.ExactIDs {
			if id != "" {
				candidates = append(candidates, retrievalCandidate{id: id})
			}
		}
		return candidates, nil
	}
	if query.ContentHash != "" {
		rows, err := s.db.QueryContext(ctx, `
			SELECT id, 'memory' AS kind FROM memory WHERE content_hash = ?
			UNION ALL
			SELECT id, 'event' AS kind FROM event WHERE content_hash = ?
			ORDER BY id ASC
		`, query.ContentHash, query.ContentHash)
		if err != nil {
			return nil, fmt.Errorf("find exact content hash: %w", err)
		}
		return scanRetrievalCandidates(rows)
	}

	if query.Text != "" && s.hasRetrievalIndex(ctx) {
		statement := `
			SELECT record_id, record_kind
			FROM retrieval_fts
			WHERE retrieval_fts MATCH ?
			ORDER BY record_id ASC
		`
		rows, err := s.db.QueryContext(ctx, statement, quoteFTSQuery(query.Text))
		if err == nil {
			candidates, scanErr := scanRetrievalCandidates(rows)
			if scanErr == nil {
				return candidates, nil
			}
		}
	}

	kind := query.Kind
	if kind == "" {
		kind = "memory"
	}
	switch kind {
	case "memory", "event":
		table := kind
		rows, err := s.db.QueryContext(
			ctx,
			"SELECT id, ? AS kind FROM "+table+" ORDER BY id ASC",
			kind,
		)
		if err != nil {
			return nil, fmt.Errorf("list %s retrieval candidates: %w", kind, err)
		}
		return scanRetrievalCandidates(rows)
	case "all":
		rows, err := s.db.QueryContext(ctx, `
			SELECT id, 'memory' AS kind FROM memory
			UNION ALL
			SELECT id, 'event' AS kind FROM event
			ORDER BY id ASC
		`)
		if err != nil {
			return nil, fmt.Errorf("list retrieval candidates: %w", err)
		}
		return scanRetrievalCandidates(rows)
	default:
		return nil, fmt.Errorf("unsupported retrieval kind %q", query.Kind)
	}
}

func scanRetrievalCandidates(rows *sql.Rows) ([]retrievalCandidate, error) {
	defer rows.Close()
	candidates := make([]retrievalCandidate, 0)
	for rows.Next() {
		var candidate retrievalCandidate
		if err := rows.Scan(&candidate.id, &candidate.kind); err != nil {
			return nil, fmt.Errorf("scan retrieval candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read retrieval candidates: %w", err)
	}
	return candidates, nil
}

func (s *Store) retrievalHit(
	ctx context.Context,
	candidate retrievalCandidate,
	query model.RetrievalQuery,
) (model.RetrievalHit, bool, error) {
	if candidate.kind == "" {
		kind, err := s.recordKind(ctx, candidate.id)
		if err != nil {
			return model.RetrievalHit{}, false, nil
		}
		candidate.kind = kind
	}
	switch candidate.kind {
	case "memory":
		memory, err := s.GetMemory(ctx, candidate.id)
		if err != nil {
			return model.RetrievalHit{}, false, nil
		}
		if !matchesMemoryRetrieval(memory, query) {
			return model.RetrievalHit{}, false, nil
		}
		return model.RetrievalHit{
			ID:          memory.ID,
			Kind:        "memory",
			Namespace:   memory.Namespace,
			Content:     memory.Content,
			ContentHash: memory.ContentHash,
			Memory:      &memory,
		}, true, nil
	case "event":
		event, err := s.GetEvent(ctx, candidate.id)
		if err != nil {
			return model.RetrievalHit{}, false, nil
		}
		if !matchesEventRetrieval(event, query) {
			return model.RetrievalHit{}, false, nil
		}
		return model.RetrievalHit{
			ID:          event.ID,
			Kind:        "event",
			Namespace:   event.Namespace,
			Content:     string(event.Payload),
			ContentHash: event.ContentHash,
			Event:       &event,
		}, true, nil
	default:
		return model.RetrievalHit{}, false, nil
	}
}

func matchesMemoryRetrieval(memory model.Memory, query model.RetrievalQuery) bool {
	if !matchesNamespace(memory.Namespace, query.Namespace, query.NamespaceScope) ||
		(query.Kind != "" && query.Kind != "all" && query.Kind != "memory" && query.Kind != memory.Kind) ||
		(query.SubjectEntityID != "" && query.SubjectEntityID != memory.SubjectEntityID) ||
		(query.Predicate != "" && query.Predicate != memory.Predicate) ||
		(query.ObjectValue != "" && query.ObjectValue != memory.ObjectValue) ||
		(query.ContentHash != "" && query.ContentHash != memory.ContentHash) {
		return false
	}
	if query.ValidAt != nil {
		if memory.ValidFrom == nil ||
			memory.ValidFrom.After(*query.ValidAt) ||
			(memory.ValidTo != nil && !query.ValidAt.Before(*memory.ValidTo)) {
			return false
		}
	}
	if query.RecordedBefore != nil && !memory.RecordedAt.Before(*query.RecordedBefore) {
		return false
	}
	if query.RecordedAfter != nil && memory.RecordedAt.Before(*query.RecordedAfter) {
		return false
	}
	if memory.Status == "superseded" && !query.IncludeSuperseded && query.ValidAt == nil {
		return false
	}
	if memory.Status == "invalidated" && !query.IncludeInvalidated {
		return false
	}
	if memory.Status == "redacted" && !query.IncludeRedacted {
		return false
	}
	if query.Text != "" && !containsTerms(memory.Content+" "+string(memory.StructuredValue), query.Text) {
		return false
	}
	return true
}

func matchesEventRetrieval(event model.Event, query model.RetrievalQuery) bool {
	if !matchesNamespace(event.Namespace, query.Namespace, query.NamespaceScope) ||
		(query.Kind != "" && query.Kind != "all" && query.Kind != "event") ||
		(query.ContentHash != "" && query.ContentHash != event.ContentHash) ||
		(query.ValidAt != nil) {
		return false
	}
	if query.RecordedBefore != nil && !event.RecordedAt.Before(*query.RecordedBefore) {
		return false
	}
	if query.RecordedAfter != nil && event.RecordedAt.Before(*query.RecordedAfter) {
		return false
	}
	return query.Text == "" || containsTerms(string(event.Payload), query.Text)
}

func matchesNamespace(namespace, exact string, scope []string) bool {
	if exact != "" && namespace != exact {
		return false
	}
	if len(scope) == 0 {
		return true
	}
	for _, allowed := range scope {
		if namespace == allowed {
			return true
		}
	}
	return false
}

func containsTerms(content, query string) bool {
	lowerContent := strings.ToLower(content)
	for _, term := range strings.Fields(strings.ToLower(query)) {
		if !strings.Contains(lowerContent, strings.Trim(term, `"`)) {
			return false
		}
	}
	return true
}

func initialRetrievalScore(
	hit model.RetrievalHit,
	query model.RetrievalQuery,
) model.ScoreBreakdown {
	score := model.ScoreBreakdown{}
	if len(query.ExactIDs) > 0 || query.ContentHash != "" {
		score.ExactMatch = 1
	}
	if query.Text != "" {
		content := strings.ToLower(hit.Content)
		terms := strings.Fields(strings.ToLower(query.Text))
		matches := 0
		for _, term := range terms {
			if strings.Contains(content, strings.Trim(term, `"`)) {
				matches++
			}
		}
		if len(terms) > 0 {
			score.TextMatch = 0.60 * float64(matches) / float64(len(terms))
		}
	}
	if query.Kind != "" || query.SubjectEntityID != "" ||
		query.Predicate != "" || query.ObjectValue != "" ||
		query.Namespace != "" || len(query.NamespaceScope) > 0 {
		score.MetadataMatch = 0.20
	}
	if query.ValidAt != nil || query.RecordedBefore != nil || query.RecordedAfter != nil {
		score.TemporalMatch = 0.15
	}
	return score
}

func quoteFTSQuery(query string) string {
	terms := strings.Fields(query)
	quoted := make([]string, 0, len(terms))
	for _, term := range terms {
		quoted = append(quoted, `"`+strings.ReplaceAll(term, `"`, `""`)+`"`)
	}
	return strings.Join(quoted, " AND ")
}

func (s *Store) hasRetrievalIndex(ctx context.Context) bool {
	var name string
	return s.db.QueryRowContext(
		ctx,
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'retrieval_fts'",
	).Scan(&name) == nil
}

func (s *Store) retrievalConflicts(
	ctx context.Context,
	hits []model.RetrievalHit,
) ([]model.ConflictGroup, error) {
	memories := make([]model.Memory, 0)
	for _, hit := range hits {
		if hit.Memory != nil {
			memories = append(memories, *hit.Memory)
		}
	}
	return s.findConflicts(ctx, memories)
}

func assignRetrievalScores(hits []model.RetrievalHit, conflictingIDs map[string]bool) {
	if len(hits) == 0 {
		return
	}
	minRecorded, maxRecorded := retrievalRecordedAt(hits[0]), retrievalRecordedAt(hits[0])
	for _, hit := range hits[1:] {
		recorded := retrievalRecordedAt(hit)
		if recorded.Before(minRecorded) {
			minRecorded = recorded
		}
		if recorded.After(maxRecorded) {
			maxRecorded = recorded
		}
	}
	span := maxRecorded.Sub(minRecorded)
	for index := range hits {
		hit := &hits[index]
		score := &hit.Score
		if conflictingIDs[hit.ID] {
			score.ConflictPenalty = 0.25
		}
		if span > 0 {
			score.Recency = 0.05 * retrievalRecordedAt(*hit).Sub(minRecorded).Seconds() / span.Seconds()
		} else {
			score.Recency = 0.05
		}
		score.Total = score.ExactMatch + score.TextMatch + score.MetadataMatch +
			score.TemporalMatch + score.Recency - score.ConflictPenalty
	}
}

func compileContext(
	hits []model.RetrievalHit,
	snapshot model.Snapshot,
	byteLimit int,
) model.ContextResult {
	result := model.ContextResult{Snapshot: snapshot, HitIDs: []string{}}
	if byteLimit < 0 {
		byteLimit = 0
	}
	var lines []string
	for _, hit := range hits {
		line := fmt.Sprintf(
			"[%s] %s %s %s",
			hit.ID,
			hit.Kind,
			hit.Namespace,
			strings.TrimSpace(hit.Content),
		)
		if byteLimit > 0 && result.Bytes+len(line)+1 > byteLimit {
			result.Truncated = true
			break
		}
		lines = append(lines, line)
		result.HitIDs = append(result.HitIDs, hit.ID)
		result.Bytes += len(line) + 1
	}
	result.Content = strings.Join(lines, "\n")
	if len(lines) > 0 {
		result.Bytes = len(result.Content)
	}
	return result
}

func retrievalRecordedAt(hit model.RetrievalHit) time.Time {
	if hit.Memory != nil {
		return hit.Memory.RecordedAt
	}
	if hit.Event != nil {
		return hit.Event.RecordedAt
	}
	return time.Time{}
}
