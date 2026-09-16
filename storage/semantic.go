package storage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"trace/model"
)

// EmbeddingProvider supplies optional vectors without becoming a Trace dependency
type EmbeddingProvider interface {
	Spec() model.EmbeddingSpec
	Embed(context.Context, []string) ([][]float32, error)
}

type semanticMemory struct {
	id, namespace, content, hash string
}

// ensureSemanticIndex creates optional semantic index tables for writable files
func ensureSemanticIndex(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS semantic_index_meta (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			model TEXT NOT NULL,
			dimensions INTEGER NOT NULL,
			revision TEXT NOT NULL,
			adapter_version TEXT NOT NULL,
			build_sequence INTEGER NOT NULL,
			built_at TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS semantic_index (
			record_id TEXT PRIMARY KEY,
			record_kind TEXT NOT NULL,
			namespace TEXT NOT NULL,
			model TEXT NOT NULL,
			dimensions INTEGER NOT NULL,
			revision TEXT NOT NULL,
			adapter_version TEXT NOT NULL,
			source_content_hash TEXT NOT NULL,
			vector BLOB NOT NULL,
			build_sequence INTEGER NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("create semantic index: %w", err)
	}
	return nil
}

// RebuildSemanticIndex replaces the optional vector index from canonical memories
func (s *Store) RebuildSemanticIndex(
	ctx context.Context,
	provider EmbeddingProvider,
) error {
	if s.readOnly {
		return errors.New("trace is read-only")
	}
	if provider == nil {
		return errors.New("embedding provider is required")
	}
	spec, err := validateEmbeddingSpec(provider.Spec())
	if err != nil {
		return err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, namespace, content, structured_value, content_hash
		FROM memory
		ORDER BY id ASC
	`)
	if err != nil {
		return fmt.Errorf("read memories for semantic index: %w", err)
	}
	memories := make([]semanticMemory, 0)
	texts := make([]string, 0)
	for rows.Next() {
		var value semanticMemory
		var structured string
		if err := rows.Scan(
			&value.id,
			&value.namespace,
			&value.content,
			&structured,
			&value.hash,
		); err != nil {
			rows.Close()
			return fmt.Errorf("scan memory for semantic index: %w", err)
		}
		memories = append(memories, value)
		texts = append(texts, value.content+" "+structured)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("read semantic memories: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close semantic memories: %w", err)
	}
	vectors, err := provider.Embed(ctx, texts)
	if err != nil {
		return fmt.Errorf("embed memories: %w", err)
	}
	if len(vectors) != len(memories) {
		return fmt.Errorf("embedding count %d does not match memory count %d", len(vectors), len(memories))
	}
	encoded := make([][]byte, len(vectors))
	for index, vector := range vectors {
		encoded[index], err = encodeVector(vector, spec.Dimensions)
		if err != nil {
			return fmt.Errorf("encode embedding %d: %w", index, err)
		}
	}
	snapshot, err := s.Snapshot(ctx)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin semantic rebuild: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "DELETE FROM semantic_index"); err != nil {
		return fmt.Errorf("clear semantic index: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM semantic_index_meta"); err != nil {
		return fmt.Errorf("clear semantic index metadata: %w", err)
	}
	for index, memory := range memories {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO semantic_index (
				record_id, record_kind, namespace, model, dimensions, revision,
				adapter_version, source_content_hash, vector, build_sequence
			) VALUES (?, 'memory', ?, ?, ?, ?, ?, ?, ?, ?)
		`,
			memory.id,
			memory.namespace,
			spec.Model,
			spec.Dimensions,
			spec.Revision,
			spec.AdapterVersion,
			memory.hash,
			encoded[index],
			snapshot.Sequence,
		); err != nil {
			return fmt.Errorf("write semantic vector: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO semantic_index_meta (
			id, model, dimensions, revision, adapter_version, build_sequence, built_at
		) VALUES (1, ?, ?, ?, ?, ?, ?)
	`,
		spec.Model,
		spec.Dimensions,
		spec.Revision,
		spec.AdapterVersion,
		snapshot.Sequence,
		model.FormatTime(snapshot.UpdatedAt),
	); err != nil {
		return fmt.Errorf("write semantic metadata: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit semantic rebuild: %w", err)
	}
	return nil
}

// SemanticIndexStatus reports optional semantic index freshness and compatibility
func (s *Store) SemanticIndexStatus(ctx context.Context) (model.IndexStatus, error) {
	status := model.IndexStatus{}
	if !s.hasSemanticIndex(ctx) {
		status.Warning = "semantic index is unavailable"
		return status, nil
	}
	status.Available = true
	var modelName, revision, adapter, builtAt string
	err := s.db.QueryRowContext(ctx, `
		SELECT model, dimensions, revision, adapter_version, build_sequence, built_at
		FROM semantic_index_meta WHERE id = 1
	`).Scan(
		&modelName,
		&status.Dimensions,
		&revision,
		&adapter,
		&status.BuildSequence,
		&builtAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		status.Warning = "semantic index has not been built"
		return status, nil
	}
	if err != nil {
		return status, fmt.Errorf("read semantic index status: %w", err)
	}
	status.Model = modelName
	status.Revision = revision
	status.AdapterVersion = adapter
	if _, err := model.ParseTime(builtAt); err != nil {
		return status, fmt.Errorf("parse semantic build time: %w", err)
	}
	if err := s.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM semantic_index",
	).Scan(&status.RecordCount); err != nil {
		return status, fmt.Errorf("count semantic index: %w", err)
	}
	snapshot, err := s.Snapshot(ctx)
	if err != nil {
		return status, err
	}
	status.CurrentSequence = snapshot.Sequence
	status.Ready = true
	if status.CurrentSequence != status.BuildSequence {
		status.Stale = true
		status.Ready = false
		status.Warning = "semantic index is stale"
	}
	return status, nil
}

// SearchHybrid adds optional semantic candidates to deterministic retrieval
func (s *Store) SearchHybrid(
	ctx context.Context,
	query model.HybridQuery,
	provider EmbeddingProvider,
) (model.RetrievalResult, error) {
	result, err := s.Search(ctx, query.RetrievalQuery)
	if err != nil {
		return model.RetrievalResult{}, err
	}
	if provider == nil || query.Text == "" {
		return result, nil
	}
	status, err := s.SemanticIndexStatus(ctx)
	if err != nil {
		return model.RetrievalResult{}, err
	}
	spec, err := validateEmbeddingSpec(provider.Spec())
	if err != nil {
		return model.RetrievalResult{}, err
	}
	if !status.Ready || status.Model != spec.Model ||
		status.Dimensions != spec.Dimensions ||
		status.Revision != spec.Revision ||
		status.AdapterVersion != spec.AdapterVersion {
		if status.Warning == "" {
			status.Warning = "semantic index is incompatible"
		}
		result.Warnings = append(result.Warnings, status.Warning)
		return result, nil
	}
	vectors, err := provider.Embed(ctx, []string{query.Text})
	if err != nil {
		return model.RetrievalResult{}, fmt.Errorf("embed retrieval query: %w", err)
	}
	if len(vectors) != 1 {
		return model.RetrievalResult{}, errors.New("embedding provider returned an invalid query count")
	}
	if _, err := encodeVector(vectors[0], spec.Dimensions); err != nil {
		return model.RetrievalResult{}, fmt.Errorf("encode query embedding: %w", err)
	}
	queryVector := vectors[0]
	rows, err := s.db.QueryContext(ctx, `
		SELECT record_id, namespace, source_content_hash, vector
		FROM semantic_index
		WHERE model = ? AND dimensions = ? AND revision = ? AND adapter_version = ?
		ORDER BY record_id ASC
	`, spec.Model, spec.Dimensions, spec.Revision, spec.AdapterVersion)
	if err != nil {
		return model.RetrievalResult{}, fmt.Errorf("read semantic candidates: %w", err)
	}
	type semanticCandidate struct {
		id, namespace, hash string
		vector              []byte
	}
	candidates := make([]semanticCandidate, 0)
	for rows.Next() {
		var candidate semanticCandidate
		if err := rows.Scan(
			&candidate.id,
			&candidate.namespace,
			&candidate.hash,
			&candidate.vector,
		); err != nil {
			rows.Close()
			return model.RetrievalResult{}, fmt.Errorf("scan semantic candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return model.RetrievalResult{}, fmt.Errorf("read semantic candidates: %w", err)
	}
	if err := rows.Close(); err != nil {
		return model.RetrievalResult{}, fmt.Errorf("close semantic candidates: %w", err)
	}
	byID := make(map[string]bool)
	for _, hit := range result.Hits {
		byID[hit.ID] = true
	}
	candidateQuery := query.RetrievalQuery
	candidateQuery.Text = ""
	for _, candidate := range candidates {
		if !matchesNamespace(candidate.namespace, candidateQuery.Namespace, candidateQuery.NamespaceScope) {
			continue
		}
		memory, err := s.GetMemory(ctx, candidate.id)
		if err != nil || memory.ContentHash != candidate.hash || !matchesMemoryRetrieval(memory, candidateQuery) {
			continue
		}
		if query.Access.Principal != "" &&
			s.Authorize(ctx, query.Access, model.OperationSearch, candidate.id) != nil {
			continue
		}
		vector, err := decodeVector(candidate.vector, spec.Dimensions)
		if err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("invalid semantic vector for %s", candidate.id))
			continue
		}
		similarity := cosineSimilarity(queryVector, vector)
		hit := model.RetrievalHit{
			ID:          memory.ID,
			Kind:        "memory",
			Namespace:   memory.Namespace,
			Content:     memory.Content,
			ContentHash: memory.ContentHash,
			Memory:      &memory,
		}
		hit.Score = initialRetrievalScore(hit, candidateQuery)
		hit.Score.SemanticMatch = similarity * semanticWeight(query.SemanticWeight)
		if !byID[candidate.id] {
			result.Hits = append(result.Hits, hit)
			byID[candidate.id] = true
		} else {
			for index := range result.Hits {
				if result.Hits[index].ID == candidate.id &&
					result.Hits[index].Score.SemanticMatch < hit.Score.SemanticMatch {
					result.Hits[index].Score.SemanticMatch = hit.Score.SemanticMatch
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return model.RetrievalResult{}, fmt.Errorf("read semantic candidates: %w", err)
	}
	conflicts, err := s.retrievalConflicts(ctx, result.Hits)
	if err != nil {
		return model.RetrievalResult{}, err
	}
	result.Conflicts = conflicts
	conflictingIDs := make(map[string]bool)
	unresolved := false
	for _, conflict := range conflicts {
		for _, id := range conflict.MemoryIDs {
			conflictingIDs[id] = true
		}
		unresolved = unresolved || !conflict.Resolved
	}
	assignRetrievalScores(result.Hits, conflictingIDs)
	sort.SliceStable(result.Hits, func(left, right int) bool {
		if result.Hits[left].Score.Total != result.Hits[right].Score.Total {
			return result.Hits[left].Score.Total > result.Hits[right].Score.Total
		}
		return result.Hits[left].ID < result.Hits[right].ID
	})
	if query.Limit > 0 && len(result.Hits) > query.Limit {
		result.Hits = result.Hits[:query.Limit]
	}
	result.EvidenceState = "absent"
	if len(result.Hits) > 0 {
		result.EvidenceState = "supported"
		if unresolved {
			result.EvidenceState = "conflicting"
		}
	}
	for index := range result.Hits {
		result.Hits[index].EvidenceState = result.EvidenceState
	}
	result.Context = compileContext(result.Hits, result.Snapshot, query.ContextByteLimit)
	if query.Strict && (result.EvidenceState == "conflicting" || result.EvidenceState == "weak") {
		result.Context = model.ContextResult{Snapshot: result.Snapshot}
	}
	return result, nil
}

func validateEmbeddingSpec(spec model.EmbeddingSpec) (model.EmbeddingSpec, error) {
	if spec.Model == "" || spec.Revision == "" || spec.AdapterVersion == "" {
		return spec, errors.New("embedding model, revision, and adapter version are required")
	}
	if spec.Dimensions <= 0 {
		return spec, errors.New("embedding dimensions must be positive")
	}
	return spec, nil
}

func encodeVector(vector []float32, dimensions int) ([]byte, error) {
	if len(vector) != dimensions {
		return nil, fmt.Errorf("vector dimensions %d, want %d", len(vector), dimensions)
	}
	encoded := bytes.NewBuffer(make([]byte, 0, dimensions*4))
	for _, value := range vector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, errors.New("vector contains a non-finite value")
		}
		if err := binary.Write(encoded, binary.LittleEndian, value); err != nil {
			return nil, err
		}
	}
	return encoded.Bytes(), nil
}

func decodeVector(encoded []byte, dimensions int) ([]float32, error) {
	if len(encoded) != dimensions*4 {
		return nil, fmt.Errorf("vector bytes %d, want %d", len(encoded), dimensions*4)
	}
	vector := make([]float32, dimensions)
	if err := binary.Read(bytes.NewReader(encoded), binary.LittleEndian, &vector); err != nil {
		return nil, err
	}
	for _, value := range vector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, errors.New("vector contains a non-finite value")
		}
	}
	return vector, nil
}

func cosineSimilarity(left, right []float32) float64 {
	var dot, leftNorm, rightNorm float64
	for index := range left {
		l := float64(left[index])
		r := float64(right[index])
		dot += l * r
		leftNorm += l * l
		rightNorm += r * r
	}
	if leftNorm == 0 || rightNorm == 0 {
		return 0
	}
	return dot / (math.Sqrt(leftNorm) * math.Sqrt(rightNorm))
}

func semanticWeight(value float64) float64 {
	if value <= 0 {
		return 0.5
	}
	return value
}

func (s *Store) hasSemanticIndex(ctx context.Context) bool {
	var name string
	return s.db.QueryRowContext(
		ctx,
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'semantic_index'",
	).Scan(&name) == nil
}
