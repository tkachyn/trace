// model defines Trace's language-neutral canonical records
package model

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// format identity values must remain stable within a compatible file version
const (
	FormatName       = "trace"
	FormatVersion    = "0.1"
	SchemaVersion    = 2
	ApplicationID    = 0x54524345
	DefaultGenerator = "trace-go"

	RelationDerivedFrom = "derived_from"
	RelationSupports    = "supports"
	RelationContradicts = "contradicts"
	RelationConfirms    = "confirms"
	RelationSupersedes  = "supersedes"
	RelationInvalidates = "invalidates"
	RelationSummarizes  = "summarizes"
)

// manifest describes the file format and schema used by a Trace file
type Manifest struct {
	FormatName       string
	FormatVersion    string
	SchemaVersion    int
	CreatedAt        time.Time
	UpdatedAt        time.Time
	Generator        string
	FileID           string
	RequiredFeatures []string
}

// provenance records where a source or derived record came from
type Provenance struct {
	ID                  string
	SourceType          string
	SourceID            string
	SourceURI           string
	Agent               string
	Model               string
	Provider            string
	ConversationID      string
	MessageID           string
	Operation           string
	CreatedAt           time.Time
	ParentProvenanceIDs []string
	Extensions          json.RawMessage
}

// event preserves an input observation without interpreting its meaning
type Event struct {
	ID                 string
	EventType          string
	Payload            json.RawMessage
	PayloadContentType string
	OccurredAt         *time.Time
	RecordedAt         time.Time
	Namespace          string
	Actor              string
	ProvenanceID       string
	ContentHash        string
	Extensions         json.RawMessage
}

// memory represents a durable fact, observation, or compiled context
type Memory struct {
	ID              string
	Kind            string
	Content         string
	StructuredValue json.RawMessage
	SubjectEntityID string
	Predicate       string
	ObjectEntityID  string
	ObjectValue     string
	ValidFrom       *time.Time
	ValidTo         *time.Time
	RecordedAt      time.Time
	Status          string
	Namespace       string
	Confidence      json.RawMessage
	ProvenanceID    string
	ContentHash     string
	Extensions      json.RawMessage
}

// entity identifies a named or typed referent used by memory records
type Entity struct {
	ID            string
	EntityType    string
	CanonicalName string
	Aliases       json.RawMessage
	Namespace     string
	ProvenanceID  string
	ContentHash   string
	Extensions    json.RawMessage
}

// derivation explains how one record supports or changes another
type Derivation struct {
	ID         string
	SourceID   string
	TargetID   string
	Relation   string
	CreatedAt  time.Time
	Actor      string
	Extensions json.RawMessage
}

// mutation records an accepted change at a transaction boundary
type Mutation struct {
	Sequence    int64
	ID          string
	CommitID    string
	Operation   string
	TargetID    string
	Actor       string
	CreatedAt   time.Time
	RequestHash string
	Metadata    json.RawMessage
}

// relationInput contains an explicit relationship between two records
type RelationInput struct {
	ID         string
	SourceID   string
	TargetID   string
	Relation   string
	Actor      string
	Extensions json.RawMessage
}

// memoryQuery defines deterministic temporal and lifecycle filters
type MemoryQuery struct {
	Namespace          string
	ValidAt            *time.Time
	RecordedBefore     *time.Time
	RecordedAfter      *time.Time
	IncludeSuperseded  bool
	IncludeInvalidated bool
	IncludeRedacted    bool
	Limit              int
}

// conflictGroup identifies contradictory records returned by a query
type ConflictGroup struct {
	MemoryIDs             []string
	Resolved              bool
	ResolutionRelationIDs []string
}

// queryResult contains temporal records and explicit conflict state
type QueryResult struct {
	Memories      []Memory
	Conflicts     []ConflictGroup
	EvidenceState string
}

// retrievalQuery defines deterministic local retrieval filters and ranking inputs
type RetrievalQuery struct {
	Text               string
	ExactIDs           []string
	ContentHash        string
	Namespace          string
	NamespaceScope     []string
	Kind               string
	SubjectEntityID    string
	Predicate          string
	ObjectValue        string
	ValidAt            *time.Time
	RecordedBefore     *time.Time
	RecordedAfter      *time.Time
	IncludeSuperseded  bool
	IncludeInvalidated bool
	IncludeRedacted    bool
	MinimumScore       float64
	Strict             bool
	Limit              int
	ContextByteLimit   int
}

// scoreBreakdown explains the deterministic components of one retrieval score
type ScoreBreakdown struct {
	ExactMatch      float64
	TextMatch       float64
	MetadataMatch   float64
	TemporalMatch   float64
	Recency         float64
	ConflictPenalty float64
	Total           float64
}

// RetrievalHit identifies one authorized canonical record returned by retrieval
type RetrievalHit struct {
	ID            string
	Kind          string
	Namespace     string
	Content       string
	ContentHash   string
	Memory        *Memory
	Event         *Event
	Score         ScoreBreakdown
	EvidenceState string
}

// ContextResult contains a reproducible presentation of retrieval evidence
type ContextResult struct {
	Snapshot  Snapshot
	HitIDs    []string
	Content   string
	Bytes     int
	Truncated bool
}

// RetrievalResult contains ranked evidence, conflicts, and compiled context
type RetrievalResult struct {
	Hits          []RetrievalHit
	Conflicts     []ConflictGroup
	EvidenceState string
	Snapshot      Snapshot
	Context       ContextResult
	Warnings      []string
}

// snapshot identifies a logical point in the append-only mutation history
type Snapshot struct {
	Sequence  int64
	UpdatedAt time.Time
}

// diffResult reports accepted mutations between two snapshots
type DiffResult struct {
	From      Snapshot
	To        Snapshot
	Mutations []Mutation
	Added     []RecordReference
}

// recordReference identifies a record without duplicating its full payload
type RecordReference struct {
	ID   string
	Kind string
}

// explanation contains deterministic evidence for one record
type Explanation struct {
	TargetID       string
	TargetKind     string
	Event          *Event
	Memory         *Memory
	Entity         *Entity
	SourceEvents   []Event
	SourceMemories []Memory
	SourceEntities []Entity
	Provenance     []Provenance
	Derivations    []Derivation
	Mutations      []Mutation
}

// eventInput contains the caller-provided fields for a new event
type EventInput struct {
	ID                 string
	EventType          string
	Payload            json.RawMessage
	PayloadContentType string
	OccurredAt         *time.Time
	RecordedAt         time.Time
	Namespace          string
	Actor              string
	Provenance         Provenance
	Extensions         json.RawMessage
}

// memoryInput contains the caller-provided fields for a new memory
type MemoryInput struct {
	ID              string
	Kind            string
	Content         string
	StructuredValue json.RawMessage
	SubjectEntityID string
	Predicate       string
	ObjectEntityID  string
	ObjectValue     string
	ValidFrom       *time.Time
	ValidTo         *time.Time
	RecordedAt      time.Time
	Status          string
	Namespace       string
	Confidence      json.RawMessage
	Provenance      Provenance
	DerivedFrom     []string
	Extensions      json.RawMessage
}

// entityInput contains the caller-provided fields for a new entity
type EntityInput struct {
	ID            string
	EntityType    string
	CanonicalName string
	Aliases       json.RawMessage
	Namespace     string
	Provenance    Provenance
	Extensions    json.RawMessage
}

// newID creates a time-sortable identifier without an external UUID dependency
func NewID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate trace id: %w", err)
	}

	now := uint64(time.Now().UnixMilli())
	value[0] = byte(now >> 40)
	value[1] = byte(now >> 32)
	value[2] = byte(now >> 24)
	value[3] = byte(now >> 16)
	value[4] = byte(now >> 8)
	value[5] = byte(now)
	value[6] = (value[6] & 0x0f) | 0x70
	value[8] = (value[8] & 0x3f) | 0x80

	return fmt.Sprintf(
		"%08x-%04x-%04x-%04x-%012x",
		value[0:4],
		value[4:6],
		value[6:8],
		value[8:10],
		value[10:16],
	), nil
}

// formatTime writes timestamps in the canonical UTC representation
func FormatTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

// parseTime reads the canonical timestamp representation
func ParseTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse timestamp %q: %w", value, err)
	}
	return parsed.UTC(), nil
}

// validateJSON rejects malformed JSON before it reaches storage
func ValidateJSON(value json.RawMessage) error {
	if len(value) == 0 {
		return nil
	}
	if !json.Valid(value) {
		return fmt.Errorf("invalid JSON")
	}
	return nil
}

// canonicalJSON normalizes JSON before hashing and storage
func CanonicalJSON(value, fallback json.RawMessage) (json.RawMessage, error) {
	if len(value) == 0 {
		value = fallback
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decode JSON: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("JSON contains multiple values")
		}
		return nil, fmt.Errorf("decode trailing JSON: %w", err)
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return nil, fmt.Errorf("encode canonical JSON: %w", err)
	}
	return encoded, nil
}

// jsonOrEmpty supplies an empty object for optional object metadata
func JSONOrEmpty(value json.RawMessage) json.RawMessage {
	if len(value) == 0 {
		return json.RawMessage(`{}`)
	}
	return value
}

// hashEvent hashes the immutable semantic fields of an event
func HashEvent(value Event) (string, error) {
	input := struct {
		EventType          string
		Payload            json.RawMessage
		PayloadContentType string
		OccurredAt         string
		RecordedAt         string
		Namespace          string
		Actor              string
		ProvenanceID       string
		Extensions         json.RawMessage
	}{
		EventType:          value.EventType,
		Payload:            value.Payload,
		PayloadContentType: value.PayloadContentType,
		RecordedAt:         FormatTime(value.RecordedAt),
		Namespace:          value.Namespace,
		Actor:              value.Actor,
		ProvenanceID:       value.ProvenanceID,
		Extensions:         JSONOrEmpty(value.Extensions),
	}
	if value.OccurredAt != nil {
		input.OccurredAt = FormatTime(*value.OccurredAt)
	}
	return hashJSON(input)
}

// hashMemory hashes the immutable semantic fields of a memory
func HashMemory(value Memory) (string, error) {
	input := struct {
		Kind            string
		Content         string
		StructuredValue json.RawMessage
		SubjectEntityID string
		Predicate       string
		ObjectEntityID  string
		ObjectValue     string
		ValidFrom       string
		ValidTo         string
		RecordedAt      string
		Status          string
		Namespace       string
		Confidence      json.RawMessage
		ProvenanceID    string
		Extensions      json.RawMessage
	}{
		Kind:            value.Kind,
		Content:         value.Content,
		StructuredValue: value.StructuredValue,
		SubjectEntityID: value.SubjectEntityID,
		Predicate:       value.Predicate,
		ObjectEntityID:  value.ObjectEntityID,
		ObjectValue:     value.ObjectValue,
		RecordedAt:      FormatTime(value.RecordedAt),
		Status:          value.Status,
		Namespace:       value.Namespace,
		Confidence:      value.Confidence,
		ProvenanceID:    value.ProvenanceID,
		Extensions:      JSONOrEmpty(value.Extensions),
	}
	if value.ValidFrom != nil {
		input.ValidFrom = FormatTime(*value.ValidFrom)
	}
	if value.ValidTo != nil {
		input.ValidTo = FormatTime(*value.ValidTo)
	}
	return hashJSON(input)
}

// hashEntity hashes the immutable semantic fields of an entity
func HashEntity(value Entity) (string, error) {
	input := struct {
		EntityType    string
		CanonicalName string
		Aliases       json.RawMessage
		Namespace     string
		ProvenanceID  string
		Extensions    json.RawMessage
	}{
		EntityType:    value.EntityType,
		CanonicalName: value.CanonicalName,
		Aliases:       value.Aliases,
		Namespace:     value.Namespace,
		ProvenanceID:  value.ProvenanceID,
		Extensions:    JSONOrEmpty(value.Extensions),
	}
	return hashJSON(input)
}

// validateInterval enforces Trace's half-open time interval rule
func ValidateInterval(from, to *time.Time) error {
	if from != nil && to != nil && !from.Before(*to) {
		return fmt.Errorf("valid_to must be after valid_from")
	}
	return nil
}

// validateMemoryKind restricts records to the v0.1 memory kinds
func ValidateMemoryKind(kind string) error {
	switch kind {
	case "fact", "observation", "context":
		return nil
	default:
		return fmt.Errorf("unsupported memory kind %q", kind)
	}
}

// validateMemoryStatus restricts records to the v0.1 lifecycle states
func ValidateMemoryStatus(status string) error {
	switch status {
	case "active", "superseded", "uncertain", "invalidated", "redacted":
		return nil
	default:
		return fmt.Errorf("unsupported memory status %q", status)
	}
}

// validateRelation restricts lifecycle edges to known v0.1 semantics
func ValidateRelation(relation string) error {
	switch relation {
	case RelationDerivedFrom,
		RelationSupports,
		RelationContradicts,
		RelationConfirms,
		RelationSupersedes,
		RelationInvalidates,
		RelationSummarizes:
		return nil
	default:
		return fmt.Errorf("unsupported relation %q", relation)
	}
}

func hashJSON(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal canonical content: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
