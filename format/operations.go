// format defines versioned machine-readable Trace ingestion contracts
package format

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"trace/model"
)

const (
	// batch protocol identifies the atomic ingestion request contract
	BatchProtocol = "trace.batch"
	// extraction protocol identifies provider-neutral extraction proposals
	ExtractionProtocol = "trace.extraction_proposal"
	// protocol version is the current machine-readable operation version
	ProtocolVersion = 1
)

// provenance contains JSON-compatible provenance fields for integrations
type Provenance struct {
	ID                  string          `json:"id,omitempty"`
	SourceType          string          `json:"source_type,omitempty"`
	SourceID            string          `json:"source_id,omitempty"`
	SourceURI           string          `json:"source_uri,omitempty"`
	Agent               string          `json:"agent,omitempty"`
	Model               string          `json:"model,omitempty"`
	Provider            string          `json:"provider,omitempty"`
	ConversationID      string          `json:"conversation_id,omitempty"`
	MessageID           string          `json:"message_id,omitempty"`
	Operation           string          `json:"operation,omitempty"`
	ParentProvenanceIDs []string        `json:"parent_provenance_ids,omitempty"`
	Extensions          json.RawMessage `json:"extensions,omitempty"`
}

// event proposal is one provider-neutral source event proposal
type EventProposal struct {
	ClientID           string          `json:"client_id"`
	ID                 string          `json:"id,omitempty"`
	EventType          string          `json:"event_type"`
	Payload            json.RawMessage `json:"payload"`
	PayloadContentType string          `json:"payload_content_type,omitempty"`
	OccurredAt         string          `json:"occurred_at,omitempty"`
	RecordedAt         string          `json:"recorded_at,omitempty"`
	Namespace          string          `json:"namespace,omitempty"`
	Actor              string          `json:"actor,omitempty"`
	Provenance         Provenance      `json:"provenance"`
	Extensions         json.RawMessage `json:"extensions,omitempty"`
}

// memory proposal is one provider-neutral derived memory proposal
type MemoryProposal struct {
	ClientID        string          `json:"client_id"`
	ID              string          `json:"id,omitempty"`
	Kind            string          `json:"kind"`
	Content         string          `json:"content,omitempty"`
	StructuredValue json.RawMessage `json:"structured_value,omitempty"`
	SubjectEntityID string          `json:"subject_entity_id,omitempty"`
	Predicate       string          `json:"predicate,omitempty"`
	ObjectEntityID  string          `json:"object_entity_id,omitempty"`
	ObjectValue     string          `json:"object_value,omitempty"`
	ValidFrom       string          `json:"valid_from,omitempty"`
	ValidTo         string          `json:"valid_to,omitempty"`
	RecordedAt      string          `json:"recorded_at,omitempty"`
	Status          string          `json:"status,omitempty"`
	Namespace       string          `json:"namespace,omitempty"`
	Confidence      json.RawMessage `json:"confidence,omitempty"`
	Provenance      Provenance      `json:"provenance"`
	DerivedFrom     []string        `json:"derived_from,omitempty"`
	Extensions      json.RawMessage `json:"extensions,omitempty"`
}

// entity proposal is one provider-neutral entity proposal
type EntityProposal struct {
	ClientID      string          `json:"client_id"`
	ID            string          `json:"id,omitempty"`
	EntityType    string          `json:"entity_type"`
	CanonicalName string          `json:"canonical_name"`
	Aliases       json.RawMessage `json:"aliases,omitempty"`
	Namespace     string          `json:"namespace,omitempty"`
	Provenance    Provenance      `json:"provenance"`
	Extensions    json.RawMessage `json:"extensions,omitempty"`
}

// relation proposal is one explicit relation proposal
type RelationProposal struct {
	ClientID   string          `json:"client_id,omitempty"`
	ID         string          `json:"id,omitempty"`
	SourceID   string          `json:"source_id"`
	TargetID   string          `json:"target_id"`
	Relation   string          `json:"relation"`
	Actor      string          `json:"actor,omitempty"`
	Extensions json.RawMessage `json:"extensions,omitempty"`
}

// batch request is an atomic, versioned ingestion request
type BatchRequest struct {
	Protocol  string             `json:"protocol"`
	Version   int                `json:"version"`
	RequestID string             `json:"request_id"`
	Actor     string             `json:"actor,omitempty"`
	Events    []EventProposal    `json:"events,omitempty"`
	Memories  []MemoryProposal   `json:"memories,omitempty"`
	Entities  []EntityProposal   `json:"entities,omitempty"`
	Relations []RelationProposal `json:"relations,omitempty"`
}

// extraction proposal is a provider-neutral proposal before Trace acceptance
type ExtractionProposal struct {
	Type       string             `json:"type"`
	Version    int                `json:"version"`
	ProposalID string             `json:"proposal_id"`
	Namespace  string             `json:"namespace"`
	Actor      string             `json:"actor"`
	Events     []EventProposal    `json:"events,omitempty"`
	Memories   []MemoryProposal   `json:"memories,omitempty"`
	Entities   []EntityProposal   `json:"entities,omitempty"`
	Relations  []RelationProposal `json:"relations,omitempty"`
	Unmapped   []json.RawMessage  `json:"unmapped,omitempty"`
	Warnings   []string           `json:"warnings,omitempty"`
}

// operation error identifies one rejected request field
type OperationError struct {
	Category string `json:"category"`
	Message  string `json:"message"`
	Path     string `json:"path,omitempty"`
	ClientID string `json:"client_id,omitempty"`
}

// batch result reports one committed or rejected atomic request
type BatchResult struct {
	Protocol  string            `json:"protocol"`
	Version   int               `json:"version"`
	RequestID string            `json:"request_id"`
	CommitID  string            `json:"commit_id,omitempty"`
	Accepted  []string          `json:"accepted,omitempty"`
	IDMap     map[string]string `json:"id_map,omitempty"`
	Errors    []OperationError  `json:"errors,omitempty"`
}

// validate checks the request envelope before storage is opened
func (request BatchRequest) Validate() error {
	if request.Protocol != BatchProtocol {
		return fmt.Errorf("unsupported batch protocol %q", request.Protocol)
	}
	if request.Version != ProtocolVersion {
		return fmt.Errorf("unsupported batch protocol version %d", request.Version)
	}
	if request.RequestID == "" {
		return fmt.Errorf("request_id is required")
	}
	return nil
}

// to batch request converts an extraction proposal into an atomic batch
func (proposal ExtractionProposal) ToBatchRequest() (BatchRequest, error) {
	if proposal.Type != ExtractionProtocol {
		return BatchRequest{}, fmt.Errorf("unsupported extraction proposal type %q", proposal.Type)
	}
	if proposal.Version != ProtocolVersion {
		return BatchRequest{}, fmt.Errorf("unsupported extraction proposal version %d", proposal.Version)
	}
	request := BatchRequest{
		Protocol:  BatchProtocol,
		Version:   ProtocolVersion,
		RequestID: proposal.ProposalID,
		Actor:     proposal.Actor,
		Events:    proposal.Events,
		Memories:  proposal.Memories,
		Entities:  proposal.Entities,
		Relations: proposal.Relations,
	}
	for index := range request.Events {
		if request.Events[index].Namespace == "" {
			request.Events[index].Namespace = proposal.Namespace
		}
	}
	for index := range request.Memories {
		if request.Memories[index].Namespace == "" {
			request.Memories[index].Namespace = proposal.Namespace
		}
	}
	for index := range request.Entities {
		if request.Entities[index].Namespace == "" {
			request.Entities[index].Namespace = proposal.Namespace
		}
	}
	return request, request.Validate()
}

// decode JSONL reads one header, record lines, and an optional commit marker
func DecodeJSONL(reader io.Reader) (BatchRequest, error) {
	scanner := bufio.NewScanner(reader)
	var request BatchRequest
	lineNumber := 0
	committed := false
	for scanner.Scan() {
		lineNumber++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(line, &envelope); err != nil {
			return BatchRequest{}, fmt.Errorf("JSONL line %d: %w", lineNumber, err)
		}
		switch envelope.Type {
		case "header":
			if err := json.Unmarshal(line, &request); err != nil {
				return BatchRequest{}, fmt.Errorf("JSONL header line %d: %w", lineNumber, err)
			}
		case "event":
			var value EventProposal
			if err := json.Unmarshal(line, &value); err != nil {
				return BatchRequest{}, fmt.Errorf("JSONL event line %d: %w", lineNumber, err)
			}
			request.Events = append(request.Events, value)
		case "memory":
			var value MemoryProposal
			if err := json.Unmarshal(line, &value); err != nil {
				return BatchRequest{}, fmt.Errorf("JSONL memory line %d: %w", lineNumber, err)
			}
			request.Memories = append(request.Memories, value)
		case "entity":
			var value EntityProposal
			if err := json.Unmarshal(line, &value); err != nil {
				return BatchRequest{}, fmt.Errorf("JSONL entity line %d: %w", lineNumber, err)
			}
			request.Entities = append(request.Entities, value)
		case "relation":
			var value RelationProposal
			if err := json.Unmarshal(line, &value); err != nil {
				return BatchRequest{}, fmt.Errorf("JSONL relation line %d: %w", lineNumber, err)
			}
			request.Relations = append(request.Relations, value)
		case "commit":
			committed = true
		default:
			return BatchRequest{}, fmt.Errorf("JSONL line %d has unsupported type %q", lineNumber, envelope.Type)
		}
	}
	if err := scanner.Err(); err != nil {
		return BatchRequest{}, fmt.Errorf("read JSONL: %w", err)
	}
	if committed || request.Protocol != "" {
		if err := request.Validate(); err != nil {
			return BatchRequest{}, err
		}
	}
	if !committed {
		return BatchRequest{}, fmt.Errorf("JSONL commit marker is required")
	}
	return request, nil
}

// model provenance converts an integration provenance value to the core model
func (value Provenance) ModelProvenance() model.Provenance {
	return model.Provenance{
		ID:                  value.ID,
		SourceType:          value.SourceType,
		SourceID:            value.SourceID,
		SourceURI:           value.SourceURI,
		Agent:               value.Agent,
		Model:               value.Model,
		Provider:            value.Provider,
		ConversationID:      value.ConversationID,
		MessageID:           value.MessageID,
		Operation:           value.Operation,
		ParentProvenanceIDs: value.ParentProvenanceIDs,
		Extensions:          value.Extensions,
	}
}

// marshal JSON emits stable compact operation payloads for subprocess clients
func (request BatchRequest) MarshalJSON() ([]byte, error) {
	type alias BatchRequest
	return json.Marshal(alias(request))
}

// normalize JSON ensures a request can be decoded without trailing values
func NormalizeJSON(data []byte, value any) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("JSON contains multiple values")
	}
	return nil
}
