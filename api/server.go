// api exposes the versioned local Trace HTTP contract
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"trace/format"
	"trace/model"
	"trace/storage"
)

// server serves one open Trace store through versioned JSON endpoints
type Server struct {
	store *storage.Store
}

// new server creates a local API handler for an open Trace store
func NewServer(store *storage.Store) http.Handler {
	return &Server{store: store}
}

// serve HTTP routes versioned API requests to the Trace store
func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if !strings.HasPrefix(request.URL.Path, "/v1/") {
		writeAPIError(writer, http.StatusNotFound, "protocol", "unsupported API path", "")
		return
	}
	path := strings.TrimPrefix(request.URL.Path, "/v1/")
	switch {
	case request.Method == http.MethodGet && path == "inspect":
		s.inspect(writer, request)
	case request.Method == http.MethodPost && path == "validate":
		s.validate(writer, request)
	case request.Method == http.MethodPost && path == "ingest":
		s.ingest(writer, request)
	case request.Method == http.MethodPost && path == "query":
		s.query(writer, request)
	case request.Method == http.MethodPost && path == "search":
		s.search(writer, request)
	case request.Method == http.MethodPost && path == "explain":
		s.explain(writer, request)
	case request.Method == http.MethodPost && path == "history":
		s.history(writer, request)
	case request.Method == http.MethodPost && path == "forget":
		s.forget(writer, request)
	case request.Method == http.MethodGet && strings.HasPrefix(path, "events/"):
		s.event(writer, request, strings.TrimPrefix(path, "events/"))
	case request.Method == http.MethodGet && strings.HasPrefix(path, "memories/"):
		s.memory(writer, request, strings.TrimPrefix(path, "memories/"))
	default:
		writeAPIError(writer, http.StatusNotFound, "protocol", "unsupported API endpoint", path)
	}
}

type apiErrorResponse struct {
	APIVersion int            `json:"api_version"`
	Error      apiErrorDetail `json:"error"`
}

type apiErrorDetail struct {
	Category  string `json:"category"`
	Message   string `json:"message"`
	Path      string `json:"path,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

func writeAPIError(writer http.ResponseWriter, status int, category, message, path string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(apiErrorResponse{
		APIVersion: 1,
		Error: apiErrorDetail{
			Category: category,
			Message:  message,
			Path:     path,
		},
	})
}

func writeAPIJSON(writer http.ResponseWriter, value any) {
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(value)
}

func decodeAPIJSON(request *http.Request, value any) error {
	decoder := json.NewDecoder(request.Body)
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("decode JSON request: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err == nil {
		return errors.New("request contains multiple JSON values")
	}
	return nil
}

func apiStatus(err error) (int, string) {
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "authorization denied"):
		return http.StatusForbidden, "unauthorized"
	case strings.Contains(message, "already exists") || strings.Contains(message, "conflict"):
		return http.StatusConflict, "conflict"
	case strings.Contains(message, "does not exist") || strings.Contains(message, "missing"):
		return http.StatusUnprocessableEntity, "reference"
	case strings.Contains(message, "invalid") || strings.Contains(message, "required") ||
		strings.Contains(message, "unsupported"):
		return http.StatusUnprocessableEntity, "validation"
	default:
		return http.StatusInternalServerError, "storage"
	}
}

func (s *Server) inspect(writer http.ResponseWriter, request *http.Request) {
	value, err := s.store.Inspect(request.Context())
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	writeAPIJSON(writer, value)
}

func (s *Server) validate(writer http.ResponseWriter, request *http.Request) {
	value, err := s.store.Validate(request.Context())
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	writeAPIJSON(writer, value)
}

func (s *Server) ingest(writer http.ResponseWriter, request *http.Request) {
	var envelope struct {
		Protocol string `json:"protocol"`
		Type     string `json:"type"`
	}
	body, err := readBody(request)
	if err != nil {
		writeAPIError(writer, http.StatusBadRequest, "protocol", err.Error(), "")
		return
	}
	if err := format.NormalizeJSON(body, &envelope); err != nil {
		writeAPIError(writer, http.StatusBadRequest, "protocol", err.Error(), "")
		return
	}
	var batch format.BatchRequest
	if envelope.Type == format.ExtractionProtocol {
		var proposal format.ExtractionProposal
		if err := format.NormalizeJSON(body, &proposal); err != nil {
			writeAPIError(writer, http.StatusBadRequest, "protocol", err.Error(), "")
			return
		}
		batch, err = proposal.ToBatchRequest()
	} else {
		err = format.NormalizeJSON(body, &batch)
	}
	if err != nil {
		writeAPIError(writer, http.StatusBadRequest, "protocol", err.Error(), "")
		return
	}
	result, err := s.store.IngestBatch(request.Context(), batch)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	if len(result.Errors) > 0 {
		status := http.StatusUnprocessableEntity
		if result.Errors[0].Category == "conflict" {
			status = http.StatusConflict
		}
		writer.WriteHeader(status)
		writeAPIJSON(writer, result)
		return
	}
	writeAPIJSON(writer, result)
}

type queryRequest struct {
	Namespace          string `json:"namespace"`
	ValidAt            string `json:"valid_at"`
	RecordedBefore     string `json:"recorded_before"`
	RecordedAfter      string `json:"recorded_after"`
	IncludeSuperseded  bool   `json:"include_superseded"`
	IncludeInvalidated bool   `json:"include_invalidated"`
	IncludeRedacted    bool   `json:"include_redacted"`
	Limit              int    `json:"limit"`
	Principal          string `json:"principal"`
	Purpose            string `json:"purpose"`
}

func (s *Server) query(writer http.ResponseWriter, request *http.Request) {
	var input queryRequest
	if err := decodeAPIJSON(request, &input); err != nil {
		writeAPIError(writer, http.StatusBadRequest, "protocol", err.Error(), "")
		return
	}
	point, before, after, err := parseAPITimes(input.ValidAt, input.RecordedBefore, input.RecordedAfter)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	if input.Principal == "" {
		writeAPIError(writer, http.StatusForbidden, "unauthorized", "principal is required", "principal")
		return
	}
	result, err := s.store.QueryMemoriesAuthorized(request.Context(), model.MemoryQuery{
		Namespace:          input.Namespace,
		ValidAt:            point,
		RecordedBefore:     before,
		RecordedAfter:      after,
		IncludeSuperseded:  input.IncludeSuperseded,
		IncludeInvalidated: input.IncludeInvalidated,
		IncludeRedacted:    input.IncludeRedacted,
		Limit:              input.Limit,
	}, model.AccessContext{
		Principal: input.Principal,
		Purpose:   input.Purpose,
	})
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	writeAPIJSON(writer, result)
}

type searchRequest struct {
	Text               string   `json:"text"`
	ExactIDs           []string `json:"exact_ids"`
	ContentHash        string   `json:"content_hash"`
	Namespace          string   `json:"namespace"`
	NamespaceScope     []string `json:"namespace_scope"`
	Kind               string   `json:"kind"`
	SubjectEntityID    string   `json:"subject_entity_id"`
	Predicate          string   `json:"predicate"`
	ObjectValue        string   `json:"object_value"`
	ValidAt            string   `json:"valid_at"`
	RecordedBefore     string   `json:"recorded_before"`
	RecordedAfter      string   `json:"recorded_after"`
	IncludeSuperseded  bool     `json:"include_superseded"`
	IncludeInvalidated bool     `json:"include_invalidated"`
	IncludeRedacted    bool     `json:"include_redacted"`
	MinimumScore       float64  `json:"minimum_score"`
	Strict             bool     `json:"strict"`
	Limit              int      `json:"limit"`
	ContextByteLimit   int      `json:"context_byte_limit"`
	Principal          string   `json:"principal"`
	Purpose            string   `json:"purpose"`
}

func (s *Server) search(writer http.ResponseWriter, request *http.Request) {
	var input searchRequest
	if err := decodeAPIJSON(request, &input); err != nil {
		writeAPIError(writer, http.StatusBadRequest, "protocol", err.Error(), "")
		return
	}
	point, before, after, err := parseAPITimes(input.ValidAt, input.RecordedBefore, input.RecordedAfter)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	if input.Principal == "" {
		writeAPIError(writer, http.StatusForbidden, "unauthorized", "principal is required", "principal")
		return
	}
	query := model.RetrievalQuery{
		Text:               input.Text,
		ExactIDs:           input.ExactIDs,
		ContentHash:        input.ContentHash,
		Namespace:          input.Namespace,
		NamespaceScope:     input.NamespaceScope,
		Kind:               input.Kind,
		SubjectEntityID:    input.SubjectEntityID,
		Predicate:          input.Predicate,
		ObjectValue:        input.ObjectValue,
		ValidAt:            point,
		RecordedBefore:     before,
		RecordedAfter:      after,
		IncludeSuperseded:  input.IncludeSuperseded,
		IncludeInvalidated: input.IncludeInvalidated,
		IncludeRedacted:    input.IncludeRedacted,
		MinimumScore:       input.MinimumScore,
		Strict:             input.Strict,
		Limit:              input.Limit,
		ContextByteLimit:   input.ContextByteLimit,
	}
	result, err := s.store.SearchAuthorized(request.Context(), query, model.AccessContext{
		Principal: input.Principal,
		Purpose:   input.Purpose,
	})
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	writeAPIJSON(writer, result)
}

type targetRequest struct {
	TargetID  string `json:"target_id"`
	Principal string `json:"principal"`
	Purpose   string `json:"purpose"`
}

func (s *Server) explain(writer http.ResponseWriter, request *http.Request) {
	var input targetRequest
	if err := decodeAPIJSON(request, &input); err != nil {
		writeAPIError(writer, http.StatusBadRequest, "protocol", err.Error(), "")
		return
	}
	if input.Principal == "" {
		writeAPIError(writer, http.StatusForbidden, "unauthorized", "principal is required", "principal")
		return
	}
	result, err := s.store.ExplainAuthorized(request.Context(), input.TargetID, model.AccessContext{
		Principal: input.Principal,
		Purpose:   input.Purpose,
	})
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	writeAPIJSON(writer, result)
}

type historyRequest struct {
	TargetID  string `json:"target_id"`
	Principal string `json:"principal"`
	Purpose   string `json:"purpose"`
}

func (s *Server) history(writer http.ResponseWriter, request *http.Request) {
	var input historyRequest
	if err := decodeAPIJSON(request, &input); err != nil {
		writeAPIError(writer, http.StatusBadRequest, "protocol", err.Error(), "")
		return
	}
	if input.Principal == "" {
		writeAPIError(writer, http.StatusForbidden, "unauthorized", "principal is required", "principal")
		return
	}
	result, err := s.store.HistoryAuthorized(request.Context(), input.TargetID, model.AccessContext{
		Principal: input.Principal,
		Purpose:   input.Purpose,
	})
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	writeAPIJSON(writer, result)
}

type forgetRequest struct {
	TargetID string `json:"target_id"`
	Mode     string `json:"mode"`
	Actor    string `json:"actor"`
	Purpose  string `json:"purpose"`
}

func (s *Server) forget(writer http.ResponseWriter, request *http.Request) {
	var input forgetRequest
	if err := decodeAPIJSON(request, &input); err != nil {
		writeAPIError(writer, http.StatusBadRequest, "protocol", err.Error(), "")
		return
	}
	result, err := s.store.Forget(request.Context(), model.ForgetRequest{
		TargetID: input.TargetID,
		Mode:     input.Mode,
		Actor:    input.Actor,
		Access: model.AccessContext{
			Principal: input.Actor,
			Purpose:   input.Purpose,
		},
	})
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	writeAPIJSON(writer, result)
}

func (s *Server) event(writer http.ResponseWriter, request *http.Request, id string) {
	access, ok := requestAccess(writer, request)
	if !ok {
		return
	}
	result, err := s.store.GetEventAuthorized(request.Context(), id, access)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	writeAPIJSON(writer, result)
}

func (s *Server) memory(writer http.ResponseWriter, request *http.Request, id string) {
	access, ok := requestAccess(writer, request)
	if !ok {
		return
	}
	result, err := s.store.GetMemoryAuthorized(request.Context(), id, access)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	writeAPIJSON(writer, result)
}

func requestAccess(writer http.ResponseWriter, request *http.Request) (model.AccessContext, bool) {
	principal := request.URL.Query().Get("principal")
	if principal == "" {
		writeAPIError(writer, http.StatusForbidden, "unauthorized", "principal is required", "principal")
		return model.AccessContext{}, false
	}
	return model.AccessContext{
		Principal: principal,
		Purpose:   request.URL.Query().Get("purpose"),
	}, true
}

func writeStoreError(writer http.ResponseWriter, err error) {
	status, category := apiStatus(err)
	writeAPIError(writer, status, category, err.Error(), "")
}

func readBody(request *http.Request) ([]byte, error) {
	defer request.Body.Close()
	return io.ReadAll(request.Body)
}

func parseAPITimes(values ...string) (*time.Time, *time.Time, *time.Time, error) {
	result := make([]*time.Time, len(values))
	for index, value := range values {
		if value == "" {
			continue
		}
		parsed, err := model.ParseTime(value)
		if err != nil {
			return nil, nil, nil, err
		}
		result[index] = &parsed
	}
	return result[0], result[1], result[2], nil
}
