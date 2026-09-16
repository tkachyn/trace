package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"trace/model"
	"trace/storage"
)

func TestVersionedAPIInspectIngestAndAuthorizedSearch(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "memory.trc")
	if err := storage.Init(ctx, path, "api-test"); err != nil {
		t.Fatalf("init: %v", err)
	}
	store, err := storage.Open(ctx, path, false)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if _, err := store.AddPolicy(ctx, model.PolicyInput{
		Effect:    model.PolicyAllow,
		Principal: "agent:test",
		Operation: model.OperationRead,
		Namespace: "user:tim",
		Actor:     "admin",
	}); err != nil {
		t.Fatalf("add read policy: %v", err)
	}
	if _, err := store.AddPolicy(ctx, model.PolicyInput{
		Effect:    model.PolicyAllow,
		Principal: "agent:test",
		Operation: model.OperationSearch,
		Namespace: "user:tim",
		Actor:     "admin",
	}); err != nil {
		t.Fatalf("add search policy: %v", err)
	}
	server := httptest.NewServer(NewServer(store))
	defer server.Close()

	response, err := http.Get(server.URL + "/v1/inspect")
	if err != nil {
		t.Fatalf("inspect request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("inspect status = %d, want 200", response.StatusCode)
	}

	payload := []byte(`{
		"protocol":"trace.batch",
		"version":1,
		"request_id":"api-batch-1",
		"actor":"agent:test",
		"events":[{
			"client_id":"event-1",
			"event_type":"conversation.message",
			"payload":{"content":"I prefer Go"},
			"namespace":"user:tim",
			"recorded_at":"2026-09-15T19:00:00Z"
		}],
		"memories":[{
			"client_id":"memory-1",
			"kind":"fact",
			"content":"Tim prefers Go",
			"namespace":"user:tim",
			"valid_from":"2026-01-01T00:00:00Z",
			"recorded_at":"2026-09-15T19:00:00Z",
			"derived_from":["event-1"]
		}]
	}`)
	response, err = http.Post(
		server.URL+"/v1/ingest",
		"application/json",
		bytes.NewReader(payload),
	)
	if err != nil {
		t.Fatalf("ingest request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("ingest status = %d, want 200", response.StatusCode)
	}
	var batch map[string]any
	if err := json.NewDecoder(response.Body).Decode(&batch); err != nil {
		t.Fatalf("decode ingest response: %v", err)
	}
	if batch["commit_id"] == "" {
		t.Fatalf("ingest response = %#v, want commit ID", batch)
	}

	searchPayload := []byte(`{
		"text":"prefers Go",
		"principal":"agent:test",
		"namespace":"user:tim",
		"valid_at":"2026-02-01T00:00:00Z"
	}`)
	response, err = http.Post(
		server.URL+"/v1/search",
		"application/json",
		bytes.NewReader(searchPayload),
	)
	if err != nil {
		t.Fatalf("search request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("search status = %d, want 200", response.StatusCode)
	}
	var search map[string]any
	if err := json.NewDecoder(response.Body).Decode(&search); err != nil {
		t.Fatalf("decode search response: %v", err)
	}
	if search["EvidenceState"] != "supported" {
		t.Fatalf("search response = %#v, want supported", search)
	}
}

func TestVersionedAPIRequiresPrincipal(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "memory.trc")
	if err := storage.Init(ctx, path, "api-test"); err != nil {
		t.Fatalf("init: %v", err)
	}
	store, err := storage.Open(ctx, path, false)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	handler := NewServer(store)
	request := httptest.NewRequest(http.MethodPost, "/v1/search", bytes.NewBufferString(`{}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("missing principal status = %d, want 403", response.Code)
	}
	var value apiErrorResponse
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		t.Fatalf("decode API error: %v", err)
	}
	if value.Error.Category != "unauthorized" {
		t.Fatalf("API error = %#v, want unauthorized", value)
	}
}

func TestVersionedAPIDirectMemoryRoundTrip(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "memory.trc")
	if err := storage.Init(ctx, path, "api-test"); err != nil {
		t.Fatalf("init: %v", err)
	}
	store, err := storage.Open(ctx, path, false)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if _, err := store.AddPolicy(ctx, model.PolicyInput{
		Effect:    model.PolicyAllow,
		Principal: "agent:test",
		Operation: model.OperationRead,
		Namespace: "user:tim",
		Actor:     "admin",
	}); err != nil {
		t.Fatalf("add policy: %v", err)
	}
	memory, err := store.AddMemory(ctx, model.MemoryInput{
		Kind:       "fact",
		Content:    "round trip",
		Namespace:  "user:tim",
		RecordedAt: time.Date(2026, 9, 15, 19, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("add memory: %v", err)
	}
	handler := NewServer(store)
	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/memories/"+memory.ID+"?principal=agent%3Atest",
		nil,
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("memory status = %d, want 200", response.Code)
	}
	var value model.Memory
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		t.Fatalf("decode memory: %v", err)
	}
	if value.ID != memory.ID || value.ContentHash != memory.ContentHash {
		t.Fatalf("memory = %#v, want %#v", value, memory)
	}
}
