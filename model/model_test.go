package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewIDHasUUIDv7Shape(t *testing.T) {
	id, err := NewID()
	if err != nil {
		t.Fatalf("new ID: %v", err)
	}
	if len(id) != 36 {
		t.Fatalf("ID length = %d, want 36", len(id))
	}
	if id[14] != '7' {
		t.Fatalf("ID version = %q, want 7", id[14])
	}
}

func TestHashEventIsStable(t *testing.T) {
	recordedAt := time.Date(2026, 9, 15, 19, 0, 0, 0, time.UTC)
	event := Event{
		EventType:          "conversation.message",
		Payload:            json.RawMessage(`{"content":"hello"}`),
		PayloadContentType: "application/json",
		RecordedAt:         recordedAt,
		Namespace:          "user:tim",
		ProvenanceID:       "prov-1",
		Extensions:         json.RawMessage(`{}`),
	}
	first, err := HashEvent(event)
	if err != nil {
		t.Fatalf("first hash: %v", err)
	}
	second, err := HashEvent(event)
	if err != nil {
		t.Fatalf("second hash: %v", err)
	}
	if first != second {
		t.Fatalf("hashes differ: %s != %s", first, second)
	}
}

func TestValidateInterval(t *testing.T) {
	from := time.Date(2026, 9, 15, 19, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	if err := ValidateInterval(&from, &to); err != nil {
		t.Fatalf("valid interval rejected: %v", err)
	}
	if err := ValidateInterval(&to, &from); err == nil {
		t.Fatal("reversed interval accepted")
	}
}

func TestCanonicalJSONNormalizesObjectFormatting(t *testing.T) {
	first, err := CanonicalJSON(json.RawMessage(`{ "b": 2, "a": 1 }`), nil)
	if err != nil {
		t.Fatalf("canonicalize first value: %v", err)
	}
	second, err := CanonicalJSON(json.RawMessage(`{"a":1,"b":2}`), nil)
	if err != nil {
		t.Fatalf("canonicalize second value: %v", err)
	}
	if string(first) != string(second) {
		t.Fatalf("canonical values differ: %s != %s", first, second)
	}
}
