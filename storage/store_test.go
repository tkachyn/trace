package storage

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"trace/model"
)

func TestInitWriteInspectAndValidate(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "memory.trc")
	if err := Init(ctx, path, "trace-test"); err != nil {
		t.Fatalf("init: %v", err)
	}

	store, err := Open(ctx, path, false)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	event, err := store.AddEvent(ctx, model.EventInput{
		EventType: "conversation.message",
		Payload:   json.RawMessage(`{"role":"user","content":"I prefer Go"}`),
		Namespace: "user:tim",
		Provenance: model.Provenance{
			SourceType: "test",
			SourceID:   "message-1",
		},
	})
	if err != nil {
		t.Fatalf("add event: %v", err)
	}

	memory, err := store.AddMemory(ctx, model.MemoryInput{
		Kind:      "fact",
		Content:   "Tim prefers Go",
		Namespace: "user:tim",
		RecordedAt: time.Date(
			2026, 9, 15, 19, 0, 0, 0, time.UTC,
		),
		DerivedFrom: []string{event.ID},
		Provenance: model.Provenance{
			SourceType: "test",
			SourceID:   "message-1",
		},
	})
	if err != nil {
		t.Fatalf("add memory: %v", err)
	}
	if memory.ContentHash == "" {
		t.Fatal("memory content hash is empty")
	}
	readEvent, err := store.GetEvent(ctx, event.ID)
	if err != nil {
		t.Fatalf("get event: %v", err)
	}
	if readEvent.ContentHash != event.ContentHash {
		t.Fatalf("event hash = %q, want %q", readEvent.ContentHash, event.ContentHash)
	}
	readMemory, err := store.GetMemory(ctx, memory.ID)
	if err != nil {
		t.Fatalf("get memory: %v", err)
	}
	if readMemory.Content != memory.Content {
		t.Fatalf("memory content = %q, want %q", readMemory.Content, memory.Content)
	}

	inspection, err := store.Inspect(ctx)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if inspection.Manifest.FormatName != model.FormatName {
		t.Fatalf("format name = %q, want %q", inspection.Manifest.FormatName, model.FormatName)
	}
	if inspection.Counts["event"] != 1 {
		t.Fatalf("event count = %d, want 1", inspection.Counts["event"])
	}
	if inspection.Counts["memory"] != 1 {
		t.Fatalf("memory count = %d, want 1", inspection.Counts["memory"])
	}
	if inspection.Counts["derivation"] != 1 {
		t.Fatalf("derivation count = %d, want 1", inspection.Counts["derivation"])
	}

	report, err := store.Validate(ctx)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if !report.Valid {
		t.Fatalf("validation errors: %v", report.Errors)
	}
}

func TestInvalidMemoryIntervalIsRejected(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "memory.trc")
	if err := Init(ctx, path, "trace-test"); err != nil {
		t.Fatalf("init: %v", err)
	}
	store, err := Open(ctx, path, false)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	from := time.Date(2026, 9, 15, 20, 0, 0, 0, time.UTC)
	to := from.Add(-time.Hour)
	_, err = store.AddMemory(ctx, model.MemoryInput{
		Kind:      "fact",
		Content:   "invalid interval",
		Namespace: "user:tim",
		ValidFrom: &from,
		ValidTo:   &to,
	})
	if err == nil {
		t.Fatal("invalid interval was accepted")
	}
}

func TestValidationDetectsTamperedEvent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "memory.trc")
	if err := Init(ctx, path, "trace-test"); err != nil {
		t.Fatalf("init: %v", err)
	}
	store, err := Open(ctx, path, false)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	event, err := store.AddEvent(ctx, model.EventInput{
		EventType: "tool.observation",
		Payload:   json.RawMessage(`{"value":1}`),
		Namespace: "app:test",
	})
	if err != nil {
		t.Fatalf("add event: %v", err)
	}
	if _, err := store.db.ExecContext(
		ctx,
		"UPDATE event SET payload = ? WHERE id = ?",
		`{"value":2}`,
		event.ID,
	); err != nil {
		t.Fatalf("tamper event: %v", err)
	}

	report, err := store.Validate(ctx)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if report.Valid {
		t.Fatal("tampered event passed validation")
	}
}
