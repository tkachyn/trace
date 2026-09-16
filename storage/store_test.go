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

func TestTemporalQueriesHistoryAndExplanation(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	january := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	june := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	july := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	earlyRecorded := january.Add(24 * time.Hour)
	lateRecorded := june.Add(24 * time.Hour)

	torontoEvent, err := store.AddEvent(ctx, model.EventInput{
		EventType:  "conversation.message",
		Payload:    json.RawMessage(`{"content":"I live in Toronto"}`),
		RecordedAt: earlyRecorded,
		Namespace:  "user:tim",
		Provenance: model.Provenance{SourceType: "test", SourceID: "message-toronto"},
	})
	if err != nil {
		t.Fatalf("add Toronto event: %v", err)
	}
	torontoMemory, err := store.AddMemory(ctx, model.MemoryInput{
		Kind:        "fact",
		Content:     "Tim lives in Toronto",
		RecordedAt:  earlyRecorded,
		ValidFrom:   &january,
		ValidTo:     &june,
		Namespace:   "user:tim",
		DerivedFrom: []string{torontoEvent.ID},
		Provenance:  model.Provenance{SourceType: "test", SourceID: "message-toronto"},
	})
	if err != nil {
		t.Fatalf("add Toronto memory: %v", err)
	}
	beforeVancouver, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot before Vancouver: %v", err)
	}

	vancouverEvent, err := store.AddEvent(ctx, model.EventInput{
		EventType:  "conversation.message",
		Payload:    json.RawMessage(`{"content":"I moved to Vancouver"}`),
		RecordedAt: lateRecorded,
		Namespace:  "user:tim",
		Provenance: model.Provenance{SourceType: "test", SourceID: "message-vancouver"},
	})
	if err != nil {
		t.Fatalf("add Vancouver event: %v", err)
	}
	vancouverMemory, err := store.AddMemory(ctx, model.MemoryInput{
		Kind:        "fact",
		Content:     "Tim lives in Vancouver",
		RecordedAt:  lateRecorded,
		ValidFrom:   &june,
		Namespace:   "user:tim",
		DerivedFrom: []string{vancouverEvent.ID},
		Provenance:  model.Provenance{SourceType: "test", SourceID: "message-vancouver"},
	})
	if err != nil {
		t.Fatalf("add Vancouver memory: %v", err)
	}
	supersession, err := store.AddRelation(ctx, model.RelationInput{
		SourceID: vancouverMemory.ID,
		TargetID: torontoMemory.ID,
		Relation: model.RelationSupersedes,
		Actor:    "test",
	})
	if err != nil {
		t.Fatalf("add supersession: %v", err)
	}
	afterVancouver, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot after Vancouver: %v", err)
	}

	past, err := store.QueryMemories(ctx, model.MemoryQuery{
		Namespace: "user:tim",
		ValidAt:   timePointer(january.AddDate(0, 2, 0)),
	})
	if err != nil {
		t.Fatalf("query Toronto point in time: %v", err)
	}
	if len(past.Memories) != 1 || past.Memories[0].ID != torontoMemory.ID {
		t.Fatalf("past memories = %#v, want Toronto", past.Memories)
	}

	present, err := store.QueryMemories(ctx, model.MemoryQuery{
		Namespace: "user:tim",
		ValidAt:   &july,
	})
	if err != nil {
		t.Fatalf("query Vancouver point in time: %v", err)
	}
	if len(present.Memories) != 1 || present.Memories[0].ID != vancouverMemory.ID {
		t.Fatalf("present memories = %#v, want Vancouver", present.Memories)
	}
	recorded, err := store.QueryMemories(ctx, model.MemoryQuery{
		Namespace:      "user:tim",
		ValidAt:        timePointer(january.AddDate(0, 2, 0)),
		RecordedBefore: timePointer(lateRecorded),
	})
	if err != nil {
		t.Fatalf("query recorded-time boundary: %v", err)
	}
	if len(recorded.Memories) != 1 || recorded.Memories[0].ID != torontoMemory.ID {
		t.Fatalf("recorded-time memories = %#v, want Toronto", recorded.Memories)
	}

	history, err := store.History(ctx, "")
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	if len(history) != 5 {
		t.Fatalf("history length = %d, want 5", len(history))
	}
	for index := 1; index < len(history); index++ {
		if history[index-1].Sequence >= history[index].Sequence {
			t.Fatalf("history sequence is not increasing: %#v", history)
		}
	}

	diff, err := store.Diff(ctx, beforeVancouver, afterVancouver)
	if err != nil {
		t.Fatalf("diff snapshots: %v", err)
	}
	if len(diff.Added) != 2 {
		t.Fatalf("diff additions = %#v, want event and memory", diff.Added)
	}
	if diff.Mutations[len(diff.Mutations)-1].Operation != "relate" {
		t.Fatalf("last diff operation = %q, want relate", diff.Mutations[len(diff.Mutations)-1].Operation)
	}
	if diff.Mutations[len(diff.Mutations)-1].TargetID != supersession.ID {
		t.Fatalf("last diff target = %q, want %q", diff.Mutations[len(diff.Mutations)-1].TargetID, supersession.ID)
	}

	explanation, err := store.Explain(ctx, vancouverMemory.ID)
	if err != nil {
		t.Fatalf("explain Vancouver memory: %v", err)
	}
	if len(explanation.SourceEvents) != 1 || explanation.SourceEvents[0].ID != vancouverEvent.ID {
		t.Fatalf("explanation source events = %#v, want Vancouver event", explanation.SourceEvents)
	}
	if len(explanation.Derivations) != 2 {
		t.Fatalf("explanation derivations = %#v, want source and supersession", explanation.Derivations)
	}
	if len(explanation.Provenance) != 2 {
		t.Fatalf("explanation provenance = %#v, want memory and event provenance", explanation.Provenance)
	}
	for _, provenance := range explanation.Provenance {
		if provenance.SourceID != "message-vancouver" {
			t.Fatalf("explanation provenance source = %q, want message-vancouver", provenance.SourceID)
		}
	}
	if len(explanation.Mutations) != 2 {
		t.Fatalf("explanation mutations = %#v, want memory and supersession", explanation.Mutations)
	}
}

func TestContradictionsRemainVisible(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	first, err := store.AddMemory(ctx, model.MemoryInput{
		Kind:       "fact",
		Content:    "Tim prefers Python",
		RecordedAt: start,
		ValidFrom:  &start,
		Namespace:  "user:tim",
		Provenance: model.Provenance{SourceType: "test", SourceID: "first"},
	})
	if err != nil {
		t.Fatalf("add first fact: %v", err)
	}
	second, err := store.AddMemory(ctx, model.MemoryInput{
		Kind:       "fact",
		Content:    "Tim prefers Go",
		RecordedAt: start.Add(time.Hour),
		ValidFrom:  &start,
		Namespace:  "user:tim",
		Provenance: model.Provenance{SourceType: "test", SourceID: "second"},
	})
	if err != nil {
		t.Fatalf("add second fact: %v", err)
	}
	if _, err := store.AddRelation(ctx, model.RelationInput{
		SourceID: second.ID,
		TargetID: first.ID,
		Relation: model.RelationContradicts,
		Actor:    "test",
	}); err != nil {
		t.Fatalf("add contradiction: %v", err)
	}

	result, err := store.QueryMemories(ctx, model.MemoryQuery{
		Namespace: "user:tim",
		ValidAt:   timePointer(start.Add(time.Hour)),
	})
	if err != nil {
		t.Fatalf("query contradiction: %v", err)
	}
	if result.EvidenceState != "conflicting" {
		t.Fatalf("evidence state = %q, want conflicting", result.EvidenceState)
	}
	if len(result.Memories) != 2 || len(result.Conflicts) != 1 {
		t.Fatalf("conflict result = %#v, want two memories and one group", result)
	}
	if result.Conflicts[0].Resolved {
		t.Fatal("unresolved contradiction was marked resolved")
	}
}

func TestDeterministicRetrievalAndContextCompilation(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	event, err := store.AddEvent(ctx, model.EventInput{
		EventType:  "conversation.message",
		Payload:    json.RawMessage(`{"content":"Tim prefers Go"}`),
		RecordedAt: start,
		Namespace:  "user:tim",
	})
	if err != nil {
		t.Fatalf("add event: %v", err)
	}
	goMemory, err := store.AddMemory(ctx, model.MemoryInput{
		Kind:        "fact",
		Content:     "Tim prefers Go",
		RecordedAt:  start,
		ValidFrom:   &start,
		Namespace:   "user:tim",
		Predicate:   "prefers",
		ObjectValue: "Go",
		DerivedFrom: []string{event.ID},
	})
	if err != nil {
		t.Fatalf("add Go memory: %v", err)
	}
	pythonMemory, err := store.AddMemory(ctx, model.MemoryInput{
		Kind:        "fact",
		Content:     "Tim prefers Python",
		RecordedAt:  start.Add(time.Hour),
		ValidFrom:   &start,
		Namespace:   "user:tim",
		Predicate:   "prefers",
		ObjectValue: "Python",
	})
	if err != nil {
		t.Fatalf("add Python memory: %v", err)
	}
	if _, err := store.AddRelation(ctx, model.RelationInput{
		SourceID: goMemory.ID,
		TargetID: pythonMemory.ID,
		Relation: model.RelationContradicts,
	}); err != nil {
		t.Fatalf("add contradiction: %v", err)
	}

	result, err := store.Search(ctx, model.RetrievalQuery{
		Text:             "prefers Go",
		NamespaceScope:   []string{"user:tim"},
		ValidAt:          timePointer(start.Add(time.Hour)),
		ContextByteLimit: 200,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if result.EvidenceState != "supported" {
		t.Fatalf("evidence state = %q, want supported", result.EvidenceState)
	}
	if len(result.Hits) != 1 || result.Hits[0].ID != goMemory.ID {
		t.Fatalf("retrieval hits = %#v, want Go memory", result.Hits)
	}
	if result.Context.HitIDs[0] != goMemory.ID || result.Context.Bytes == 0 {
		t.Fatalf("compiled context = %#v, want Go memory", result.Context)
	}

	exact, err := store.Search(ctx, model.RetrievalQuery{
		ExactIDs: []string{pythonMemory.ID},
		Kind:     "memory",
	})
	if err != nil {
		t.Fatalf("exact search: %v", err)
	}
	if len(exact.Hits) != 1 || exact.Hits[0].ID != pythonMemory.ID {
		t.Fatalf("exact hits = %#v, want Python memory", exact.Hits)
	}
	if exact.Hits[0].Score.ExactMatch != 1 {
		t.Fatalf("exact score = %#v, want exact match", exact.Hits[0].Score)
	}

	conflicting, err := store.Search(ctx, model.RetrievalQuery{
		Namespace:         "user:tim",
		ValidAt:           timePointer(start.Add(time.Hour)),
		IncludeSuperseded: true,
	})
	if err != nil {
		t.Fatalf("conflict search: %v", err)
	}
	if conflicting.EvidenceState != "conflicting" {
		t.Fatalf("conflict evidence state = %q, want conflicting", conflicting.EvidenceState)
	}
	strict, err := store.Search(ctx, model.RetrievalQuery{
		Namespace: "user:tim",
		ValidAt:   timePointer(start.Add(time.Hour)),
		Strict:    true,
	})
	if err != nil {
		t.Fatalf("strict conflict search: %v", err)
	}
	if strict.Context.Content != "" {
		t.Fatalf("strict conflict context = %q, want empty", strict.Context.Content)
	}

	before := result.Context.Content
	if err := store.RebuildRetrievalIndex(ctx); err != nil {
		t.Fatalf("rebuild retrieval index: %v", err)
	}
	after, err := store.Search(ctx, model.RetrievalQuery{
		Text:           "prefers Go",
		NamespaceScope: []string{"user:tim"},
		ValidAt:        timePointer(start.Add(time.Hour)),
	})
	if err != nil {
		t.Fatalf("search after rebuild: %v", err)
	}
	if len(after.Hits) != 1 || after.Hits[0].ID != goMemory.ID {
		t.Fatalf("rebuilt retrieval hits = %#v, want Go memory", after.Hits)
	}
	if before != after.Context.Content {
		t.Fatalf("context after rebuild = %q, want %q", after.Context.Content, before)
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "memory.trc")
	if err := Init(ctx, path, "trace-test"); err != nil {
		t.Fatalf("init: %v", err)
	}
	store, err := Open(ctx, path, false)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		store.Close()
	})
	return store
}

func timePointer(value time.Time) *time.Time {
	return &value
}
