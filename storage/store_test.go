package storage

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"trace/format"
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

func TestPolicyAuthorizationAndDependencyForget(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	allowRead := model.PolicyInput{
		Effect:    model.PolicyAllow,
		Principal: "agent:alice",
		Operation: model.OperationRead,
		Namespace: "user:tim",
		Actor:     "admin",
	}
	if _, err := store.AddPolicy(ctx, allowRead); err != nil {
		t.Fatalf("add read policy: %v", err)
	}
	allowForget := model.PolicyInput{
		Effect:    model.PolicyAllow,
		Principal: "admin",
		Operation: model.OperationForget,
		Namespace: "user:tim",
		Actor:     "admin",
	}
	if _, err := store.AddPolicy(ctx, allowForget); err != nil {
		t.Fatalf("add forget policy: %v", err)
	}

	event, err := store.AddEvent(ctx, model.EventInput{
		EventType: "conversation.message",
		Payload:   json.RawMessage(`{"content":"private source"}`),
		Namespace: "user:tim",
	})
	if err != nil {
		t.Fatalf("add source event: %v", err)
	}
	memory, err := store.AddMemory(ctx, model.MemoryInput{
		Kind:        "fact",
		Content:     "private fact",
		Namespace:   "user:tim",
		DerivedFrom: []string{event.ID},
	})
	if err != nil {
		t.Fatalf("add source memory: %v", err)
	}
	observation, err := store.AddMemory(ctx, model.MemoryInput{
		Kind:        "observation",
		Content:     "private observation",
		Namespace:   "user:tim",
		DerivedFrom: []string{memory.ID},
	})
	if err != nil {
		t.Fatalf("add observation: %v", err)
	}

	if _, err := store.GetMemoryAuthorized(ctx, memory.ID, model.AccessContext{
		Principal: "agent:alice",
	}); err != nil {
		t.Fatalf("authorized memory read: %v", err)
	}
	if _, err := store.GetMemoryAuthorized(ctx, memory.ID, model.AccessContext{
		Principal: "agent:bob",
	}); err == nil {
		t.Fatal("unauthorized memory read was accepted")
	}
	if _, err := store.AddPolicy(ctx, model.PolicyInput{
		Effect:           model.PolicyDeny,
		Principal:        "agent:alice",
		Operation:        model.OperationRead,
		Namespace:        "user:tim",
		ResourceSelector: json.RawMessage(`{"id":"` + memory.ID + `"}`),
		Actor:            "admin",
	}); err != nil {
		t.Fatalf("add deny policy: %v", err)
	}
	if _, err := store.GetMemoryAuthorized(ctx, memory.ID, model.AccessContext{
		Principal: "agent:alice",
	}); err == nil {
		t.Fatal("deny policy did not override allow policy")
	}

	deletions, err := store.Forget(ctx, model.ForgetRequest{
		TargetID: event.ID,
		Actor:    "admin",
		Access: model.AccessContext{
			Principal: "admin",
		},
	})
	if err != nil {
		t.Fatalf("forget dependency closure: %v", err)
	}
	if len(deletions) != 3 {
		t.Fatalf("deletions = %d, want event, memory, and observation", len(deletions))
	}
	if _, err := store.GetEvent(ctx, event.ID); err == nil {
		t.Fatal("forgotten event is still readable")
	}
	if _, err := store.GetMemory(ctx, memory.ID); err == nil {
		t.Fatal("forgotten memory is still readable")
	}
	if _, err := store.GetMemory(ctx, observation.ID); err == nil {
		t.Fatal("forgotten observation is still readable")
	}
	repeated, err := store.Forget(ctx, model.ForgetRequest{
		TargetID: event.ID,
		Actor:    "admin",
		Access: model.AccessContext{
			Principal: "admin",
		},
	})
	if err != nil {
		t.Fatalf("repeat forget: %v", err)
	}
	if len(repeated) != 1 || repeated[0].TargetID != event.ID {
		t.Fatalf("repeat forget result = %#v", repeated)
	}
	report, err := store.Validate(ctx)
	if err != nil {
		t.Fatalf("validate after forget: %v", err)
	}
	if !report.Valid {
		t.Fatalf("validation after forget: %v", report.Errors)
	}
}

func TestRedactionRemovesPayloadAndKeepsRecordIdentity(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if _, err := store.AddPolicy(ctx, model.PolicyInput{
		Effect:    model.PolicyAllow,
		Principal: "admin",
		Operation: model.OperationForget,
		Namespace: "user:tim",
		Actor:     "admin",
	}); err != nil {
		t.Fatalf("add forget policy: %v", err)
	}
	memory, err := store.AddMemory(ctx, model.MemoryInput{
		Kind:      "fact",
		Content:   "private preference",
		Namespace: "user:tim",
	})
	if err != nil {
		t.Fatalf("add memory: %v", err)
	}
	if _, err := store.Forget(ctx, model.ForgetRequest{
		TargetID: memory.ID,
		Mode:     "target_only_redact",
		Actor:    "admin",
		Access:   model.AccessContext{Principal: "admin"},
	}); err != nil {
		t.Fatalf("redact memory: %v", err)
	}
	redacted, err := store.GetMemory(ctx, memory.ID)
	if err != nil {
		t.Fatalf("read redacted memory: %v", err)
	}
	if redacted.Content != "" || redacted.Status != "redacted" {
		t.Fatalf("redacted memory = %#v", redacted)
	}
	result, err := store.Search(ctx, model.RetrievalQuery{
		Text: "private preference",
	})
	if err != nil {
		t.Fatalf("search redacted memory: %v", err)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("redacted search hits = %#v, want none", result.Hits)
	}
}

func TestAtomicBatchIngestion(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	request := format.BatchRequest{
		Protocol:  format.BatchProtocol,
		Version:   format.ProtocolVersion,
		RequestID: "batch-1",
		Actor:     "agent:extractor",
		Events: []format.EventProposal{{
			ClientID:   "event-1",
			EventType:  "conversation.message",
			Payload:    json.RawMessage(`{"content":"I prefer Go"}`),
			Namespace:  "user:tim",
			RecordedAt: "2026-09-15T19:00:00Z",
		}},
		Memories: []format.MemoryProposal{{
			ClientID:    "memory-1",
			Kind:        "fact",
			Content:     "Tim prefers Go",
			Namespace:   "user:tim",
			RecordedAt:  "2026-09-15T19:00:00Z",
			DerivedFrom: []string{"event-1"},
		}},
	}
	result, err := store.IngestBatch(ctx, request)
	if err != nil {
		t.Fatalf("ingest batch: %v", err)
	}
	if len(result.Errors) != 0 || result.CommitID == "" {
		t.Fatalf("batch result = %#v, want committed batch", result)
	}
	if len(result.Accepted) != 2 || result.IDMap["event-1"] == "" || result.IDMap["memory-1"] == "" {
		t.Fatalf("batch IDs = %#v, want event and memory", result)
	}
	history, err := store.History(ctx, "")
	if err != nil {
		t.Fatalf("batch history: %v", err)
	}
	if len(history) != 2 || history[0].CommitID != result.CommitID || history[1].CommitID != result.CommitID {
		t.Fatalf("batch history = %#v, want shared commit ID", history)
	}

	inspection, err := store.Inspect(ctx)
	if err != nil {
		t.Fatalf("inspect before rejected batch: %v", err)
	}
	rejected, err := store.IngestBatch(ctx, format.BatchRequest{
		Protocol:  format.BatchProtocol,
		Version:   format.ProtocolVersion,
		RequestID: "batch-invalid",
		Actor:     "agent:extractor",
		Memories: []format.MemoryProposal{{
			ClientID:    "invalid-memory",
			Kind:        "fact",
			Content:     "must not commit",
			Namespace:   "user:tim",
			DerivedFrom: []string{"missing-source"},
		}},
	})
	if err != nil {
		t.Fatalf("rejected batch: %v", err)
	}
	if len(rejected.Errors) != 1 || rejected.CommitID != "" {
		t.Fatalf("rejected result = %#v, want one error and no commit", rejected)
	}
	after, err := store.Inspect(ctx)
	if err != nil {
		t.Fatalf("inspect after rejected batch: %v", err)
	}
	if after.Counts["memory"] != inspection.Counts["memory"] {
		t.Fatalf("memory count after rejected batch = %d, want %d", after.Counts["memory"], inspection.Counts["memory"])
	}
}

type testEmbeddingProvider struct{}

func (testEmbeddingProvider) Spec() model.EmbeddingSpec {
	return model.EmbeddingSpec{
		Model:          "test-embedding",
		Dimensions:     2,
		Revision:       "1",
		AdapterVersion: "test",
	}
}

func (testEmbeddingProvider) Embed(_ context.Context, texts []string) ([][]float32, error) {
	vectors := make([][]float32, len(texts))
	for index, text := range texts {
		if strings.Contains(strings.ToLower(text), "python") {
			vectors[index] = []float32{0, 1}
		} else {
			vectors[index] = []float32{1, 0}
		}
	}
	return vectors, nil
}

func TestSemanticIndexRelationshipsAndEntityLookup(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	first, err := store.AddMemory(ctx, model.MemoryInput{
		Kind:      "fact",
		Content:   "Tim prefers Go",
		Namespace: "user:tim",
	})
	if err != nil {
		t.Fatalf("add Go memory: %v", err)
	}
	second, err := store.AddMemory(ctx, model.MemoryInput{
		Kind:      "fact",
		Content:   "Tim uses Python",
		Namespace: "user:tim",
	})
	if err != nil {
		t.Fatalf("add Python memory: %v", err)
	}
	entity, err := store.AddEntity(ctx, model.EntityInput{
		EntityType:    "person",
		CanonicalName: "Tim",
		Aliases:       json.RawMessage(`["Timothy"]`),
		Namespace:     "user:tim",
	})
	if err != nil {
		t.Fatalf("add entity: %v", err)
	}
	relation, err := store.AddRelation(ctx, model.RelationInput{
		SourceID: first.ID,
		TargetID: second.ID,
		Relation: model.RelationSupports,
	})
	if err != nil {
		t.Fatalf("add relation: %v", err)
	}
	if err := store.RebuildSemanticIndex(ctx, testEmbeddingProvider{}); err != nil {
		t.Fatalf("rebuild semantic index: %v", err)
	}
	status, err := store.SemanticIndexStatus(ctx)
	if err != nil {
		t.Fatalf("semantic status: %v", err)
	}
	if !status.Ready || status.RecordCount != 2 {
		t.Fatalf("semantic status = %#v, want ready with two records", status)
	}
	result, err := store.SearchHybrid(ctx, model.HybridQuery{
		RetrievalQuery: model.RetrievalQuery{
			Text:      "prefers Go",
			Namespace: "user:tim",
		},
	}, testEmbeddingProvider{})
	if err != nil {
		t.Fatalf("hybrid search: %v", err)
	}
	if len(result.Hits) != 2 || result.Hits[0].ID != first.ID {
		t.Fatalf("hybrid hits = %#v, want Go first and Python second", result.Hits)
	}
	if result.Hits[0].Score.SemanticMatch == 0 {
		t.Fatalf("hybrid score = %#v, want semantic component", result.Hits[0].Score)
	}
	entities, err := store.LookupEntities(ctx, model.EntityQuery{Text: "Timothy"})
	if err != nil {
		t.Fatalf("lookup entity alias: %v", err)
	}
	if len(entities) != 1 || entities[0].ID != entity.ID {
		t.Fatalf("entity alias results = %#v, want Tim", entities)
	}
	traversal, err := store.Traverse(ctx, model.RelationshipQuery{
		StartIDs:  []string{first.ID},
		Relations: []string{model.RelationSupports},
		MaxHops:   1,
	})
	if err != nil {
		t.Fatalf("traverse relationship: %v", err)
	}
	if len(traversal.Records) != 1 || traversal.Records[0].ID != second.ID ||
		len(traversal.Edges) != 1 || traversal.Edges[0].Derivation.ID != relation.ID {
		t.Fatalf("traversal = %#v, want one support edge", traversal)
	}
	if _, err := store.AddMemory(ctx, model.MemoryInput{
		Kind:      "fact",
		Content:   "new memory",
		Namespace: "user:tim",
	}); err != nil {
		t.Fatalf("add stale memory: %v", err)
	}
	status, err = store.SemanticIndexStatus(ctx)
	if err != nil {
		t.Fatalf("stale semantic status: %v", err)
	}
	if !status.Stale || status.Ready {
		t.Fatalf("stale semantic status = %#v, want stale", status)
	}
}

func TestBundleExportImportRoundTripAndIdempotency(t *testing.T) {
	ctx := context.Background()
	source := newTestStore(t)
	event, err := source.AddEvent(ctx, model.EventInput{
		EventType: "conversation.message",
		Payload:   json.RawMessage(`{"content":"bundle source"}`),
		Namespace: "user:tim",
	})
	if err != nil {
		t.Fatalf("add event: %v", err)
	}
	if _, err := source.AddMemory(ctx, model.MemoryInput{
		Kind:        "fact",
		Content:     "bundle fact",
		Namespace:   "user:tim",
		DerivedFrom: []string{event.ID},
	}); err != nil {
		t.Fatalf("add memory: %v", err)
	}
	bundlePath := filepath.Join(t.TempDir(), "trace-bundle")
	manifest, err := source.ExportBundle(ctx, bundlePath)
	if err != nil {
		t.Fatalf("export bundle: %v", err)
	}
	if manifest.BundleID == "" || manifest.Counts["event"] != 1 || manifest.Counts["memory"] != 1 {
		t.Fatalf("bundle manifest = %#v", manifest)
	}
	targetPath := filepath.Join(t.TempDir(), "imported.trc")
	result, err := ImportBundle(ctx, bundlePath, targetPath, "new")
	if err != nil {
		t.Fatalf("import bundle: %v", err)
	}
	if result.Idempotent || result.IDMap[event.ID] != event.ID {
		t.Fatalf("import result = %#v, want non-idempotent identity-preserving import", result)
	}
	target, err := Open(ctx, targetPath, false)
	if err != nil {
		t.Fatalf("open imported target: %v", err)
	}
	defer target.Close()
	inspection, err := target.Inspect(ctx)
	if err != nil {
		t.Fatalf("inspect imported target: %v", err)
	}
	if inspection.Counts["event"] != 1 || inspection.Counts["memory"] != 1 {
		t.Fatalf("imported counts = %#v, want one event and memory", inspection.Counts)
	}
	report, err := target.Validate(ctx)
	if err != nil {
		t.Fatalf("validate imported target: %v", err)
	}
	if !report.Valid {
		t.Fatalf("imported target validation errors: %v", report.Errors)
	}
	repeated, err := ImportBundle(ctx, bundlePath, targetPath, "merge")
	if err != nil {
		t.Fatalf("repeat bundle import: %v", err)
	}
	if !repeated.Idempotent || repeated.BundleID != manifest.BundleID {
		t.Fatalf("repeat import = %#v, want idempotent result", repeated)
	}
}

func TestBundleChecksumFailureDoesNotCreateTarget(t *testing.T) {
	ctx := context.Background()
	source := newTestStore(t)
	if _, err := source.AddMemory(ctx, model.MemoryInput{
		Kind:      "fact",
		Content:   "checksum source",
		Namespace: "user:tim",
	}); err != nil {
		t.Fatalf("add memory: %v", err)
	}
	bundlePath := filepath.Join(t.TempDir(), "trace-bundle")
	if _, err := source.ExportBundle(ctx, bundlePath); err != nil {
		t.Fatalf("export bundle: %v", err)
	}
	eventsPath := filepath.Join(bundlePath, "events.jsonl")
	events, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatalf("read events bundle: %v", err)
	}
	if err := os.WriteFile(eventsPath, append(events, []byte(`{"tampered":true}`+"\n")...), 0o644); err != nil {
		t.Fatalf("tamper events bundle: %v", err)
	}
	targetPath := filepath.Join(t.TempDir(), "target.trc")
	if _, err := ImportBundle(ctx, bundlePath, targetPath, "new"); err == nil {
		t.Fatal("checksum failure was accepted")
	}
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		t.Fatalf("checksum failure created target: %v", err)
	}
}

func TestBundleMergeRemapsConflictingRecordIDs(t *testing.T) {
	ctx := context.Background()
	sourcePath := filepath.Join(t.TempDir(), "source.trc")
	if err := Init(ctx, sourcePath, "bundle-test"); err != nil {
		t.Fatalf("init source: %v", err)
	}
	source, err := Open(ctx, sourcePath, false)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer source.Close()
	const recordID = "bundle-conflict-event"
	if _, err := source.AddEvent(ctx, model.EventInput{
		ID:        recordID,
		EventType: "source",
		Payload:   json.RawMessage(`{"value":"source"}`),
		Namespace: "user:tim",
	}); err != nil {
		t.Fatalf("add source event: %v", err)
	}
	bundlePath := filepath.Join(t.TempDir(), "trace-bundle")
	if _, err := source.ExportBundle(ctx, bundlePath); err != nil {
		t.Fatalf("export source bundle: %v", err)
	}
	targetPath := filepath.Join(t.TempDir(), "target.trc")
	if err := Init(ctx, targetPath, "bundle-test"); err != nil {
		t.Fatalf("init target: %v", err)
	}
	target, err := Open(ctx, targetPath, false)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	if _, err := target.AddEvent(ctx, model.EventInput{
		ID:        recordID,
		EventType: "target",
		Payload:   json.RawMessage(`{"value":"target"}`),
		Namespace: "user:tim",
	}); err != nil {
		target.Close()
		t.Fatalf("add target event: %v", err)
	}
	target.Close()
	result, err := ImportBundle(ctx, bundlePath, targetPath, "merge")
	if err != nil {
		t.Fatalf("merge conflicting bundle: %v", err)
	}
	if result.IDMap[recordID] == recordID || result.IDMap[recordID] == "" {
		t.Fatalf("merge ID map = %#v, want remapped event", result.IDMap)
	}
	merged, err := Open(ctx, targetPath, true)
	if err != nil {
		t.Fatalf("open merged target: %v", err)
	}
	defer merged.Close()
	inspection, err := merged.Inspect(ctx)
	if err != nil {
		t.Fatalf("inspect merged target: %v", err)
	}
	if inspection.Counts["event"] != 2 {
		t.Fatalf("merged event count = %d, want 2", inspection.Counts["event"])
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
