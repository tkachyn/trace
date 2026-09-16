// main exposes the initial Trace command line interface
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"trace/model"
	"trace/storage"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "trace:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: trace <init|inspect|validate|add-event|remember|query|history|explain|diff> ...")
	}

	switch args[0] {
	case "init":
		return runInit(ctx, args[1:])
	case "inspect":
		return runInspect(ctx, args[1:])
	case "validate":
		return runValidate(ctx, args[1:])
	case "add-event":
		return runAddEvent(ctx, args[1:])
	case "remember":
		return runRemember(ctx, args[1:])
	case "query":
		return runQuery(ctx, args[1:])
	case "history":
		return runHistory(ctx, args[1:])
	case "explain":
		return runExplain(ctx, args[1:])
	case "diff":
		return runDiff(ctx, args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runInit(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return errors.New("usage: trace init FILE")
	}
	return storage.Init(ctx, args[0], model.DefaultGenerator)
}

func runInspect(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	jsonOutput := fs.Bool("json", false, "emit machine-readable JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: trace inspect [--json] FILE")
	}

	store, err := storage.Open(ctx, fs.Arg(0), true)
	if err != nil {
		return err
	}
	defer store.Close()
	inspection, err := store.Inspect(ctx)
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(inspection)
	}
	fmt.Printf("format: %s %s\n", inspection.Manifest.FormatName, inspection.Manifest.FormatVersion)
	fmt.Printf("schema: %d\n", inspection.Manifest.SchemaVersion)
	fmt.Printf("file id: %s\n", inspection.Manifest.FileID)
	fmt.Printf("generator: %s\n", inspection.Manifest.Generator)
	fmt.Printf("created: %s\n", inspection.Manifest.CreatedAt.Format(time.RFC3339Nano))
	fmt.Printf("updated: %s\n", inspection.Manifest.UpdatedAt.Format(time.RFC3339Nano))
	for _, name := range []string{
		"event",
		"memory",
		"entity",
		"provenance",
		"derivation",
		"mutation",
		"relationship",
		"policy",
		"deletion",
	} {
		fmt.Printf("%s: %d\n", name, inspection.Counts[name])
	}
	return nil
}

func runValidate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	jsonOutput := fs.Bool("json", false, "emit machine-readable JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: trace validate [--json] FILE")
	}

	store, err := storage.Open(ctx, fs.Arg(0), true)
	if err != nil {
		return err
	}
	defer store.Close()
	report, err := store.Validate(ctx)
	if err != nil {
		return err
	}
	if *jsonOutput {
		if err := writeJSON(report); err != nil {
			return err
		}
	} else {
		fmt.Printf("valid: %t\n", report.Valid)
		for _, message := range report.Errors {
			fmt.Printf("error: %s\n", message)
		}
		for _, message := range report.Warnings {
			fmt.Printf("warning: %s\n", message)
		}
	}
	if !report.Valid {
		return errors.New("trace validation failed")
	}
	return nil
}

func runAddEvent(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("add-event", flag.ContinueOnError)
	eventType := fs.String("type", "", "event type")
	payload := fs.String("payload", `{}`, "JSON payload")
	namespace := fs.String("namespace", "", "namespace")
	actor := fs.String("actor", "", "actor identifier")
	recordedAt := fs.String("recorded-at", "", "RFC3339 timestamp")
	occurredAt := fs.String("occurred-at", "", "RFC3339 timestamp")
	sourceType := fs.String("source-type", "trace.cli", "provenance source type")
	sourceID := fs.String("source-id", "", "provenance source ID")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: trace add-event [flags] FILE")
	}
	recorded, err := parseOptionalTime(*recordedAt)
	if err != nil {
		return fmt.Errorf("recorded-at: %w", err)
	}
	occurred, err := parseOptionalTime(*occurredAt)
	if err != nil {
		return fmt.Errorf("occurred-at: %w", err)
	}

	store, err := storage.Open(ctx, fs.Arg(0), false)
	if err != nil {
		return err
	}
	defer store.Close()
	event, err := store.AddEvent(ctx, model.EventInput{
		EventType:  *eventType,
		Payload:    json.RawMessage(*payload),
		RecordedAt: valueOrNow(recorded),
		OccurredAt: occurred,
		Namespace:  *namespace,
		Actor:      *actor,
		Provenance: model.Provenance{
			SourceType: *sourceType,
			SourceID:   *sourceID,
			Operation:  "trace add-event",
			Agent:      *actor,
		},
	})
	if err != nil {
		return err
	}
	return writeJSON(event)
}

func runRemember(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("remember", flag.ContinueOnError)
	kind := fs.String("kind", "fact", "fact, observation, or context")
	content := fs.String("content", "", "memory content")
	structured := fs.String("structured", "", "structured JSON value")
	namespace := fs.String("namespace", "", "namespace")
	actor := fs.String("actor", "", "actor identifier")
	recordedAt := fs.String("recorded-at", "", "RFC3339 timestamp")
	validFrom := fs.String("valid-from", "", "RFC3339 timestamp")
	validTo := fs.String("valid-to", "", "RFC3339 timestamp")
	derivedFrom := fs.String("derived-from", "", "comma-separated source IDs")
	sourceType := fs.String("source-type", "trace.cli", "provenance source type")
	sourceID := fs.String("source-id", "", "provenance source ID")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: trace remember [flags] FILE")
	}
	recorded, err := parseOptionalTime(*recordedAt)
	if err != nil {
		return fmt.Errorf("recorded-at: %w", err)
	}
	from, err := parseOptionalTime(*validFrom)
	if err != nil {
		return fmt.Errorf("valid-from: %w", err)
	}
	to, err := parseOptionalTime(*validTo)
	if err != nil {
		return fmt.Errorf("valid-to: %w", err)
	}

	var structuredValue json.RawMessage
	if *structured != "" {
		structuredValue = json.RawMessage(*structured)
	}
	var sources []string
	for _, source := range strings.Split(*derivedFrom, ",") {
		if source = strings.TrimSpace(source); source != "" {
			sources = append(sources, source)
		}
	}

	store, err := storage.Open(ctx, fs.Arg(0), false)
	if err != nil {
		return err
	}
	defer store.Close()
	memory, err := store.AddMemory(ctx, model.MemoryInput{
		Kind:            *kind,
		Content:         *content,
		StructuredValue: structuredValue,
		ValidFrom:       from,
		ValidTo:         to,
		RecordedAt:      valueOrNow(recorded),
		Namespace:       *namespace,
		DerivedFrom:     sources,
		Provenance: model.Provenance{
			SourceType: *sourceType,
			SourceID:   *sourceID,
			Operation:  "trace remember",
			Agent:      *actor,
		},
	})
	if err != nil {
		return err
	}
	return writeJSON(memory)
}

func runQuery(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	jsonOutput := fs.Bool("json", false, "emit machine-readable JSON")
	namespace := fs.String("namespace", "", "namespace filter")
	validAt := fs.String("valid-at", "", "RFC3339 point-in-time filter")
	recordedBefore := fs.String("recorded-before", "", "exclusive RFC3339 recorded-time filter")
	recordedAfter := fs.String("recorded-after", "", "inclusive RFC3339 recorded-time filter")
	includeSuperseded := fs.Bool("include-superseded", false, "include superseded memories")
	includeInvalidated := fs.Bool("include-invalidated", false, "include invalidated memories")
	includeRedacted := fs.Bool("include-redacted", false, "include redacted memories")
	limit := fs.Int("limit", 100, "maximum number of memories")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: trace query [flags] FILE")
	}
	point, err := parseOptionalTime(*validAt)
	if err != nil {
		return fmt.Errorf("valid-at: %w", err)
	}
	before, err := parseOptionalTime(*recordedBefore)
	if err != nil {
		return fmt.Errorf("recorded-before: %w", err)
	}
	after, err := parseOptionalTime(*recordedAfter)
	if err != nil {
		return fmt.Errorf("recorded-after: %w", err)
	}

	store, err := storage.Open(ctx, fs.Arg(0), true)
	if err != nil {
		return err
	}
	defer store.Close()
	result, err := store.QueryMemories(ctx, model.MemoryQuery{
		Namespace:          *namespace,
		ValidAt:            point,
		RecordedBefore:     before,
		RecordedAfter:      after,
		IncludeSuperseded:  *includeSuperseded,
		IncludeInvalidated: *includeInvalidated,
		IncludeRedacted:    *includeRedacted,
		Limit:              *limit,
	})
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(result)
	}
	fmt.Printf("evidence: %s\n", result.EvidenceState)
	fmt.Printf("memories: %d\n", len(result.Memories))
	for _, memory := range result.Memories {
		fmt.Printf("%s [%s] %s\n", memory.ID, memory.Status, memory.Content)
	}
	fmt.Printf("conflicts: %d\n", len(result.Conflicts))
	return nil
}

func runHistory(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("history", flag.ContinueOnError)
	jsonOutput := fs.Bool("json", false, "emit machine-readable JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 || fs.NArg() > 2 {
		return errors.New("usage: trace history [--json] FILE [RECORD_ID]")
	}
	targetID := ""
	if fs.NArg() == 2 {
		targetID = fs.Arg(1)
	}
	store, err := storage.Open(ctx, fs.Arg(0), true)
	if err != nil {
		return err
	}
	defer store.Close()
	history, err := store.History(ctx, targetID)
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(history)
	}
	for _, mutation := range history {
		fmt.Printf("%d %s %s %s\n", mutation.Sequence, mutation.Operation, mutation.TargetID, mutation.CreatedAt.Format(time.RFC3339Nano))
	}
	return nil
}

func runExplain(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("explain", flag.ContinueOnError)
	jsonOutput := fs.Bool("json", false, "emit machine-readable JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("usage: trace explain [--json] FILE RECORD_ID")
	}
	store, err := storage.Open(ctx, fs.Arg(0), true)
	if err != nil {
		return err
	}
	defer store.Close()
	explanation, err := store.Explain(ctx, fs.Arg(1))
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(explanation)
	}
	fmt.Printf("target: %s [%s]\n", explanation.TargetID, explanation.TargetKind)
	fmt.Printf("source events: %d\n", len(explanation.SourceEvents))
	fmt.Printf("source memories: %d\n", len(explanation.SourceMemories))
	fmt.Printf("source entities: %d\n", len(explanation.SourceEntities))
	fmt.Printf("derivations: %d\n", len(explanation.Derivations))
	fmt.Printf("mutations: %d\n", len(explanation.Mutations))
	return nil
}

func runDiff(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("diff", flag.ContinueOnError)
	jsonOutput := fs.Bool("json", false, "emit machine-readable JSON")
	from := fs.Int64("from", 0, "starting mutation sequence, exclusive")
	to := fs.Int64("to", 0, "ending mutation sequence, inclusive; zero means current")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: trace diff [--json] [--from N] [--to N] FILE")
	}
	store, err := storage.Open(ctx, fs.Arg(0), true)
	if err != nil {
		return err
	}
	defer store.Close()
	diff, err := store.Diff(ctx, model.Snapshot{
		Sequence: *from,
	}, model.Snapshot{
		Sequence: *to,
	})
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(diff)
	}
	fmt.Printf("from: %d\n", diff.From.Sequence)
	fmt.Printf("to: %d\n", diff.To.Sequence)
	fmt.Printf("mutations: %d\n", len(diff.Mutations))
	for _, mutation := range diff.Mutations {
		fmt.Printf("%d %s %s\n", mutation.Sequence, mutation.Operation, mutation.TargetID)
	}
	fmt.Printf("added records: %d\n", len(diff.Added))
	return nil
}

func parseOptionalTime(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := model.ParseTime(value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func valueOrNow(value *time.Time) time.Time {
	if value == nil {
		return time.Now().UTC()
	}
	return *value
}

func writeJSON(value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode JSON output: %w", err)
	}
	fmt.Println(string(encoded))
	return nil
}
