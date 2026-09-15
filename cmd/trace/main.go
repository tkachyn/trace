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
		return errors.New("usage: trace <init|inspect|validate|add-event|remember> ...")
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
