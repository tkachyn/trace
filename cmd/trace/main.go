// main exposes the initial Trace command line interface
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"trace/api"
	"trace/format"
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
		return errors.New("usage: trace <init|inspect|validate|add-event|remember|query|search|rebuild-index|semantic-status|entity-lookup|traverse|policy-add|forget|ingest|serve|export|import|history|explain|diff> ...")
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
	case "search":
		return runSearch(ctx, args[1:])
	case "rebuild-index":
		return runRebuildIndex(ctx, args[1:])
	case "semantic-status":
		return runSemanticStatus(ctx, args[1:])
	case "entity-lookup":
		return runEntityLookup(ctx, args[1:])
	case "traverse":
		return runTraverse(ctx, args[1:])
	case "policy-add":
		return runPolicyAdd(ctx, args[1:])
	case "forget":
		return runForget(ctx, args[1:])
	case "ingest":
		return runIngest(ctx, args[1:])
	case "serve":
		return runServe(ctx, args[1:])
	case "export":
		return runExport(ctx, args[1:])
	case "import":
		return runImport(ctx, args[1:])
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
	caller := fs.String("caller", "", "caller principal for policy evaluation")
	purpose := fs.String("purpose", "", "access purpose for policy evaluation")
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
	query := model.MemoryQuery{
		Namespace:          *namespace,
		ValidAt:            point,
		RecordedBefore:     before,
		RecordedAfter:      after,
		IncludeSuperseded:  *includeSuperseded,
		IncludeInvalidated: *includeInvalidated,
		IncludeRedacted:    *includeRedacted,
		Limit:              *limit,
	}
	var result model.QueryResult
	if *caller == "" {
		result, err = store.QueryMemories(ctx, query)
	} else {
		result, err = store.QueryMemoriesAuthorized(ctx, query, model.AccessContext{
			Principal: *caller,
			Purpose:   *purpose,
		})
	}
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

func runSearch(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	jsonOutput := fs.Bool("json", false, "emit machine-readable JSON")
	text := fs.String("text", "", "text to search")
	exactIDs := fs.String("id", "", "comma-separated exact record IDs")
	contentHash := fs.String("hash", "", "exact content hash")
	namespace := fs.String("namespace", "", "namespace filter")
	namespaceScope := fs.String("namespace-scope", "", "comma-separated namespace scope")
	kind := fs.String("kind", "memory", "memory, event, or all")
	subject := fs.String("subject", "", "subject entity ID filter")
	predicate := fs.String("predicate", "", "predicate filter")
	objectValue := fs.String("object", "", "object value filter")
	validAt := fs.String("valid-at", "", "RFC3339 point-in-time filter")
	recordedBefore := fs.String("recorded-before", "", "exclusive RFC3339 recorded-time filter")
	recordedAfter := fs.String("recorded-after", "", "inclusive RFC3339 recorded-time filter")
	includeSuperseded := fs.Bool("include-superseded", false, "include superseded memories")
	includeInvalidated := fs.Bool("include-invalidated", false, "include invalidated memories")
	includeRedacted := fs.Bool("include-redacted", false, "include redacted memories")
	minimumScore := fs.Float64("min-score", 0, "minimum deterministic score")
	strict := fs.Bool("strict", false, "omit context for weak or conflicting evidence")
	limit := fs.Int("limit", 100, "maximum number of hits")
	contextBytes := fs.Int("context-bytes", 0, "maximum compiled context bytes")
	caller := fs.String("caller", "", "caller principal for policy evaluation")
	purpose := fs.String("purpose", "", "access purpose for policy evaluation")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: trace search [flags] FILE")
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
	query := model.RetrievalQuery{
		Text:               *text,
		ExactIDs:           splitCSV(*exactIDs),
		ContentHash:        *contentHash,
		Namespace:          *namespace,
		NamespaceScope:     splitCSV(*namespaceScope),
		Kind:               *kind,
		SubjectEntityID:    *subject,
		Predicate:          *predicate,
		ObjectValue:        *objectValue,
		ValidAt:            point,
		RecordedBefore:     before,
		RecordedAfter:      after,
		IncludeSuperseded:  *includeSuperseded,
		IncludeInvalidated: *includeInvalidated,
		IncludeRedacted:    *includeRedacted,
		MinimumScore:       *minimumScore,
		Strict:             *strict,
		Limit:              *limit,
		ContextByteLimit:   *contextBytes,
	}
	var result model.RetrievalResult
	if *caller == "" {
		result, err = store.Search(ctx, query)
	} else {
		result, err = store.SearchAuthorized(ctx, query, model.AccessContext{
			Principal: *caller,
			Purpose:   *purpose,
		})
	}
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(result)
	}
	fmt.Printf("evidence: %s\n", result.EvidenceState)
	fmt.Printf("hits: %d\n", len(result.Hits))
	for _, hit := range result.Hits {
		fmt.Printf("%s [%s] %.3f %s\n", hit.ID, hit.Kind, hit.Score.Total, hit.Content)
	}
	fmt.Printf("context bytes: %d\n", result.Context.Bytes)
	return nil
}

func runRebuildIndex(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("rebuild-index", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: trace rebuild-index FILE")
	}
	store, err := storage.Open(ctx, fs.Arg(0), false)
	if err != nil {
		return err
	}
	defer store.Close()
	return store.RebuildRetrievalIndex(ctx)
}

func runSemanticStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("semantic-status", flag.ContinueOnError)
	jsonOutput := fs.Bool("json", false, "emit machine-readable JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: trace semantic-status [--json] FILE")
	}
	store, err := storage.Open(ctx, fs.Arg(0), true)
	if err != nil {
		return err
	}
	defer store.Close()
	status, err := store.SemanticIndexStatus(ctx)
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(status)
	}
	fmt.Printf("available: %t\n", status.Available)
	fmt.Printf("ready: %t\n", status.Ready)
	fmt.Printf("stale: %t\n", status.Stale)
	fmt.Printf("records: %d\n", status.RecordCount)
	if status.Warning != "" {
		fmt.Printf("warning: %s\n", status.Warning)
	}
	return nil
}

func runEntityLookup(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("entity-lookup", flag.ContinueOnError)
	jsonOutput := fs.Bool("json", false, "emit machine-readable JSON")
	text := fs.String("text", "", "canonical name or alias")
	namespace := fs.String("namespace", "", "namespace filter")
	limit := fs.Int("limit", 100, "maximum number of entities")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: trace entity-lookup [flags] FILE")
	}
	store, err := storage.Open(ctx, fs.Arg(0), true)
	if err != nil {
		return err
	}
	defer store.Close()
	entities, err := store.LookupEntities(ctx, model.EntityQuery{
		Text:      *text,
		Namespace: *namespace,
		Limit:     *limit,
	})
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(entities)
	}
	for _, entity := range entities {
		fmt.Printf("%s [%s] %s\n", entity.ID, entity.EntityType, entity.CanonicalName)
	}
	return nil
}

func runTraverse(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("traverse", flag.ContinueOnError)
	jsonOutput := fs.Bool("json", false, "emit machine-readable JSON")
	start := fs.String("start", "", "comma-separated start record IDs")
	relations := fs.String("relations", "", "comma-separated relation filters")
	direction := fs.String("direction", "outgoing", "outgoing, incoming, or both")
	maxHops := fs.Int("max-hops", 1, "maximum traversal hops")
	namespace := fs.String("namespace", "", "namespace filter")
	limit := fs.Int("limit", 100, "maximum number of records")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: trace traverse [flags] FILE")
	}
	store, err := storage.Open(ctx, fs.Arg(0), true)
	if err != nil {
		return err
	}
	defer store.Close()
	result, err := store.Traverse(ctx, model.RelationshipQuery{
		StartIDs:  splitCSV(*start),
		Relations: splitCSV(*relations),
		Direction: *direction,
		MaxHops:   *maxHops,
		Namespace: *namespace,
		Limit:     *limit,
	})
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(result)
	}
	fmt.Printf("records: %d\n", len(result.Records))
	fmt.Printf("edges: %d\n", len(result.Edges))
	return nil
}

func runPolicyAdd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("policy-add", flag.ContinueOnError)
	effect := fs.String("effect", "", "allow or deny")
	principal := fs.String("principal", "", "caller principal or *")
	operation := fs.String("operation", "read", "protected operation")
	namespace := fs.String("namespace", "", "bound namespace")
	selector := fs.String("selector", "{}", "resource selector JSON")
	conditions := fs.String("conditions", "{}", "conditions JSON")
	expiresAt := fs.String("expires-at", "", "RFC3339 expiry")
	actor := fs.String("actor", "", "policy author")
	jsonOutput := fs.Bool("json", false, "emit machine-readable JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: trace policy-add [flags] FILE")
	}
	expires, err := parseOptionalTime(*expiresAt)
	if err != nil {
		return fmt.Errorf("expires-at: %w", err)
	}
	store, err := storage.Open(ctx, fs.Arg(0), false)
	if err != nil {
		return err
	}
	defer store.Close()
	policy, err := store.AddPolicy(ctx, model.PolicyInput{
		Effect:           *effect,
		Principal:        *principal,
		Operation:        *operation,
		Namespace:        *namespace,
		ResourceSelector: json.RawMessage(*selector),
		Conditions:       json.RawMessage(*conditions),
		ExpiresAt:        expires,
		Actor:            *actor,
	})
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(policy)
	}
	fmt.Printf("policy: %s\n", policy.ID)
	return nil
}

func runForget(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("forget", flag.ContinueOnError)
	mode := fs.String("mode", "dependency_closure", "dependency_closure, target_only, dependency_closure_redact, or target_only_redact")
	actor := fs.String("actor", "", "caller and actor principal")
	purpose := fs.String("purpose", "", "forget purpose")
	jsonOutput := fs.Bool("json", false, "emit machine-readable JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("usage: trace forget [flags] FILE RECORD_ID")
	}
	store, err := storage.Open(ctx, fs.Arg(0), false)
	if err != nil {
		return err
	}
	defer store.Close()
	deletions, err := store.Forget(ctx, model.ForgetRequest{
		TargetID: fs.Arg(1),
		Mode:     *mode,
		Actor:    *actor,
		Access: model.AccessContext{
			Principal: *actor,
			Purpose:   *purpose,
		},
	})
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(deletions)
	}
	fmt.Printf("forgotten: %d\n", len(deletions))
	return nil
}

func runIngest(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	jsonl := fs.Bool("jsonl", false, "read the versioned JSONL batch protocol")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: trace ingest [--jsonl] FILE")
	}
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		return fmt.Errorf("read ingest input: %w", err)
	}
	var request format.BatchRequest
	if *jsonl {
		request, err = format.DecodeJSONL(strings.NewReader(string(input)))
	} else {
		var envelope struct {
			Protocol string `json:"protocol"`
			Type     string `json:"type"`
		}
		if err := format.NormalizeJSON(input, &envelope); err != nil {
			return fmt.Errorf("decode ingest input: %w", err)
		}
		if envelope.Type == format.ExtractionProtocol {
			var proposal format.ExtractionProposal
			if err := format.NormalizeJSON(input, &proposal); err != nil {
				return fmt.Errorf("decode extraction proposal: %w", err)
			}
			request, err = proposal.ToBatchRequest()
		} else {
			if err := format.NormalizeJSON(input, &request); err != nil {
				return fmt.Errorf("decode batch request: %w", err)
			}
		}
	}
	if err != nil {
		return err
	}
	store, err := storage.Open(ctx, fs.Arg(0), false)
	if err != nil {
		return err
	}
	defer store.Close()
	result, err := store.IngestBatch(ctx, request)
	if err != nil {
		return err
	}
	if err := writeJSON(result); err != nil {
		return err
	}
	if len(result.Errors) > 0 {
		return errors.New("trace ingest rejected")
	}
	return nil
}

func runServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	listen := fs.String("listen", "127.0.0.1:8080", "loopback listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: trace serve [--listen HOST:PORT] FILE")
	}
	store, err := storage.Open(ctx, fs.Arg(0), false)
	if err != nil {
		return err
	}
	defer store.Close()
	server := &http.Server{
		Addr:    *listen,
		Handler: api.NewServer(store),
	}
	return server.ListenAndServe()
}

func runExport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	jsonOutput := fs.Bool("json", false, "emit machine-readable JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("usage: trace export [--json] FILE DESTINATION")
	}
	store, err := storage.Open(ctx, fs.Arg(0), true)
	if err != nil {
		return err
	}
	defer store.Close()
	manifest, err := store.ExportBundle(ctx, fs.Arg(1))
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(manifest)
	}
	fmt.Printf("bundle: %s\n", manifest.BundleID)
	fmt.Printf("destination: %s\n", fs.Arg(1))
	return nil
}

func runImport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	mode := fs.String("mode", "merge", "new or merge")
	jsonOutput := fs.Bool("json", false, "emit machine-readable JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("usage: trace import [--mode new|merge] [--json] SOURCE FILE")
	}
	result, err := storage.ImportBundle(ctx, fs.Arg(0), fs.Arg(1), *mode)
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(result)
	}
	fmt.Printf("bundle: %s\n", result.BundleID)
	fmt.Printf("id mappings: %d\n", len(result.IDMap))
	fmt.Printf("idempotent: %t\n", result.Idempotent)
	return nil
}

func runHistory(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("history", flag.ContinueOnError)
	jsonOutput := fs.Bool("json", false, "emit machine-readable JSON")
	caller := fs.String("caller", "", "caller principal for policy evaluation")
	purpose := fs.String("purpose", "", "access purpose for policy evaluation")
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
	var history []model.Mutation
	if *caller == "" {
		history, err = store.History(ctx, targetID)
	} else {
		history, err = store.HistoryAuthorized(ctx, targetID, model.AccessContext{
			Principal: *caller,
			Purpose:   *purpose,
		})
	}
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
	caller := fs.String("caller", "", "caller principal for policy evaluation")
	purpose := fs.String("purpose", "", "access purpose for policy evaluation")
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
	var explanation model.Explanation
	if *caller == "" {
		explanation, err = store.Explain(ctx, fs.Arg(1))
	} else {
		explanation, err = store.ExplainAuthorized(ctx, fs.Arg(1), model.AccessContext{
			Principal: *caller,
			Purpose:   *purpose,
		})
	}
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

func splitCSV(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}
