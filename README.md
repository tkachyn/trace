# Trace

Trace is an open, model-independent memory infrastructure for AI agents. It
stores durable agent memory in inspectable `.trc` files while preserving
provenance, temporal validity, change history, relationships, policies, and
deletion semantics.

Trace keeps canonical memory independent from embeddings, language models, and
retrieval providers. The current reference implementation is written in Go,
with Python integrations for extraction and evaluation.

## Features

- SQLite-backed `.trc` files with a versioned logical schema
- canonical events, memories, entities, provenance, derivations, and mutations
- valid-time and recorded-time queries
- explicit confirmation, contradiction, supersession, history, diffs, and explanations
- deterministic exact, metadata, SQLite FTS5, and hybrid retrieval
- optional rebuildable semantic indexes with model and content-hash metadata
- bounded relationship traversal and entity alias lookup
- namespace-scoped policies with deny precedence and fail-closed authorization
- dependency-aware forgetting with tombstone and redaction modes
- atomic JSON and JSONL batch ingestion
- versioned loopback HTTP API and dependency-free Python client
- deterministic JSONL bundle export/import with checksums and merge mappings

## Requirements

- Go 1.27 or newer
- Python 3.10 or newer for the integration package and benchmark helpers

The Go core does not require Python, an LLM, an embedding provider, or a
network service.

## Quick start

Create a local Trace file:

```text
go run ./cmd/trace init memory.trc
```

Add a source event and derived memory:

```text
go run ./cmd/trace add-event `
  --type conversation.message `
  --payload '{"content":"I prefer Go"}' `
  --namespace user:tim `
  memory.trc

go run ./cmd/trace remember `
  --kind fact `
  --content "Tim prefers Go" `
  --namespace user:tim `
  memory.trc
```

Inspect and validate the file:

```text
go run ./cmd/trace inspect --json memory.trc
go run ./cmd/trace validate --json memory.trc
```

Search canonical memory:

```text
go run ./cmd/trace search `
  --text "prefers Go" `
  --namespace user:tim `
  --json `
  memory.trc
```

For a compiled binary, build the command once:

```text
go build -o trace.exe ./cmd/trace
trace.exe inspect memory.trc
```

## Commands

The command line interface is intended for local operation, inspection, and
automation. Every inspection and retrieval command supports JSON output where
applicable.

```text
trace init FILE
trace add-event FILE
trace remember FILE
trace query FILE
trace search FILE
trace inspect FILE
trace validate FILE
trace history FILE [RECORD_ID]
trace explain FILE RECORD_ID
trace diff FILE
trace rebuild-index FILE
trace semantic-status FILE
trace entity-lookup FILE
trace traverse FILE
trace policy-add FILE
trace forget FILE RECORD_ID
trace ingest FILE
trace serve FILE
trace export FILE DESTINATION
trace import SOURCE FILE
```

`trace ingest` accepts the versioned `trace.batch` JSON contract on standard
input. JSONL ingestion uses a header, record lines, and an explicit commit
marker. The entire batch is validated and committed atomically.

## API and Python integration

Start the local versioned HTTP API on loopback:

```text
go run ./cmd/trace serve --listen 127.0.0.1:8080 memory.trc
```

The API is exposed under `/v1` and includes ingestion, query, search,
explanation, history, forgetting, inspection, validation, and direct record
reads. Protected reads require an explicit caller principal.

The Python package uses the Go CLI or HTTP API as its boundary. It does not
open SQLite files or reimplement Trace storage semantics.

```text
$env:PYTHONPATH = "python"
python -m unittest discover -s python/tests -p "test_*.py"
```

## Bundles

Export a deterministic, text-readable interchange bundle:

```text
go run ./cmd/trace export --json memory.trc memory-bundle
```

Import it into a new file:

```text
go run ./cmd/trace import --mode new --json memory-bundle imported.trc
```

Bundles contain a manifest, canonical JSONL record files, and `CHECKSUMS`.
Rebuildable retrieval indexes are excluded. Imports validate checksums before
mutation, preserve collision-free IDs, remap conflicting IDs, and record
imported bundle IDs for idempotent retries.

## Project layout

| path | purpose |
| ------------- | ---------------------------------------- |
| `cmd/trace` | command line interface and local API server |
| `model` | canonical records, queries, policies, and result types |
| `storage` | SQLite persistence, validation, retrieval, indexes, and bundles |
| `format` | versioned ingestion and interchange contracts |
| `api` | versioned loopback HTTP API |
| `python/trace_integration` | provider-neutral Python client and benchmark helpers |
| `docs/design` | technical design and roadmap |

## Architecture

| layer | responsibility |
| ------------- | ---------------------------------------- |
| canonical model | define portable records, identifiers, timestamps, hashes, and relationships |
| storage | persist canonical records and enforce transactions, validation, policy, and deletion |
| retrieval | provide exact, metadata, full-text, semantic, and relationship strategies |
| interfaces | expose the Go CLI, JSON/JSONL operations, HTTP API, and Python clients |
| interchange | export and import deterministic bundles without treating indexes as truth |

The logical model is the compatibility boundary. SQLite is the current
physical container, while FTS5 and semantic indexes are rebuildable
accelerators.

## Future updates

The next roadmap work focuses on hardening and proving the implementation:

- crash-injection, corruption recovery, and stale-index testing
- benchmark datasets from 1K through practical larger scales
- cold-start, rebuild, storage-size, and concurrent-reader measurements
- a documented SQLite driver and performance decision
- a public logical format specification and compatibility policy
- an independent reader or importer for conformance testing
- security and privacy review of authorization, deletion, and bundle flows
- evaluation of signed bundles and additional protocol adapters when users need them

Trace will only move to a custom physical container if reproducible workloads
show that SQLite materially limits portability, recovery, storage efficiency,
or performance. Embeddings remain optional, and MCP remains an adapter rather
than a replacement for the Trace API or file format.

## License

Trace is available under the MIT License.
