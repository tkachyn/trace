package storage

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"trace/format"
	"trace/model"
)

type bundleTable struct {
	table string
	file  string
	json  map[string]bool
}

var bundleTables = []bundleTable{
	{table: "namespace", file: "namespaces.jsonl"},
	{table: "provenance", file: "provenance.jsonl", json: map[string]bool{"parent_provenance_ids": true, "extensions": true}},
	{table: "event", file: "events.jsonl", json: map[string]bool{"payload": true, "extensions": true}},
	{table: "memory", file: "memories.jsonl", json: map[string]bool{"structured_value": true, "confidence": true, "extensions": true}},
	{table: "entity", file: "entities.jsonl", json: map[string]bool{"aliases": true, "extensions": true}},
	{table: "derivation", file: "derivations.jsonl", json: map[string]bool{"extensions": true}},
	{table: "mutation", file: "mutations.jsonl", json: map[string]bool{"metadata": true}},
	{table: "policy", file: "policies.jsonl", json: map[string]bool{"resource_selector": true, "conditions": true}},
	{table: "policy_binding", file: "policy_bindings.jsonl"},
	{table: "deletion", file: "deletions.jsonl", json: map[string]bool{"details": true}},
}

// ExportBundle writes a deterministic directory interchange bundle
func (s *Store) ExportBundle(ctx context.Context, destination string) (format.BundleManifest, error) {
	if destination == "" {
		return format.BundleManifest{}, errors.New("bundle destination is required")
	}
	if _, err := os.Stat(destination); err == nil {
		return format.BundleManifest{}, fmt.Errorf("bundle destination already exists: %s", destination)
	} else if !os.IsNotExist(err) {
		return format.BundleManifest{}, fmt.Errorf("check bundle destination: %w", err)
	}
	if err := os.MkdirAll(destination, 0o755); err != nil {
		return format.BundleManifest{}, fmt.Errorf("create bundle destination: %w", err)
	}
	inspection, err := s.Inspect(ctx)
	if err != nil {
		return format.BundleManifest{}, err
	}
	snapshot, err := s.Snapshot(ctx)
	if err != nil {
		return format.BundleManifest{}, err
	}
	bundleID, err := model.NewID()
	if err != nil {
		return format.BundleManifest{}, err
	}
	rowsByFile, err := s.readBundleRows(ctx)
	if err != nil {
		return format.BundleManifest{}, err
	}
	files := make([]string, 0, len(bundleTables)+1)
	counts := make(map[string]int)
	for _, table := range bundleTables {
		rows := rowsByFile[table.file]
		if err := writeBundleJSONL(filepath.Join(destination, table.file), rows); err != nil {
			return format.BundleManifest{}, err
		}
		files = append(files, table.file)
		counts[table.table] = len(rows)
	}
	sort.Strings(files)
	manifestFiles := append([]string(nil), files...)
	manifest := format.BundleManifest{
		BundleID:          bundleID,
		SourceFileID:      inspection.Manifest.FileID,
		FormatName:        model.FormatName,
		FormatVersion:     model.FormatVersion,
		SchemaVersion:     inspection.Manifest.SchemaVersion,
		BundleVersion:     format.BundleVersion,
		SnapshotSequence:  snapshot.Sequence,
		ExportedAt:        time.Now().UTC(),
		ChecksumAlgorithm: format.ChecksumAlgorithm,
		Files:             manifestFiles,
		Counts:            counts,
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return format.BundleManifest{}, fmt.Errorf("encode bundle manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(destination, "manifest.json"), append(manifestBytes, '\n'), 0o644); err != nil {
		return format.BundleManifest{}, fmt.Errorf("write bundle manifest: %w", err)
	}
	files = append(files, "manifest.json")
	sort.Strings(files)
	if err := writeBundleChecksums(destination, files); err != nil {
		return format.BundleManifest{}, err
	}
	return manifest, nil
}

// ImportBundle validates and atomically merges a directory bundle
func ImportBundle(
	ctx context.Context,
	source, target, mode string,
) (format.ImportResult, error) {
	if mode == "" {
		mode = "merge"
	}
	if mode != "new" && mode != "merge" {
		return format.ImportResult{}, fmt.Errorf("unsupported bundle import mode %q", mode)
	}
	manifest, rowsByFile, err := readBundle(source)
	if err != nil {
		return format.ImportResult{}, err
	}
	if manifest.FormatName != model.FormatName ||
		manifest.FormatVersion != model.FormatVersion ||
		manifest.SchemaVersion != model.SchemaVersion ||
		manifest.BundleVersion != format.BundleVersion {
		return format.ImportResult{}, errors.New("unsupported bundle version")
	}
	if target == "" {
		return format.ImportResult{}, errors.New("import target is required")
	}
	if _, err := os.Stat(target); os.IsNotExist(err) {
		if err := Init(ctx, target, model.DefaultGenerator); err != nil {
			return format.ImportResult{}, err
		}
	} else if err != nil {
		return format.ImportResult{}, fmt.Errorf("check import target: %w", err)
	}
	store, err := Open(ctx, target, false)
	if err != nil {
		return format.ImportResult{}, err
	}
	defer store.Close()
	var alreadyImported bool
	if err := store.db.QueryRowContext(
		ctx,
		"SELECT EXISTS (SELECT 1 FROM bundle_import WHERE bundle_id = ?)",
		manifest.BundleID,
	).Scan(&alreadyImported); err != nil {
		return format.ImportResult{}, fmt.Errorf("check bundle import ledger: %w", err)
	}
	if alreadyImported {
		return format.ImportResult{
			BundleID:   manifest.BundleID,
			Mode:       mode,
			Idempotent: true,
			IDMap:      map[string]string{},
			Losses:     []format.LossReport{},
			Counts:     manifest.Counts,
		}, nil
	}
	if mode == "new" {
		inspection, err := store.Inspect(ctx)
		if err != nil {
			return format.ImportResult{}, err
		}
		if inspection.Counts["event"] != 0 || inspection.Counts["memory"] != 0 ||
			inspection.Counts["entity"] != 0 {
			return format.ImportResult{}, errors.New("new bundle import requires an empty target")
		}
	}
	result, err := store.importBundleRows(ctx, manifest, rowsByFile, mode)
	if err != nil {
		return format.ImportResult{}, err
	}
	if err := store.RebuildRetrievalIndex(ctx); err != nil {
		return format.ImportResult{}, err
	}
	return result, nil
}

func (s *Store) readBundleRows(ctx context.Context) (map[string][]map[string]any, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin bundle snapshot: %w", err)
	}
	defer tx.Rollback()
	result := make(map[string][]map[string]any)
	for _, table := range bundleTables {
		rows, err := readTableRows(ctx, tx, table)
		if err != nil {
			return nil, err
		}
		result[table.file] = rows
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit bundle snapshot: %w", err)
	}
	return result, nil
}

func readTableRows(
	ctx context.Context,
	queryer interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	},
	table bundleTable,
) ([]map[string]any, error) {
	rows, err := queryer.QueryContext(ctx, "SELECT * FROM "+table.table)
	if err != nil {
		return nil, fmt.Errorf("read bundle table %s: %w", table.table, err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		destinations := make([]any, len(columns))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, fmt.Errorf("scan bundle table %s: %w", table.table, err)
		}
		value := make(map[string]any, len(columns))
		for index, column := range columns {
			current := values[index]
			if bytes, ok := current.([]byte); ok {
				current = string(bytes)
			}
			if table.json[column] {
				if current == nil {
					value[column] = nil
				} else {
					raw := []byte(fmt.Sprint(current))
					if !json.Valid(raw) {
						return nil, fmt.Errorf("bundle table %s column %s contains invalid JSON", table.table, column)
					}
					value[column] = json.RawMessage(raw)
				}
			} else {
				value[column] = current
			}
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read bundle table %s: %w", table.table, err)
	}
	sort.Slice(result, func(left, right int) bool {
		leftID, leftHasID := result[left]["id"].(string)
		rightID, rightHasID := result[right]["id"].(string)
		if leftHasID || rightHasID {
			return leftID < rightID
		}
		leftBytes, _ := json.Marshal(result[left])
		rightBytes, _ := json.Marshal(result[right])
		return string(leftBytes) < string(rightBytes)
	})
	return result, nil
}

func writeBundleJSONL(path string, rows []map[string]any) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create bundle file %s: %w", path, err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			return fmt.Errorf("write bundle file %s: %w", path, err)
		}
	}
	return nil
}

func writeBundleChecksums(directory string, files []string) error {
	file, err := os.Create(filepath.Join(directory, "CHECKSUMS"))
	if err != nil {
		return fmt.Errorf("create bundle checksums: %w", err)
	}
	defer file.Close()
	sort.Strings(files)
	for _, name := range files {
		data, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			return fmt.Errorf("read bundle file for checksum: %w", err)
		}
		sum := sha256.Sum256(data)
		if _, err := fmt.Fprintf(file, "%s  %s\n", hex.EncodeToString(sum[:]), name); err != nil {
			return fmt.Errorf("write bundle checksum: %w", err)
		}
	}
	return nil
}

func readBundle(source string) (format.BundleManifest, map[string][]map[string]any, error) {
	manifestBytes, err := os.ReadFile(filepath.Join(source, "manifest.json"))
	if err != nil {
		return format.BundleManifest{}, nil, fmt.Errorf("read bundle manifest: %w", err)
	}
	var manifest format.BundleManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return format.BundleManifest{}, nil, fmt.Errorf("decode bundle manifest: %w", err)
	}
	checksums, err := os.ReadFile(filepath.Join(source, "CHECKSUMS"))
	if err != nil {
		return format.BundleManifest{}, nil, fmt.Errorf("read bundle checksums: %w", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(checksums)), "\n") {
		parts := strings.SplitN(line, "  ", 2)
		if len(parts) != 2 {
			return format.BundleManifest{}, nil, errors.New("invalid bundle checksum line")
		}
		data, err := os.ReadFile(filepath.Join(source, parts[1]))
		if err != nil {
			return format.BundleManifest{}, nil, fmt.Errorf("read checksummed file: %w", err)
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != parts[0] {
			return format.BundleManifest{}, nil, fmt.Errorf("checksum mismatch for %s", parts[1])
		}
	}
	rowsByFile := make(map[string][]map[string]any)
	for _, table := range bundleTables {
		rows, err := readBundleJSONL(filepath.Join(source, table.file))
		if err != nil {
			return format.BundleManifest{}, nil, err
		}
		rowsByFile[table.file] = rows
	}
	return manifest, rowsByFile, nil
}

func readBundleJSONL(path string) ([]map[string]any, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open bundle records %s: %w", path, err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	result := make([]map[string]any, 0)
	for scanner.Scan() {
		var value map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			return nil, fmt.Errorf("decode bundle record %s: %w", path, err)
		}
		result = append(result, value)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read bundle records %s: %w", path, err)
	}
	return result, nil
}

func (s *Store) importBundleRows(
	ctx context.Context,
	manifest format.BundleManifest,
	rowsByFile map[string][]map[string]any,
	mode string,
) (format.ImportResult, error) {
	idMap := make(map[string]string)
	allRows := make([]map[string]any, 0)
	for _, table := range bundleTables {
		allRows = append(allRows, rowsByFile[table.file]...)
	}
	seen := make(map[string]bool)
	for _, row := range allRows {
		if id, ok := row["id"].(string); ok && id != "" {
			if seen[id] {
				return format.ImportResult{}, fmt.Errorf("duplicate bundle ID %s", id)
			}
			seen[id] = true
		}
	}
	for _, table := range bundleTables {
		for _, row := range rowsByFile[table.file] {
			oldID, ok := row["id"].(string)
			if !ok || oldID == "" {
				continue
			}
			exists, hash, err := s.existingBundleID(ctx, table.table, oldID)
			if err != nil {
				return format.ImportResult{}, err
			}
			if !exists {
				idMap[oldID] = oldID
				continue
			}
			if table.table != "event" && table.table != "memory" && table.table != "entity" {
				idMap[oldID] = oldID
				continue
			}
			if contentHash, ok := row["content_hash"].(string); ok && contentHash == hash {
				idMap[oldID] = oldID
				continue
			}
			newID, err := model.NewID()
			if err != nil {
				return format.ImportResult{}, err
			}
			idMap[oldID] = newID
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return format.ImportResult{}, fmt.Errorf("begin bundle import: %w", err)
	}
	defer tx.Rollback()
	for _, table := range bundleTables {
		for _, original := range rowsByFile[table.file] {
			row := rewriteBundleRow(cloneRow(original), idMap)
			if err := insertBundleRow(ctx, tx, table, row); err != nil {
				return format.ImportResult{}, err
			}
		}
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO bundle_import (bundle_id, source_file_id, imported_at)
		VALUES (?, ?, ?)
	`, manifest.BundleID, manifest.SourceFileID, model.FormatTime(now)); err != nil {
		return format.ImportResult{}, fmt.Errorf("record bundle import: %w", err)
	}
	if err := updateManifestTimestamp(ctx, tx, now); err != nil {
		return format.ImportResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return format.ImportResult{}, fmt.Errorf("commit bundle import: %w", err)
	}
	counts := make(map[string]int)
	for _, table := range bundleTables {
		counts[table.table] = len(rowsByFile[table.file])
	}
	losses := []format.LossReport{}
	if mode == "merge" {
		losses = append(losses, format.LossReport{
			SourceFormat:   manifest.FormatName,
			SourceVersion:  manifest.FormatVersion,
			AdapterVersion: "trace-bundle-1",
			Disposition:    "lossless",
			Reason:         "Trace bundle records preserve canonical fields",
		})
	}
	return format.ImportResult{
		BundleID: manifest.BundleID,
		Mode:     mode,
		IDMap:    idMap,
		Losses:   losses,
		Counts:   counts,
	}, nil
}

func (s *Store) existingBundleID(ctx context.Context, table, id string) (bool, string, error) {
	column := "id"
	switch table {
	case "event", "memory", "entity":
		column = "content_hash"
	}
	var hash sql.NullString
	err := s.db.QueryRowContext(
		ctx,
		"SELECT "+column+" FROM "+table+" WHERE id = ?",
		id,
	).Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("check bundle ID %s: %w", id, err)
	}
	return true, hash.String, nil
}

func cloneRow(value map[string]any) map[string]any {
	result := make(map[string]any, len(value))
	for key, current := range value {
		result[key] = current
	}
	return result
}

func rewriteBundleRow(row map[string]any, idMap map[string]string) map[string]any {
	for _, field := range []string{
		"id", "source_id", "target_id", "provenance_id", "policy_id",
		"subject_entity_id", "object_entity_id",
	} {
		if id, ok := row[field].(string); ok {
			if mapped, exists := idMap[id]; exists {
				row[field] = mapped
			}
		}
	}
	return row
}

func insertBundleRow(
	ctx context.Context,
	tx *sql.Tx,
	table bundleTable,
	row map[string]any,
) error {
	columns := make([]string, 0, len(row))
	for column := range row {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	placeholders := make([]string, len(columns))
	args := make([]any, len(columns))
	for index, column := range columns {
		placeholders[index] = "?"
		args[index] = bundleSQLValue(row[column], table.json[column])
	}
	statement := fmt.Sprintf(
		"INSERT OR IGNORE INTO %s (%s) VALUES (%s)",
		table.table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)
	if _, err := tx.ExecContext(ctx, statement, args...); err != nil {
		return fmt.Errorf("insert bundle %s row %v: %w", table.table, row, err)
	}
	return nil
}

func bundleSQLValue(value any, jsonColumn bool) any {
	if value == nil && jsonColumn {
		return "null"
	}
	switch current := value.(type) {
	case json.RawMessage:
		return string(current)
	case []any, map[string]any:
		encoded, _ := json.Marshal(current)
		return string(encoded)
	default:
		return value
	}
}
