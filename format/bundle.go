package format

import "time"

const (
	// bundle version identifies the directory interchange profile
	BundleVersion = 1
	// checksum algorithm identifies the manifest checksum encoding
	ChecksumAlgorithm = "sha256"
)

// BundleManifest describes one deterministic Trace interchange bundle
type BundleManifest struct {
	BundleID          string         `json:"bundle_id"`
	SourceFileID      string         `json:"source_file_id"`
	FormatName        string         `json:"format_name"`
	FormatVersion     string         `json:"format_version"`
	SchemaVersion     int            `json:"schema_version"`
	BundleVersion     int            `json:"bundle_version"`
	SnapshotSequence  int64          `json:"snapshot_sequence"`
	ExportedAt        time.Time      `json:"exported_at"`
	ChecksumAlgorithm string         `json:"checksum_algorithm"`
	Files             []string       `json:"files"`
	Counts            map[string]int `json:"counts"`
}

// LossReport records a preserved, approximated, dropped, or unresolved field
type LossReport struct {
	SourceFormat   string `json:"source_format"`
	SourceVersion  string `json:"source_version"`
	AdapterVersion string `json:"adapter_version"`
	SourcePath     string `json:"source_path"`
	Destination    string `json:"destination"`
	Disposition    string `json:"disposition"`
	Reason         string `json:"reason"`
	Preservation   string `json:"preservation,omitempty"`
}

// ImportResult reports bundle mappings and compatibility losses
type ImportResult struct {
	BundleID   string            `json:"bundle_id"`
	Mode       string            `json:"mode"`
	Idempotent bool              `json:"idempotent"`
	IDMap      map[string]string `json:"id_map"`
	Losses     []LossReport      `json:"losses"`
	Counts     map[string]int    `json:"counts"`
}
