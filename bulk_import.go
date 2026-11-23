package pgx

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// SourceSnapshot is a local, immutable copy of a supplier source file.
//
// The supplier only publishes the file once per day, so a failed import must
// never trigger a second fetch. FetchSource downloads the source once and
// stores it under SnapshotDir keyed by its checksum; every later run,
// including retries, reads that local copy through Open instead of calling
// the supplier again.
type SourceSnapshot struct {
	// SnapshotDir is the directory in which source files are cached.
	SnapshotDir string

	// Name identifies the supplier feed (e.g. "products"). It is used to name
	// the cached file and must be filesystem-safe.
	Name string

	// Checksum is the hex-encoded SHA-256 of the fetched content.
	Checksum string

	path string
}

// FetchSource streams a supplier feed from fetch into a local snapshot. If a
// snapshot with identical content already exists it is reused unchanged.
//
// expectChecksum enables refetch-free retries: pass the checksum of a
// previously stored snapshot (from [SourceSnapshot.Checksum]) and, when that
// file is still present locally, fetch is never called. Pass an empty string
// for the first fetch of the day. The supplier only publishes once per day,
// so a retry must always pass the original snapshot's checksum rather than
// calling the supplier a second time.
func FetchSource(ctx context.Context, snapshotDir, name, expectChecksum string, fetch func(context.Context) (io.ReadCloser, error)) (*SourceSnapshot, error) {
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		return nil, fmt.Errorf("create snapshot dir: %w", err)
	}

	// A feed can be versioned by many checksums (the supplier may have
	// published different content on earlier days). Only an existing snapshot
	// whose checksum the caller already knows proves the current content is
	// cached; when expectChecksum is empty we must fetch, because a stale
	// snapshot cannot be assumed to match today's feed.
	if expectChecksum != "" {
		finalPath := filepath.Join(snapshotDir, name+"-"+expectChecksum)
		if info, err := os.Stat(finalPath); err == nil && !info.IsDir() {
			return &SourceSnapshot{
				SnapshotDir: snapshotDir,
				Name:        name,
				Checksum:    expectChecksum,
				path:        finalPath,
			}, nil
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("check snapshot: %w", err)
		}
	}

	rc, err := fetch(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch supplier source %q: %w", name, err)
	}
	defer rc.Close()

	// First land the payload in a temp file so a failed fetch can never leave
	// a half-written snapshot behind.
	tmp, err := os.CreateTemp(snapshotDir, "."+name+".tmp-*")
	if err != nil {
		return nil, fmt.Errorf("create snapshot temp file: %w", err)
	}
	tmpName := tmp.Name()

	hasher := sha256.New()
	if _, err := io.Copy(io.MultiWriter(tmp, hasher), rc); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return nil, fmt.Errorf("store supplier source %q: %w", name, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return nil, fmt.Errorf("finalize snapshot: %w", err)
	}

	checksum := hex.EncodeToString(hasher.Sum(nil))
	finalPath := filepath.Join(snapshotDir, name+"-"+checksum)
	if _, err := os.Stat(finalPath); err == nil {
		// Same content was fetched earlier; keep the existing snapshot.
		os.Remove(tmpName)
	} else if !errors.Is(err, os.ErrNotExist) {
		os.Remove(tmpName)
		return nil, fmt.Errorf("check snapshot: %w", err)
	} else if err := os.Rename(tmpName, finalPath); err != nil {
		os.Remove(tmpName)
		return nil, fmt.Errorf("publish snapshot: %w", err)
	}

	return &SourceSnapshot{
		SnapshotDir: snapshotDir,
		Name:        name,
		Checksum:    checksum,
		path:        finalPath,
	}, nil
}

// Open opens the local snapshot for reading. It never contacts the supplier,
// which is what makes retries safe against the once-per-day fetch limit.
func (s *SourceSnapshot) Open() (*os.File, error) {
	return os.Open(s.path)
}

// Path returns the on-disk location of the snapshot.
func (s *SourceSnapshot) Path() string {
	return s.path
}

// RowParser converts one raw source line into the column values expected by
// the target table. Returning an error marks that source line as a bad row
// without aborting the import; the line is never sent to the database.
type RowParser func(sourceLine int, line []byte) ([]any, error)

// BulkImportResult extends [ResilientCopyResult] with source-level positions
// and parse failures. Every failure, whether from parsing or from the
// database, carries the 1-based source line so the supplier can be told
// exactly which record is bad.
type BulkImportResult struct {
	ResilientCopyResult

	// ParseFailures holds source lines the parser rejected. They were never
	// sent to the database.
	ParseFailures []RowFailure

	// TotalLines is the number of data lines read from the snapshot.
	TotalLines int
}

// AllFailures returns parse failures and database failures merged and sorted
// by source line, giving the operator one ordered list of every bad record
// to send back to the supplier.
func (r *BulkImportResult) AllFailures() []RowFailure {
	merged := make([]RowFailure, 0, len(r.ParseFailures)+len(r.Failures))
	merged = append(merged, r.ParseFailures...)
	merged = append(merged, r.Failures...)
	sort.SliceStable(merged, func(i, j int) bool {
		return merged[i].SourceLine < merged[j].SourceLine
	})
	return merged
}

// BulkImportOptions controls [BulkImport].
type BulkImportOptions struct {
	// ChunkSize is passed through to [Conn.CopyFromResilient].
	ChunkSize int

	// SkipHeaderLines is the number of leading lines (e.g. a CSV header) to
	// ignore before parsing data.
	SkipHeaderLines int
}

// BulkImport reads a supplier snapshot line by line, parses each line with
// parseRow, and loads the valid rows with [Conn.CopyFromResilient].
//
// Rows that fail to parse are collected in [BulkImportResult.ParseFailures];
// rows the database rejects are collected in Failures. Good rows are
// committed either way, so one bad record never discards the rest of the
// feed. The snapshot is read from disk, so re-running after a fix never
// re-fetches the supplier file.
func (c *Conn) BulkImport(
	ctx context.Context,
	snapshot *SourceSnapshot,
	tableName Identifier,
	columnNames []string,
	parseRow RowParser,
	opts *BulkImportOptions,
) (*BulkImportResult, error) {
	if snapshot == nil {
		return nil, errors.New("bulk import: nil source snapshot")
	}

	chunkSize := 0
	skipHeader := 0
	if opts != nil {
		chunkSize = opts.ChunkSize
		skipHeader = opts.SkipHeaderLines
	}

	f, err := snapshot.Open()
	if err != nil {
		return nil, fmt.Errorf("open snapshot: %w", err)
	}
	defer f.Close()

	result := &BulkImportResult{}
	var rows [][]any
	// sourceLineOfRow maps a row's position in the rows slice to its 1-based
	// line in the source file, so database failures can report source lines.
	sourceLineOfRow := make([]int, 0, 1024)

	scanner := newLineScanner(f)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		if lineNo <= skipHeader {
			continue
		}

		raw := scanner.Bytes()
		if len(raw) == 0 {
			continue
		}

		values, err := parseRow(lineNo, raw)
		if err != nil {
			result.TotalLines++
			result.ParseFailures = append(result.ParseFailures, RowFailure{
				SourceLine: lineNo,
				Err:        err,
			})
			continue
		}

		result.TotalLines++
		rows = append(rows, values)
		sourceLineOfRow = append(sourceLineOfRow, lineNo)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}

	copyResult, err := c.CopyFromResilient(ctx, tableName, columnNames, rows, &ResilientCopyOptions{
		ChunkSize: chunkSize,
	})
	if err != nil {
		return nil, err
	}

	result.ResilientCopyResult = *copyResult
	for i := range result.Failures {
		failure := &result.Failures[i]
		if failure.RowIndex >= 0 && failure.RowIndex < len(sourceLineOfRow) {
			failure.SourceLine = sourceLineOfRow[failure.RowIndex]
		}
	}

	return result, nil
}
