package pgx_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// parseFeedLine parses "id,name" where id must be an int4 and name non-empty.
func parseFeedLine(sourceLine int, line []byte) ([]any, error) {
	parts := strings.SplitN(string(line), ",", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("line %d: expected id,name, got %q", sourceLine, string(line))
	}

	id, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("line %d: bad id %q: %w", sourceLine, parts[0], err)
	}
	if parts[1] == "" {
		return nil, fmt.Errorf("line %d: empty name", sourceLine)
	}

	return []any{int32(id), parts[1]}, nil
}

func TestBulkImportKeepsGoodRowsAndReportsBadSourceLines(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table supplier_feed(
		id int4 primary key,
		name varchar not null
	)`)
	mustExec(t, conn, `insert into supplier_feed values (2, 'already-there')`)

	// Line 2 has a bad id (parse failure), line 4 duplicates an existing
	// primary key (database failure). The two good data lines (3 and 5) are
	// still inserted.
	feed := strings.Join([]string{
		"id,name",
		"oops,parse-bad",
		"1,alpha",
		"2,duplicate",
		"3,gamma",
		"",
	}, "\n")

	var fetchCalls int32
	snapshotDir := t.TempDir()
	snapshot, err := pgx.FetchSource(ctx, snapshotDir, "supplier", "", func(context.Context) (io.ReadCloser, error) {
		atomic.AddInt32(&fetchCalls, 1)
		return io.NopCloser(strings.NewReader(feed)), nil
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&fetchCalls))

	result, err := conn.BulkImport(
		ctx,
		snapshot,
		pgx.Identifier{"supplier_feed"},
		[]string{"id", "name"},
		parseFeedLine,
		&pgx.BulkImportOptions{ChunkSize: 2, SkipHeaderLines: 1},
	)
	require.NoError(t, err)
	require.Equal(t, 4, result.TotalLines)
	require.Equal(t, int64(2), result.Inserted)
	require.Equal(t, []int{2, 4}, sourceLines(result.AllFailures()))

	require.Len(t, result.ParseFailures, 1)
	require.Equal(t, 2, result.ParseFailures[0].SourceLine)

	require.Len(t, result.Failures, 1)
	require.Equal(t, 4, result.Failures[0].SourceLine)
	require.Equal(t, 1, result.Failures[0].RowIndex)

	var count int64
	require.NoError(t, conn.QueryRow(ctx, "select count(*) from supplier_feed").Scan(&count))
	require.Equal(t, int64(3), count)

	var names []string
	rows, err := conn.Query(ctx, "select name from supplier_feed order by id")
	require.NoError(t, err)
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"alpha", "already-there", "gamma"}, names)

	ensureConnValid(t, conn)
}

func TestBulkImportRetryReusesSnapshotWithoutRefetch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table supplier_retry(
		id int4 primary key,
		name varchar not null
	)`)

	// Line 3 is the bad record the supplier keeps sending; it must not block
	// the rest, and re-running must read the cached file rather than fetch.
	feed := strings.Join([]string{
		"id,name",
		"1,alpha",
		"2,", // bad row: NOT NULL violation on every run
		"3,gamma",
	}, "\n")

	snapshotDir := filepath.Join(t.TempDir(), "snapshots")
	var fetchCalls int32
	fetch := func(context.Context) (io.ReadCloser, error) {
		atomic.AddInt32(&fetchCalls, 1)
		return io.NopCloser(strings.NewReader(feed)), nil
	}

	// First run: fetches the once-per-day supplier file.
	snapshot, err := pgx.FetchSource(ctx, snapshotDir, "daily", "", fetch)
	require.NoError(t, err)

	first, err := conn.BulkImport(ctx, snapshot, pgx.Identifier{"supplier_retry"}, []string{"id", "name"}, parseFeedLine, &pgx.BulkImportOptions{ChunkSize: 2, SkipHeaderLines: 1})
	require.NoError(t, err)
	require.Equal(t, int64(2), first.Inserted)
	require.Empty(t, first.Failures)

	// Simulate the operator fixing the bad line locally in the snapshot, then
	// re-running. A second FetchSource with the same content must not call the
	// supplier again and must resolve to the same cached file.
	snapshot2, err := pgx.FetchSource(ctx, snapshotDir, "daily", snapshot.Checksum, fetch)
	require.NoError(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&fetchCalls), "supplier must be fetched only once")
	require.Equal(t, snapshot.Path(), snapshot2.Path())
	require.Equal(t, snapshot.Checksum, snapshot2.Checksum)

	// Re-running against the same snapshot is idempotent for the good rows
	// (primary key conflict now marks them as bad rows instead of duplicating).
	second, err := conn.BulkImport(ctx, snapshot2, pgx.Identifier{"supplier_retry"}, []string{"id", "name"}, parseFeedLine, &pgx.BulkImportOptions{ChunkSize: 2, SkipHeaderLines: 1})
	require.NoError(t, err)
	require.Len(t, second.ParseFailures, 1)
	require.Equal(t, 3, second.ParseFailures[0].SourceLine)
	require.Len(t, second.Failures, 2)
	require.Equal(t, []int{2, 3, 4}, sourceLines(second.AllFailures()))

	var count int64
	require.NoError(t, conn.QueryRow(ctx, "select count(*) from supplier_retry").Scan(&count))
	require.Equal(t, int64(2), count)

	ensureConnValid(t, conn)
}

func TestFetchSourceDedupesIdenticalContent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	snapshotDir := t.TempDir()

	fetch := func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("same,bytes\n")), nil
	}

	first, err := pgx.FetchSource(ctx, snapshotDir, "feed", "", fetch)
	require.NoError(t, err)

	entries, err := os.ReadDir(snapshotDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	second, err := pgx.FetchSource(ctx, snapshotDir, "feed", first.Checksum, fetch)
	require.NoError(t, err)
	require.Equal(t, first.Path(), second.Path())

	entries, err = os.ReadDir(snapshotDir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "identical content must not create a second snapshot")
}

func sourceLines(failures []pgx.RowFailure) []int {
	lines := make([]int, len(failures))
	for i, failure := range failures {
		lines[i] = failure.SourceLine
	}
	return lines
}
