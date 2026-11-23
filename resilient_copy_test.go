package pgx_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestConnCopyFromResilientSkipsBadRowsAndKeepsGoodOnes(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table resilient_foo(
		a int4 primary key,
		b varchar not null
	)`)

	// Row 1 (index 1) violates NOT NULL, row 3 (index 3) violates the int4
	// type. The other three rows must still land.
	inputRows := [][]any{
		{int32(1), "one"},
		{int32(2), nil},
		{int32(3), "three"},
		{"not-an-int", "four"},
		{int32(5), "five"},
	}

	result, err := conn.CopyFromResilient(ctx, pgx.Identifier{"resilient_foo"}, []string{"a", "b"}, inputRows, &pgx.ResilientCopyOptions{ChunkSize: 2})
	require.NoError(t, err)
	require.Equal(t, int64(3), result.Inserted)
	require.Len(t, result.Failures, 2)

	// Failures must point at the exact bad rows.
	require.Equal(t, 1, result.Failures[0].RowIndex)
	require.Equal(t, 3, result.Failures[1].RowIndex)

	// The server-side rejection keeps its PgError; the client-side encoding
	// failure is reported directly with the column name.
	var pgErr *pgconn.PgError
	require.ErrorAs(t, result.Failures[0].Err, &pgErr)
	require.Contains(t, result.Failures[1].Err.Error(), `column "a"`)

	// And the good rows must actually be in the table, in order.
	rows, err := conn.Query(ctx, "select a, b from resilient_foo order by a")
	require.NoError(t, err)

	var got [][]any
	for rows.Next() {
		values, valuesErr := rows.Values()
		require.NoError(t, valuesErr)
		got = append(got, values)
	}
	require.NoError(t, rows.Err())

	require.Equal(t, [][]any{
		{int32(1), "one"},
		{int32(3), "three"},
		{int32(5), "five"},
	}, got)

	ensureConnValid(t, conn)
}

func TestConnCopyFromResilientAllGoodCommitsInChunks(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table resilient_all_good(a int4)`)

	var inputRows [][]any
	for i := 0; i < 2500; i++ {
		inputRows = append(inputRows, []any{int32(i)})
	}

	result, err := conn.CopyFromResilient(ctx, pgx.Identifier{"resilient_all_good"}, []string{"a"}, inputRows, &pgx.ResilientCopyOptions{ChunkSize: 500})
	require.NoError(t, err)
	require.Equal(t, int64(2500), result.Inserted)
	require.Empty(t, result.Failures)

	var count int64
	require.NoError(t, conn.QueryRow(ctx, "select count(*) from resilient_all_good").Scan(&count))
	require.Equal(t, int64(2500), count)

	ensureConnValid(t, conn)
}

func TestConnCopyFromResilientBadRowAtEveryBoundary(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table resilient_boundaries(a int4)`)

	// Every third row is bad; with a chunk size of 3 every chunk fails and
	// falls back to row-by-row isolation.
	var inputRows [][]any
	for i := 0; i < 9; i++ {
		if i%3 == 2 {
			inputRows = append(inputRows, []any{"bad"})
		} else {
			inputRows = append(inputRows, []any{int32(i)})
		}
	}

	result, err := conn.CopyFromResilient(ctx, pgx.Identifier{"resilient_boundaries"}, []string{"a"}, inputRows, &pgx.ResilientCopyOptions{ChunkSize: 3})
	require.NoError(t, err)
	require.Equal(t, int64(6), result.Inserted)
	require.Len(t, result.Failures, 3)
	require.Equal(t, []int{2, 5, 8}, []int{
		result.Failures[0].RowIndex,
		result.Failures[1].RowIndex,
		result.Failures[2].RowIndex,
	})

	ensureConnValid(t, conn)
}

func TestConnCopyFromResilientUniqueViolationIsRowFailure(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table resilient_unique(a int4 primary key)`)
	mustExec(t, conn, `insert into resilient_unique values (42)`)

	inputRows := [][]any{
		{int32(1)},
		{int32(42)}, // duplicate primary key
		{int32(3)},
	}

	result, err := conn.CopyFromResilient(ctx, pgx.Identifier{"resilient_unique"}, []string{"a"}, inputRows, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Inserted)
	require.Len(t, result.Failures, 1)
	require.Equal(t, 1, result.Failures[0].RowIndex)

	var pgErr *pgconn.PgError
	require.ErrorAs(t, result.Failures[0].Err, &pgErr)
	require.Equal(t, "23505", pgErr.Code)

	ensureConnValid(t, conn)
}

func TestConnCopyFromResilientEmptyAndMisshapenRows(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn := mustConnectString(t, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(t, conn)

	mustExec(t, conn, `create temporary table resilient_shape(a int4, b text)`)

	// No rows at all is a successful no-op without contacting COPY.
	empty, err := conn.CopyFromResilient(ctx, pgx.Identifier{"resilient_shape"}, []string{"a", "b"}, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), empty.Inserted)
	require.Empty(t, empty.Failures)

	// Rows with the wrong column count are rejected client-side and never
	// reach the server; well-formed rows around them still load.
	rows := [][]any{
		{int32(1), "one"},
		{int32(2)}, // missing column
		{int32(3), "three"},
	}
	result, err := conn.CopyFromResilient(ctx, pgx.Identifier{"resilient_shape"}, []string{"a", "b"}, rows, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Inserted)
	require.Len(t, result.Failures, 1)
	require.Equal(t, 1, result.Failures[0].RowIndex)
	require.Contains(t, result.Failures[0].Err.Error(), "expected 2 values")

	ensureConnValid(t, conn)
}
