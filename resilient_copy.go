package pgx

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgconn"
)

// RowFailure records a single source row that could not be inserted together
// with the reason. SourceLine is the 1-based position of the row in the
// original source (0 when the caller does not track source positions).
type RowFailure struct {
	// SourceLine is the 1-based position of the rejected row in the original
	// source data. It is 0 when the source does not provide positions.
	SourceLine int

	// RowIndex is the 0-based position of the rejected row relative to the
	// rows passed to CopyFromResilient.
	RowIndex int

	// Values are the values of the rejected row.
	Values []any

	// Err is the error reported for the row.
	Err error
}

// ResilientCopyResult summarizes a resilient bulk copy. Rows that are valid
// are committed; invalid rows are reported in Failures instead of aborting
// the whole import.
type ResilientCopyResult struct {
	// Inserted is the number of rows successfully copied.
	Inserted int64

	// Failures holds the rows rejected by the server or by the source.
	Failures []RowFailure
}

// ResilientCopyOptions controls resilient copy behavior.
type ResilientCopyOptions struct {
	// ChunkSize is the maximum number of rows attempted in one COPY
	// transaction. Larger chunks are faster but re-process more rows when a
	// bad row is encountered. Defaults to 1000 when zero.
	ChunkSize int
}

// CopyFromResilient copies rows using the COPY protocol while tolerating bad
// rows. Unlike [Conn.CopyFrom], which is all-or-nothing, rows are copied in
// chunks inside independent transactions. A chunk containing a bad row is
// rolled back and retried row by row, so every valid row is committed and
// every invalid row is returned in [ResilientCopyResult.Failures] with its
// source position and the underlying error.
//
// Rows are first validated client-side against the target column types: a
// value that cannot be encoded is rejected without ever touching the server.
// The remaining rows are copied in chunks inside independent transactions; a
// chunk rejected by the server (e.g. a NOT NULL or unique violation) is
// rolled back and retried row by row. Only server-side data errors (SQLSTATE
// class 22 and 23, plus a handful of related format errors) are treated as bad
// rows. Connection failures, context cancellation and other operational
// errors abort the import and are returned to the caller because retrying
// them cannot isolate a row.
func (c *Conn) CopyFromResilient(
	ctx context.Context,
	tableName Identifier,
	columnNames []string,
	rows [][]any,
	opts *ResilientCopyOptions,
) (*ResilientCopyResult, error) {
	chunkSize := 0
	if opts != nil {
		chunkSize = opts.ChunkSize
	}
	if chunkSize <= 0 {
		chunkSize = 1000
	}

	sd, err := c.describeCopyTarget(ctx, tableName, columnNames)
	if err != nil {
		return nil, err
	}

	result := &ResilientCopyResult{}
	validRows := make([][]any, 0, len(rows))
	// validRowIndex maps a position in validRows back to the caller's row index
	// so failures detected later keep their original positions.
	validRowIndex := make([]int, 0, len(rows))

	for rowIndex, row := range rows {
		if failure, ok := c.validateRow(sd, rowIndex, row, columnNames); !ok {
			result.Failures = append(result.Failures, failure)
			continue
		}
		validRows = append(validRows, row)
		validRowIndex = append(validRowIndex, rowIndex)
	}

	for start := 0; start < len(validRows); start += chunkSize {
		end := start + chunkSize
		if end > len(validRows) {
			end = len(validRows)
		}

		n, err := c.copyChunk(ctx, tableName, columnNames, validRows[start:end], validRowIndex[start:end], result)
		result.Inserted += n
		if err != nil {
			return result, err
		}
	}

	// Failures are collected in two phases (client-side validation, then
	// server-side rejection). Re-sort by row position so the report reads in
	// source order regardless of where a row was rejected.
	sort.SliceStable(result.Failures, func(i, j int) bool {
		return result.Failures[i].RowIndex < result.Failures[j].RowIndex
	})

	return result, nil
}

// describeCopyTarget resolves the OID of every target column using the same
// describe path Conn.CopyFrom relies on, so validation and the actual COPY
// agree on the target types.
func (c *Conn) describeCopyTarget(ctx context.Context, tableName Identifier, columnNames []string) (*pgconn.StatementDescription, error) {
	quotedTableName := tableName.Sanitize()
	cbuf := make([]byte, 0, len(columnNames)*16)
	for i, columnName := range columnNames {
		if i != 0 {
			cbuf = append(cbuf, ',', ' ')
		}
		cbuf = append(cbuf, quoteIdentifier(columnName)...)
	}

	mode := c.config.DefaultQueryExecMode
	if mode == QueryExecModeExec || mode == QueryExecModeSimpleProtocol {
		mode = QueryExecModeDescribeExec
	}

	sd, err := c.getStatementDescription(ctx, mode, fmt.Sprintf("select %s from %s", string(cbuf), quotedTableName))
	if err != nil {
		return nil, fmt.Errorf("statement description failed: %w", err)
	}
	return sd, nil
}

// validateRow checks the shape of a row and whether every value can be
// encoded in the binary format the COPY protocol requires. A row that fails
// validation is returned as a RowFailure and excluded from the COPY.
func (c *Conn) validateRow(sd *pgconn.StatementDescription, rowIndex int, row []any, columnNames []string) (RowFailure, bool) {
	if len(row) != len(columnNames) {
		return RowFailure{
			RowIndex: rowIndex,
			Values:   row,
			Err:      fmt.Errorf("expected %d values, got %d values", len(columnNames), len(row)),
		}, false
	}

	for i, value := range row {
		if _, err := c.typeMap.Encode(sd.Fields[i].DataTypeOID, BinaryFormatCode, value, nil); err != nil {
			return RowFailure{
				RowIndex: rowIndex,
				Values:   row,
				Err:      fmt.Errorf("column %q: %w", columnNames[i], err),
			}, false
		}
	}

	return RowFailure{}, true
}

// copyChunk attempts one chunk. On a data error it falls back to row by row
// insertion so valid rows in the chunk are still committed. It returns the
// number of inserted rows and a non-data error that must abort the import.
func (c *Conn) copyChunk(
	ctx context.Context,
	tableName Identifier,
	columnNames []string,
	chunk [][]any,
	rowIndexes []int,
	result *ResilientCopyResult,
) (int64, error) {
	tx, err := c.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin chunk transaction: %w", err)
	}

	n, err := tx.CopyFrom(ctx, tableName, columnNames, CopyFromRows(chunk))
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit chunk: %w", err)
		}
		return n, nil
	}

	// The COPY failed: the whole chunk transaction is poisoned. Roll it back
	// before deciding whether the error is row-isolatable.
	if rbErr := tx.Rollback(ctx); rbErr != nil {
		return 0, fmt.Errorf("rollback failed chunk: %w (copy error: %v)", rbErr, err)
	}

	if !isRowDataError(err) {
		return 0, fmt.Errorf("copy chunk starting at row %d: %w", rowIndexes[0], err)
	}

	return c.isolateBadRows(ctx, tableName, columnNames, chunk, rowIndexes, result)
}

// isolateBadRows retries the rows of a failed chunk one at a time, each in
// its own transaction. Good rows commit, bad rows are appended to the result.
func (c *Conn) isolateBadRows(
	ctx context.Context,
	tableName Identifier,
	columnNames []string,
	chunk [][]any,
	rowIndexes []int,
	result *ResilientCopyResult,
) (int64, error) {
	var inserted int64

	for offset, row := range chunk {
		rowIndex := rowIndexes[offset]

		tx, err := c.Begin(ctx)
		if err != nil {
			return inserted, fmt.Errorf("begin row transaction: %w", err)
		}

		_, err = tx.CopyFrom(ctx, tableName, columnNames, CopyFromRows([][]any{row}))
		if err == nil {
			if err := tx.Commit(ctx); err != nil {
				return inserted, fmt.Errorf("commit row %d: %w", rowIndex, err)
			}
			inserted++
			continue
		}

		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return inserted, fmt.Errorf("rollback failed row %d: %w (copy error: %v)", rowIndex, rbErr, err)
		}

		if !isRowDataError(err) {
			return inserted, fmt.Errorf("copy row %d: %w", rowIndex, err)
		}

		result.Failures = append(result.Failures, RowFailure{
			RowIndex: rowIndex,
			Values:   row,
			Err:      err,
		})
	}

	return inserted, nil
}

// isRowDataError reports whether err is a server-side data error that can be
// attributed to an individual row and retried in isolation. Connection,
// serialization and operational errors are not row errors and must abort.
func isRowDataError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}

	// Class 22: data exception (bad type, bad value, overflow, ...).
	// Class 23: integrity constraint violation (not-null, unique, FK, check).
	// Class 42 only covers malformed definitions, which are not row data; it is
	// intentionally excluded.
	switch pgErr.Code[:2] {
	case "22", "23":
		return true
	}

	// A handful of non-classed errors that still describe a specific bad row.
	switch pgErr.Code {
	case "0A000", // feature_not_supported, e.g. unsupported value encoding
		"21000", // cardinal_violation from malformed row shape
		"42804", // datatype_mismatch
		"42883", // undefined_function hit while coercing a value
		"54000": // program_limit_exceeded, e.g. field too large
		return true
	}

	return false
}
