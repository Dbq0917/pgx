package pgx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// QueuedQuery is a query that has been queued for execution via a [Batch].
type QueuedQuery struct {
	SQL       string
	Arguments []any
	Fn        batchItemFunc
	sd        *pgconn.StatementDescription
	err       error
}

type batchItemFunc func(br BatchResults) error

// Query sets fn to be called when the response to qq is received.
func (qq *QueuedQuery) Query(fn func(rows Rows) error) {
	qq.Fn = func(br BatchResults) error {
		rows, _ := br.Query()
		defer rows.Close()

		err := fn(rows)
		if err != nil {
			return err
		}
		rows.Close()

		return rows.Err()
	}
}

// Query sets fn to be called when the response to qq is received.
func (qq *QueuedQuery) QueryRow(fn func(row Row) error) {
	qq.Fn = func(br BatchResults) error {
		row := br.QueryRow()
		return fn(row)
	}
}

// Exec sets fn to be called when the response to qq is received.
//
// Note: for simple batch insert uses where it is not required to handle
// each potential error individually, it's sufficient to not set any callbacks,
// and just handle the return value of [BatchResults.Close].
func (qq *QueuedQuery) Exec(fn func(ct pgconn.CommandTag) error) {
	qq.Fn = func(br BatchResults) error {
		ct, err := br.Exec()
		if err != nil {
			return err
		}

		return fn(ct)
	}
}

// Err returns the error attributed to qq by the most recent batch execution. It is nil if qq succeeded.
//
// If an earlier query in the batch failed, qq was not executed and Err returns an error where
// errors.Is(err, ErrBatchAborted) is true. The value is set as each result is read or when BatchResults.Close
// returns.
func (qq *QueuedQuery) Err() error {
	return qq.err
}

// Batch queries are a way of bundling multiple queries together to avoid
// unnecessary network round trips. A Batch must only be sent once.
type Batch struct {
	QueuedQueries []*QueuedQuery

	txOptions *TxOptions
}

// BeginTx queues the query that begins a transaction with txOptions as the first query of b, saving the round trip that
// [Conn.BeginTx] spends on it. It must be called before any query is queued and panics otherwise. Like any queued query,
// the begin query has a result: it is the first result of the batch. The transaction is available from
// [BatchResults.Tx] once b has been sent.
func (b *Batch) BeginTx(txOptions TxOptions) {
	if len(b.QueuedQueries) != 0 {
		panic("Batch.BeginTx must be called before any query is queued")
	}

	b.Queue(txOptions.beginSQL())
	b.txOptions = &txOptions
}

// Queue queues a query to batch b. query can be an SQL query or the name of a prepared statement. The only pgx option
// argument that is supported is [QueryRewriter]. Queries are executed using the connection's DefaultQueryExecMode
// (see [ConnConfig.DefaultQueryExecMode]).
//
// While query can contain multiple statements if the connection's DefaultQueryExecMode is [QueryExecModeSimpleProtocol],
// this should be avoided. QueuedQuery.Fn must not be set as it will only be called for the first query. That is,
// [QueuedQuery.Query], [QueuedQuery.QueryRow], and [QueuedQuery.Exec] must not be called. In addition, any error
// messages or tracing that include the current query may reference the wrong query.
func (b *Batch) Queue(query string, arguments ...any) *QueuedQuery {
	qq := &QueuedQuery{
		SQL:       query,
		Arguments: arguments,
	}
	b.QueuedQueries = append(b.QueuedQueries, qq)
	return qq
}

// Len returns number of queries that have been queued so far.
func (b *Batch) Len() int {
	return len(b.QueuedQueries)
}

type BatchResults interface {
	// Exec reads the results from the next query in the batch as if the query has been sent with [Conn.Exec]. Prefer
	// calling Exec on the QueuedQuery, or just calling Close.
	Exec() (pgconn.CommandTag, error)

	// Query reads the results from the next query in the batch as if the query has been sent with [Conn.Query]. Prefer
	// calling [QueuedQuery.Query].
	Query() (Rows, error)

	// QueryRow reads the results from the next query in the batch as if the query has been sent with [Conn.QueryRow].
	// Prefer calling [QueuedQuery.QueryRow].
	QueryRow() Row

	// Close closes the batch operation. All unread results are read and any callback functions registered with
	// [QueuedQuery.Query], [QueuedQuery.QueryRow], or [QueuedQuery.Exec] will be called. If a callback function returns an
	// error or the batch encounters an error subsequent callback functions will not be called.
	//
	// For simple batch inserts inside a transaction or similar queries, it's sufficient to not set any callbacks,
	// and just handle the return value of Close.
	//
	// If a query fails, its result reports the original error. Queries after it were not executed and report an error
	// where errors.Is(err, ErrBatchAborted) is true. The outcome of every queued query is also available from
	// QueuedQuery.Err after Close returns. This does not split the batch: it is still executed and rolled back
	// atomically.
	//
	// Close must be called before the underlying connection can be used again. Any error that occurred during a batch
	// operation may have made it impossible to resyncronize the connection with the server. In this case the underlying
	// connection will have been closed.
	//
	// Close is safe to call multiple times. If it returns an error subsequent calls will return the same error. Callback
	// functions will not be rerun.
	Close() error

	// Tx returns the transaction begun by [Batch.BeginTx], or nil if the batch did not begin one. It is available before
	// Close is called so that a Rollback can be deferred as usual, but the transaction must not otherwise be used until
	// Close has been called. If the batch fails before the transaction is begun, the transaction is closed by the time
	// Close returns, and its methods return an error where errors.Is(ErrTxClosed) is true.
	Tx() Tx
}

type batchResults struct {
	ctx       context.Context
	conn      *Conn
	mrr       *pgconn.MultiResultReader
	err       error // fatal error that prevents reading any more results
	queryErr  error // first error attributed to an individual query
	b         *Batch
	qqIdx     int
	closed    bool
	endTraced bool
}

// Exec reads the results from the next query in the batch as if the query has been sent with Exec.
func (br *batchResults) Exec() (pgconn.CommandTag, error) {
	if br.err != nil {
		return pgconn.CommandTag{}, br.err
	}
	if br.closed {
		return pgconn.CommandTag{}, fmt.Errorf("batch already closed")
	}

	if br.queryErr != nil {
		query, _, ok := br.nextQueryAndArgs()
		if !ok {
			return pgconn.CommandTag{}, errors.New("no more results in batch")
		}
		err := newErrBatchQueryAborted(br.b, br.qqIdx-1, query)
		br.b.QueuedQueries[br.qqIdx-1].err = err
		return pgconn.CommandTag{}, err
	}

	query, arguments, ok := br.nextQueryAndArgs()
	if !ok {
		return pgconn.CommandTag{}, errors.New("no more results in batch")
	}

	readStartTime := time.Now()

	if !br.mrr.NextResult() {
		err := br.mrr.Close()
		if err == nil {
			err = errors.New("no more results in batch")
		}
		br.recordQueryResult(br.qqIdx-1, err)
		if br.conn.batchTracer != nil {
			br.conn.batchTracer.TraceBatchQuery(br.ctx, br.conn, TraceBatchQueryData{
				SQL:           query,
				Args:          arguments,
				Err:           err,
				ReadStartTime: readStartTime,
			})
		}
		return pgconn.CommandTag{}, err
	}

	commandTag, err := br.mrr.ResultReader().Close()
	if err != nil {
		br.recordQueryResult(br.qqIdx-1, err)
		br.mrr.Close()
	}

	if br.conn.batchTracer != nil {
		br.conn.batchTracer.TraceBatchQuery(br.ctx, br.conn, TraceBatchQueryData{
			SQL:           query,
			Args:          arguments,
			CommandTag:    commandTag,
			Err:           err,
			ReadStartTime: readStartTime,
		})
	}

	return commandTag, err
}

// Query reads the results from the next query in the batch as if the query has been sent with Query.
func (br *batchResults) Query() (Rows, error) {
	if br.err != nil {
		return &baseRows{err: br.err, closed: true}, br.err
	}

	if br.closed {
		alreadyClosedErr := fmt.Errorf("batch already closed")
		return &baseRows{err: alreadyClosedErr, closed: true}, alreadyClosedErr
	}

	if br.queryErr != nil {
		query, _, ok := br.nextQueryAndArgs()
		if !ok {
			err := errors.New("no more results in batch")
			return &baseRows{err: err, closed: true}, err
		}
		err := newErrBatchQueryAborted(br.b, br.qqIdx-1, query)
		br.b.QueuedQueries[br.qqIdx-1].err = err
		return &baseRows{err: err, closed: true}, err
	}

	query, arguments, ok := br.nextQueryAndArgs()
	if !ok {
		query = "batch query"
	}

	rows := br.conn.getRows(br.ctx, query, arguments)
	rows.batchTracer = br.conn.batchTracer

	if !br.mrr.NextResult() {
		rows.err = br.mrr.Close()
		if rows.err == nil {
			rows.err = errors.New("no more results in batch")
		}
		rows.closed = true
		br.recordQueryResult(br.qqIdx-1, rows.err)

		if br.conn.batchTracer != nil {
			br.conn.batchTracer.TraceBatchQuery(br.ctx, br.conn, TraceBatchQueryData{
				SQL:           query,
				Args:          arguments,
				Err:           rows.err,
				ReadStartTime: rows.startTime,
			})
		}

		return rows, rows.err
	}

	rows.resultReader = br.mrr.ResultReader()
	return rows, nil
}

// QueryRow reads the results from the next query in the batch as if the query has been sent with QueryRow.
func (br *batchResults) QueryRow() Row {
	rows, _ := br.Query()
	return (*connRow)(rows.(*baseRows))
}

// Close closes the batch operation. Any error that occurred during a batch operation may have made it impossible to
// resyncronize the connection with the server. In this case the underlying connection will have been closed.
func (br *batchResults) Close() error {
	defer func() {
		if !br.endTraced {
			if br.conn != nil && br.conn.batchTracer != nil {
				br.conn.batchTracer.TraceBatchEnd(br.ctx, br.conn, TraceBatchEndData{Err: br.finalErr()})
			}
			br.endTraced = true
		}

		invalidateCachesOnBatchResultsError(br.conn, br.b, br.finalErr())
	}()

	if br.err != nil {
		return br.err
	}

	if br.closed {
		return br.finalErr()
	}

	// Read and run fn for all remaining items
	for br.err == nil && br.b != nil && br.qqIdx < len(br.b.QueuedQueries) {
		if br.queryErr != nil {
			// PostgreSQL discards all requests after an error until the next Sync. Record the error for every
			// remaining query without trying to read results that will never arrive.
			qqIdx := br.qqIdx
			br.qqIdx++
			br.b.QueuedQueries[qqIdx].err = newErrBatchQueryAborted(br.b, qqIdx, br.b.QueuedQueries[qqIdx].SQL)
		} else if br.b.QueuedQueries[br.qqIdx].Fn != nil {
			err := br.b.QueuedQueries[br.qqIdx].Fn(br)
			if err != nil {
				br.recordQueryResult(br.qqIdx-1, err)
			}
		} else {
			br.Exec()
		}
	}

	br.closed = true

	err := br.mrr.Close()
	if br.err == nil && br.queryErr == nil {
		br.err = err
	}

	return br.finalErr()
}

func (br *batchResults) Tx() Tx {
	return nil
}

func (br *batchResults) earlyError() error {
	return br.err
}

func (br *batchResults) finalErr() error {
	if br.err != nil {
		return br.err
	}
	return br.queryErr
}

func (br *batchResults) recordQueryResult(qqIdx int, err error) {
	if br.b != nil {
		if qqIdx >= 0 && qqIdx < len(br.b.QueuedQueries) {
			br.b.QueuedQueries[qqIdx].err = err
		}
	}

	if err == nil {
		return
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if br.queryErr == nil {
			br.queryErr = err
		}
	} else {
		if br.err == nil {
			br.err = err
		}
	}
}

func (br *batchResults) nextQueryAndArgs() (query string, args []any, ok bool) {
	if br.b != nil && br.qqIdx < len(br.b.QueuedQueries) {
		bi := br.b.QueuedQueries[br.qqIdx]
		query = bi.SQL
		args = bi.Arguments
		ok = true
		br.qqIdx++
	}
	return query, args, ok
}

type pipelineBatchResults struct {
	ctx         context.Context
	conn        *Conn
	pipeline    *pgconn.Pipeline
	lastRows    *baseRows
	lastRowsIdx int
	err         error // fatal error that prevents reading any more results
	queryErr    error // first error attributed to an individual query
	b           *Batch
	qqIdx       int
	closed      bool
	endTraced   bool
}

// Exec reads the results from the next query in the batch as if the query has been sent with Exec.
func (br *pipelineBatchResults) Exec() (pgconn.CommandTag, error) {
	if br.err != nil {
		return pgconn.CommandTag{}, br.err
	}
	if br.closed {
		return pgconn.CommandTag{}, fmt.Errorf("batch already closed")
	}

	br.finishRows()
	if br.err != nil {
		return pgconn.CommandTag{}, br.err
	}

	if br.queryErr != nil {
		query, _, err := br.nextQueryAndArgs()
		if err != nil {
			return pgconn.CommandTag{}, err
		}
		err = newErrBatchQueryAborted(br.b, br.qqIdx-1, query)
		br.b.QueuedQueries[br.qqIdx-1].err = err
		return pgconn.CommandTag{}, err
	}

	query, arguments, err := br.nextQueryAndArgs()
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	qqIdx := br.qqIdx - 1

	readStartTime := time.Now()

	results, err := br.pipeline.GetResults()
	if err != nil {
		br.recordQueryResult(qqIdx, err)
		if br.conn.batchTracer != nil {
			br.conn.batchTracer.TraceBatchQuery(br.ctx, br.conn, TraceBatchQueryData{
				SQL:           query,
				Args:          arguments,
				Err:           err,
				ReadStartTime: readStartTime,
			})
		}
		return pgconn.CommandTag{}, err
	}
	var commandTag pgconn.CommandTag
	switch results := results.(type) {
	case *pgconn.ResultReader:
		commandTag, err = results.Close()
	default:
		err = fmt.Errorf("unexpected pipeline result: %T", results)
	}
	br.recordQueryResult(qqIdx, err)

	if br.conn.batchTracer != nil {
		br.conn.batchTracer.TraceBatchQuery(br.ctx, br.conn, TraceBatchQueryData{
			SQL:           query,
			Args:          arguments,
			CommandTag:    commandTag,
			Err:           err,
			ReadStartTime: readStartTime,
		})
	}

	return commandTag, err
}

// Query reads the results from the next query in the batch as if the query has been sent with Query.
func (br *pipelineBatchResults) Query() (Rows, error) {
	if br.err != nil {
		return &baseRows{err: br.err, closed: true}, br.err
	}

	if br.closed {
		alreadyClosedErr := fmt.Errorf("batch already closed")
		return &baseRows{err: alreadyClosedErr, closed: true}, alreadyClosedErr
	}

	br.finishRows()
	if br.err != nil {
		return &baseRows{err: br.err, closed: true}, br.err
	}

	if br.queryErr != nil {
		query, _, err := br.nextQueryAndArgs()
		if err != nil {
			return &baseRows{err: err, closed: true}, err
		}
		err = newErrBatchQueryAborted(br.b, br.qqIdx-1, query)
		br.b.QueuedQueries[br.qqIdx-1].err = err
		return &baseRows{err: err, closed: true}, err
	}

	query, arguments, err := br.nextQueryAndArgs()
	if err != nil {
		return &baseRows{err: err, closed: true}, err
	}
	qqIdx := br.qqIdx - 1

	rows := br.conn.getRows(br.ctx, query, arguments)
	rows.batchTracer = br.conn.batchTracer
	br.lastRows = rows
	br.lastRowsIdx = qqIdx

	results, err := br.pipeline.GetResults()
	if err != nil {
		br.recordQueryResult(qqIdx, err)
		rows.err = err
		rows.closed = true

		if br.conn.batchTracer != nil {
			br.conn.batchTracer.TraceBatchQuery(br.ctx, br.conn, TraceBatchQueryData{
				SQL:           query,
				Args:          arguments,
				Err:           err,
				ReadStartTime: rows.startTime,
			})
		}
	} else {
		switch results := results.(type) {
		case *pgconn.ResultReader:
			rows.resultReader = results
		default:
			err = fmt.Errorf("unexpected pipeline result: %T", results)
			br.recordQueryResult(qqIdx, err)
			rows.err = err
			rows.closed = true

			if br.conn.batchTracer != nil {
				br.conn.batchTracer.TraceBatchQuery(br.ctx, br.conn, TraceBatchQueryData{
					SQL:           query,
					Args:          arguments,
					Err:           err,
					ReadStartTime: rows.startTime,
				})
			}
		}
	}

	return rows, rows.err
}

// QueryRow reads the results from the next query in the batch as if the query has been sent with QueryRow.
func (br *pipelineBatchResults) QueryRow() Row {
	rows, _ := br.Query()
	return (*connRow)(rows.(*baseRows))
}

// Close closes the batch operation. Any error that occurred during a batch operation may have made it impossible to
// resyncronize the connection with the server. In this case the underlying connection will have been closed.
func (br *pipelineBatchResults) Close() error {
	defer func() {
		if !br.endTraced {
			if br.conn.batchTracer != nil {
				br.conn.batchTracer.TraceBatchEnd(br.ctx, br.conn, TraceBatchEndData{Err: br.finalErr()})
			}
			br.endTraced = true
		}

		invalidateCachesOnBatchResultsError(br.conn, br.b, br.finalErr())
	}()

	if br.closed {
		return br.finalErr()
	}

	br.finishRows()

	// Read and run fn for all remaining items
	for br.err == nil && br.b != nil && br.qqIdx < len(br.b.QueuedQueries) {
		if br.queryErr != nil {
			// PostgreSQL discards all requests after an error until the next Sync. Record the error for every
			// remaining query without trying to read results that will never arrive.
			qqIdx := br.qqIdx
			br.qqIdx++
			br.b.QueuedQueries[qqIdx].err = newErrBatchQueryAborted(br.b, qqIdx, br.b.QueuedQueries[qqIdx].SQL)
		} else if br.b.QueuedQueries[br.qqIdx].Fn != nil {
			err := br.b.QueuedQueries[br.qqIdx].Fn(br)
			if err != nil {
				br.recordQueryResult(br.qqIdx-1, err)
			}
		} else {
			br.Exec()
		}
	}

	br.closed = true

	err := br.pipeline.Close()
	if br.err == nil && br.queryErr == nil {
		br.err = err
	}

	return br.finalErr()
}

func (br *pipelineBatchResults) Tx() Tx {
	return nil
}

func (br *pipelineBatchResults) earlyError() error {
	return br.err
}

func (br *pipelineBatchResults) finalErr() error {
	if br.err != nil {
		return br.err
	}
	return br.queryErr
}

func (br *pipelineBatchResults) finishRows() {
	if br.lastRows == nil {
		return
	}

	if !br.lastRows.closed {
		br.lastRows.Close()
	}
	br.recordQueryResult(br.lastRowsIdx, br.lastRows.err)
	br.lastRows = nil
}

func (br *pipelineBatchResults) recordQueryResult(qqIdx int, err error) {
	if br.b != nil {
		if qqIdx >= 0 && qqIdx < len(br.b.QueuedQueries) {
			br.b.QueuedQueries[qqIdx].err = err
		}
	}

	if err == nil {
		return
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if br.queryErr == nil {
			br.queryErr = err
		}
	} else {
		if br.err == nil {
			br.err = err
		}
	}
}

func (br *pipelineBatchResults) nextQueryAndArgs() (query string, args []any, err error) {
	if br.b == nil {
		return "", nil, errors.New("no reference to batch")
	}

	if br.qqIdx >= len(br.b.QueuedQueries) {
		return "", nil, errors.New("no more results in batch")
	}

	bi := br.b.QueuedQueries[br.qqIdx]
	br.qqIdx++
	return bi.SQL, bi.Arguments, nil
}

type emptyBatchResults struct {
	conn   *Conn
	closed bool
}

// Exec reads the results from the next query in the batch as if the query has been sent with Exec.
func (br *emptyBatchResults) Exec() (pgconn.CommandTag, error) {
	if br.closed {
		return pgconn.CommandTag{}, fmt.Errorf("batch already closed")
	}
	return pgconn.CommandTag{}, errors.New("no more results in batch")
}

// Query reads the results from the next query in the batch as if the query has been sent with Query.
func (br *emptyBatchResults) Query() (Rows, error) {
	if br.closed {
		alreadyClosedErr := fmt.Errorf("batch already closed")
		return &baseRows{err: alreadyClosedErr, closed: true}, alreadyClosedErr
	}

	rows := br.conn.getRows(context.Background(), "", nil)
	rows.err = errors.New("no more results in batch")
	rows.closed = true
	return rows, rows.err
}

// QueryRow reads the results from the next query in the batch as if the query has been sent with QueryRow.
func (br *emptyBatchResults) QueryRow() Row {
	rows, _ := br.Query()
	return (*connRow)(rows.(*baseRows))
}

// Close closes the batch operation. Any error that occurred during a batch operation may have made it impossible to
// resyncronize the connection with the server. In this case the underlying connection will have been closed.
func (br *emptyBatchResults) Close() error {
	br.closed = true
	return nil
}

func (br *emptyBatchResults) Tx() Tx {
	return nil
}

// txBatchResults are the results of a batch that begins a transaction with [Batch.BeginTx].
type txBatchResults struct {
	BatchResults
	tx     *dbTx
	closed bool
	err    error
}

func (br *txBatchResults) Tx() Tx {
	return br.tx
}

// Close closes the batch and, when that leaves the connection outside a transaction because the begin query failed or
// the batch was never sent, the transaction as well. Only the first call does this: once the transaction has been
// committed or rolled back the connection may already be in use elsewhere and must not be touched.
func (br *txBatchResults) Close() error {
	if br.closed {
		return br.err
	}
	br.closed = true

	br.err = br.BatchResults.Close()
	if br.tx.conn.PgConn().TxStatus() == 'I' {
		br.tx.closed = true
	}
	return br.err
}

// FailedBatchResults returns [BatchResults] for b that report err from every method, as if sending b had failed before
// anything reached the server. It is for code that wraps [Conn.SendBatch] and can fail before it has a connection to
// send b on, such as a connection pool. If b began a transaction with [Batch.BeginTx], Tx returns a closed transaction.
func FailedBatchResults(b *Batch, err error) BatchResults {
	br := &batchResults{err: err}
	if b.txOptions == nil {
		return br
	}

	return &txBatchResults{BatchResults: br, tx: &dbTx{closed: true}, closed: true, err: err}
}

// invalidates statement and description caches on batch results error
func invalidateCachesOnBatchResultsError(conn *Conn, b *Batch, err error) {
	if err != nil && conn != nil && b != nil {
		if sc := conn.statementCache; sc != nil {
			for _, bi := range b.QueuedQueries {
				sc.Invalidate(bi.SQL)
			}
		}

		if sc := conn.descriptionCache; sc != nil {
			for _, bi := range b.QueuedQueries {
				sc.Invalidate(bi.SQL)
			}
		}
	}
}

// ErrBatchAborted is returned for a query that was not executed because an earlier query in the same batch failed.
// PostgreSQL discards queued queries after an error until the next synchronization point, so their individual database
// errors are not available.
var ErrBatchAborted = errors.New("batch query was not executed because an earlier query failed")

// BatchQueryAbortedError identifies a query that was not executed because an earlier query in the same batch failed.
type BatchQueryAbortedError struct {
	// Index is the zero-based position of the query in the batch.
	Index int

	// SQL is the query that was not executed.
	SQL string
}

func newErrBatchQueryAborted(b *Batch, qqIdx int, sql string) error {
	if b != nil && qqIdx >= 0 && qqIdx < len(b.QueuedQueries) {
		sql = b.QueuedQueries[qqIdx].SQL
	}

	return &BatchQueryAbortedError{Index: qqIdx, SQL: sql}
}

func (e *BatchQueryAbortedError) Error() string {
	return fmt.Sprintf("batch query %d was not executed because an earlier query failed", e.Index)
}

func (e *BatchQueryAbortedError) Unwrap() error {
	return ErrBatchAborted
}

func (e *BatchQueryAbortedError) Is(target error) bool {
	return target == ErrBatchAborted
}

// ErrPreprocessingBatch occurs when an error is encountered while preprocessing a batch.
// The two preprocessing steps are "prepare" (server-side SQL parse/plan) and
// "build" (client-side argument encoding).
type ErrPreprocessingBatch struct {
	step string // "prepare" or "build"
	sql  string
	err  error
}

func newErrPreprocessingBatch(step, sql string, err error) ErrPreprocessingBatch {
	return ErrPreprocessingBatch{step: step, sql: sql, err: err}
}

func (e ErrPreprocessingBatch) Error() string {
	// intentionally not including the SQL query in the error message
	// to avoid leaking potentially sensitive information into logs.
	// If the user wants the SQL, they can call SQL().
	return fmt.Sprintf("error preprocessing batch (%s): %v", e.step, e.err)
}

func (e ErrPreprocessingBatch) Unwrap() error {
	return e.err
}

func (e ErrPreprocessingBatch) SQL() string {
	return e.sql
}
