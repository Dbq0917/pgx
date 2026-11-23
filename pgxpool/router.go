package pgxpool

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DefaultStickyWriteWindow is the default [RouterConfig.StickyWriteWindow].
const DefaultStickyWriteWindow = 5 * time.Second

// RouterConfig is the configuration for creating a [Router] with [NewRouter].
type RouterConfig struct {
	// Primary is the pool configuration for the primary (read-write) server. It must have been created by
	// [ParseConfig]. Required.
	Primary *Config

	// Replicas are the pool configurations for the read-only replica servers. Each config must have been created by
	// [ParseConfig]. Read queries are distributed over the replicas in round-robin order. If empty, all queries are
	// served by the primary.
	Replicas []*Config

	// StickyWriteWindow is how long after a write read queries keep being routed to the primary. This prevents a read
	// that immediately follows a write from being sent to a replica that has not replayed the write yet and observing
	// a stale value. If zero, [DefaultStickyWriteWindow] is used. If negative, stickiness is disabled and reads are
	// sent to replicas even immediately after a write.
	StickyWriteWindow time.Duration

	// IsReadOnlyQuery reports whether the given SQL only reads data and therefore may be executed on a replica. If
	// nil, a conservative default is used: only single statements beginning with SELECT, TABLE, VALUES, or SHOW that
	// do not contain a locking clause such as FOR UPDATE are considered read-only. Everything else, including CTEs
	// (WITH ...) and multiple statements, is executed on the primary.
	//
	// Note that no SQL inspection can detect writes performed by called functions (e.g. SELECT nextval('seq') or
	// SELECT a_writing_function()). Such queries must be sent to the primary explicitly, e.g. via
	// [Router.Primary].
	IsReadOnlyQuery func(sql string) bool
}

// Router routes queries between a primary server pool and read-only replica pools, providing read/write splitting.
//
// Writes (Exec, SendBatch, CopyFrom, transactions, and queries not classified as read-only) always go to the primary.
// Read-only queries go to one of the replicas in round-robin order, except during the sticky write window after a
// write, when they go to the primary to guarantee read-your-writes consistency.
//
// Router implements a similar interface to [Pool].
type Router struct {
	primary  *Pool
	replicas []*Pool

	stickyWriteWindow time.Duration
	isReadOnlyQuery   func(sql string) bool

	nextReplica       atomic.Uint64
	lastWriteUnixNano atomic.Int64
}

// NewRouter creates a new [Router]. config.Primary is required. Connections to satisfy MinConns and MinIdleConns of
// each pool are established in the background, see [NewWithConfig].
func NewRouter(ctx context.Context, config *RouterConfig) (*Router, error) {
	if config == nil || config.Primary == nil {
		return nil, errors.New("pgxpool: RouterConfig.Primary is required")
	}

	primary, err := NewWithConfig(ctx, config.Primary)
	if err != nil {
		return nil, err
	}

	replicas := make([]*Pool, 0, len(config.Replicas))
	for _, replicaConfig := range config.Replicas {
		if replicaConfig == nil {
			primary.Close()
			return nil, errors.New("pgxpool: RouterConfig.Replicas must not contain nil")
		}
		replica, err := NewWithConfig(ctx, replicaConfig)
		if err != nil {
			primary.Close()
			for _, r := range replicas {
				r.Close()
			}
			return nil, err
		}
		replicas = append(replicas, replica)
	}

	stickyWriteWindow := config.StickyWriteWindow
	if stickyWriteWindow == 0 {
		stickyWriteWindow = DefaultStickyWriteWindow
	}

	isReadOnlyQuery := config.IsReadOnlyQuery
	if isReadOnlyQuery == nil {
		isReadOnlyQuery = defaultIsReadOnlyQuery
	}

	return &Router{
		primary:           primary,
		replicas:          replicas,
		stickyWriteWindow: stickyWriteWindow,
		isReadOnlyQuery:   isReadOnlyQuery,
	}, nil
}

// Primary returns the pool connected to the primary (read-write) server.
func (r *Router) Primary() *Pool { return r.primary }

// Replicas returns the pools connected to the read-only replica servers.
func (r *Router) Replicas() []*Pool {
	replicas := make([]*Pool, len(r.replicas))
	copy(replicas, r.replicas)
	return replicas
}

// Close closes the primary and all replica pools.
func (r *Router) Close() {
	r.primary.Close()
	for _, replica := range r.replicas {
		replica.Close()
	}
}

// Ping pings the primary and all replicas. The first error encountered is returned.
func (r *Router) Ping(ctx context.Context) error {
	if err := r.primary.Ping(ctx); err != nil {
		return err
	}
	for _, replica := range r.replicas {
		if err := replica.Ping(ctx); err != nil {
			return err
		}
	}
	return nil
}

// recordWrite marks the current time as the last write. Reads are routed to the primary until
// [RouterConfig.StickyWriteWindow] has elapsed. It is called before the write is executed so that a concurrent read
// can never be routed to a replica that is missing a completed write.
func (r *Router) recordWrite() {
	r.lastWriteUnixNano.Store(time.Now().UnixNano())
}

// readPool returns the pool a read-only query should be executed on.
func (r *Router) readPool() *Pool {
	if len(r.replicas) == 0 {
		return r.primary
	}
	if r.stickyWriteWindow > 0 {
		if lastWrite := r.lastWriteUnixNano.Load(); lastWrite != 0 &&
			time.Since(time.Unix(0, lastWrite)) < r.stickyWriteWindow {
			return r.primary
		}
	}
	i := r.nextReplica.Add(1) - 1
	return r.replicas[i%uint64(len(r.replicas))]
}

// queryPool returns the pool the given SQL of a Query or QueryRow call should be executed on. SQL that is not
// read-only is recorded as a write and routed to the primary.
func (r *Router) queryPool(sql string) *Pool {
	if r.isReadOnlyQuery(sql) {
		return r.readPool()
	}
	r.recordWrite()
	return r.primary
}

// Exec executes SQL on the primary. See [Pool.Exec].
func (r *Router) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	r.recordWrite()
	return r.primary.Exec(ctx, sql, arguments...)
}

// Query executes a query that returns [pgx.Rows]. Read-only queries are executed on a replica unless they occur
// within the sticky write window after a write. All other queries are executed on the primary. See [Pool.Query].
func (r *Router) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return r.queryPool(sql).Query(ctx, sql, args...)
}

// QueryRow executes a query that is expected to return at most one row. It is routed like [Router.Query]. See
// [Pool.QueryRow].
func (r *Router) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return r.queryPool(sql).QueryRow(ctx, sql, args...)
}

// SendBatch sends a batch on the primary. See [Pool.SendBatch].
func (r *Router) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	r.recordWrite()
	return r.primary.SendBatch(ctx, b)
}

// Begin starts a transaction on the primary. See [Pool.Begin].
func (r *Router) Begin(ctx context.Context) (pgx.Tx, error) {
	return r.BeginTx(ctx, pgx.TxOptions{})
}

// BeginTx starts a transaction on the primary with the given transaction mode. A successful commit is recorded as a
// write. See [Pool.BeginTx].
func (r *Router) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	tx, err := r.primary.BeginTx(ctx, txOptions)
	if err != nil {
		return nil, err
	}
	return &routerTx{Tx: tx, router: r}, nil
}

// CopyFrom copies rows on the primary. See [Pool.CopyFrom].
func (r *Router) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	r.recordWrite()
	return r.primary.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

// routerTx records a write on the router when the transaction is committed.
type routerTx struct {
	pgx.Tx
	router *Router
}

func (tx *routerTx) Commit(ctx context.Context) error {
	err := tx.Tx.Commit(ctx)
	if err == nil {
		tx.router.recordWrite()
	}
	return err
}

// defaultIsReadOnlyQuery is the default [RouterConfig.IsReadOnlyQuery]. It reports true only for single statements
// beginning with SELECT, TABLE, VALUES, or SHOW that do not contain a locking clause (FOR UPDATE, FOR NO KEY UPDATE,
// FOR SHARE, FOR KEY SHARE). Leading and embedded whitespace, comments, string literals, and quoted identifiers are
// skipped during the scan.
func defaultIsReadOnlyQuery(sql string) bool {
	firstWord, hasLockClause, hasMultipleStatements := scanSQLStatement(sql)
	if hasLockClause || hasMultipleStatements {
		return false
	}
	switch firstWord {
	case "select", "table", "values", "show":
		return true
	default:
		return false
	}
}

// scanSQLStatement scans sql and returns the first keyword, whether a locking clause (FOR UPDATE / FOR SHARE and
// variants) is present, and whether more than one statement is present.
func scanSQLStatement(sql string) (firstWord string, hasLockClause, hasMultipleStatements bool) {
	previousWord := ""
	wordCount := 0

	i := 0
	for i < len(sql) {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f':
			i++
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			i += 2
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			i = skipBlockComment(sql, i+2)
		case c == '\'':
			i = skipQuoted(sql, i, '\'')
		case c == '"':
			i = skipQuoted(sql, i, '"')
		case c == '$':
			i = skipDollarQuoted(sql, i)
		case c == ';':
			if hasMoreStatements(sql[i+1:]) {
				hasMultipleStatements = true
			}
			previousWord = ""
			i++
		case isSQLWordStart(c):
			j := i + 1
			for j < len(sql) && isSQLWordPart(sql[j]) {
				j++
			}
			word := lowerASCII(sql[i:j])
			if wordCount == 0 {
				firstWord = word
			}
			wordCount++
			if previousWord == "for" && (word == "update" || word == "share" || word == "no" || word == "key") {
				hasLockClause = true
			}
			previousWord = word
			i = j
		default:
			previousWord = ""
			i++
		}
	}

	return firstWord, hasLockClause, hasMultipleStatements
}

// skipBlockComment returns the index just past the end of the block comment starting at start (the index just after
// the opening /*). Nested block comments are supported. Unterminated comments consume the rest of sql.
func skipBlockComment(sql string, start int) int {
	depth := 1
	i := start
	for i < len(sql) && depth > 0 {
		if i+1 < len(sql) && sql[i] == '/' && sql[i+1] == '*' {
			depth++
			i += 2
		} else if i+1 < len(sql) && sql[i] == '*' && sql[i+1] == '/' {
			depth--
			i += 2
		} else {
			i++
		}
	}
	return i
}

// skipQuoted returns the index just past the quoted string or identifier starting at start (the index of the opening
// quote). The quote character is escaped by doubling it. Unterminated quotes consume the rest of sql.
func skipQuoted(sql string, start int, quote byte) int {
	i := start + 1
	for i < len(sql) {
		if sql[i] == quote {
			if i+1 < len(sql) && sql[i+1] == quote {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return i
}

// skipDollarQuoted returns the index just past the dollar-quoted string starting at start (the index of the opening
// $). If start does not begin a valid dollar quote tag, start+1 is returned. Unterminated strings consume the rest of
// sql.
func skipDollarQuoted(sql string, start int) int {
	i := start + 1
	for i < len(sql) && (isSQLWordPart(sql[i])) {
		i++
	}
	if i >= len(sql) || sql[i] != '$' {
		return start + 1
	}
	tag := sql[start : i+1]
	i++
	for i < len(sql) {
		if sql[i] == '$' && len(sql)-i >= len(tag) && sql[i:i+len(tag)] == tag {
			return i + len(tag)
		}
		i++
	}
	return i
}

// hasMoreStatements reports whether sql contains anything but whitespace and comments.
func hasMoreStatements(sql string) bool {
	i := 0
	for i < len(sql) {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f' || c == ';':
			i++
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			i += 2
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			i = skipBlockComment(sql, i+2)
		default:
			return true
		}
	}
	return false
}

func isSQLWordStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isSQLWordPart(c byte) bool {
	return isSQLWordStart(c) || (c >= '0' && c <= '9')
}

func lowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}
