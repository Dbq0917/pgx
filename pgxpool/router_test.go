package pgxpool_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

var routerTestTableCounter atomic.Int64

// newRouterTestPoolConfig returns a pool config for the test database whose connections carry the given
// application_name, so tests can tell which pool served a query.
func newRouterTestPoolConfig(t *testing.T, applicationName string) *pgxpool.Config {
	t.Helper()
	config, err := pgxpool.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	config.ConnConfig.RuntimeParams["application_name"] = applicationName
	return config
}

func newRouterTestRouter(t *testing.T, stickyWriteWindow time.Duration, replicaNames ...string) *pgxpool.Router {
	t.Helper()
	config := &pgxpool.RouterConfig{
		Primary:           newRouterTestPoolConfig(t, "pgxpool_router_primary"),
		StickyWriteWindow: stickyWriteWindow,
	}
	for _, name := range replicaNames {
		config.Replicas = append(config.Replicas, newRouterTestPoolConfig(t, name))
	}
	router, err := pgxpool.NewRouter(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(router.Close)
	return router
}

// queryApplicationName returns the application_name of the connection that served the query.
func queryApplicationName(t *testing.T, ctx context.Context, router *pgxpool.Router) string {
	t.Helper()
	var name string
	require.NoError(t, router.QueryRow(ctx, "select current_setting('application_name')").Scan(&name))
	return name
}

func newRouterTestTable(t *testing.T, ctx context.Context, router *pgxpool.Router) string {
	t.Helper()
	table := fmt.Sprintf("pgxpool_router_test_%d_%d", time.Now().UnixNano(), routerTestTableCounter.Add(1))
	_, err := router.Exec(ctx, "create table "+table+" (id serial primary key, v text not null)")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = router.Primary().Exec(context.Background(), "drop table if exists "+table)
	})
	return table
}

func TestRouterRequiresPrimary(t *testing.T) {
	t.Parallel()

	_, err := pgxpool.NewRouter(context.Background(), &pgxpool.RouterConfig{})
	require.Error(t, err)

	_, err = pgxpool.NewRouter(context.Background(), nil)
	require.Error(t, err)
}

func TestRouterReadGoesToReplica(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	router := newRouterTestRouter(t, time.Minute, "pgxpool_router_replica")

	require.Equal(t, "pgxpool_router_replica", queryApplicationName(t, ctx, router))
}

func TestRouterReadGoesToPrimaryWithoutReplicas(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	router := newRouterTestRouter(t, time.Minute)

	require.Equal(t, "pgxpool_router_primary", queryApplicationName(t, ctx, router))
}

func TestRouterWriteThenReadSticksToPrimary(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	router := newRouterTestRouter(t, time.Minute, "pgxpool_router_replica")
	table := newRouterTestTable(t, ctx, router)

	_, err := router.Exec(ctx, "insert into "+table+" (v) values ($1)", "new-value")
	require.NoError(t, err)

	// A read immediately after a write must be served by the primary and observe the new value, not a stale one.
	require.Equal(t, "pgxpool_router_primary", queryApplicationName(t, ctx, router))

	var v string
	require.NoError(t, router.QueryRow(ctx, "select v from "+table+" where id = 1").Scan(&v))
	require.Equal(t, "new-value", v)
}

func TestRouterReadLeavesPrimaryAfterStickyWriteWindow(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	router := newRouterTestRouter(t, 50*time.Millisecond, "pgxpool_router_replica")
	table := newRouterTestTable(t, ctx, router)

	_, err := router.Exec(ctx, "insert into "+table+" (v) values ($1)", "new-value")
	require.NoError(t, err)

	require.Equal(t, "pgxpool_router_primary", queryApplicationName(t, ctx, router))

	time.Sleep(150 * time.Millisecond)

	require.Equal(t, "pgxpool_router_replica", queryApplicationName(t, ctx, router))
}

func TestRouterNegativeStickyWriteWindowDisablesStickiness(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	router := newRouterTestRouter(t, -1, "pgxpool_router_replica")
	table := newRouterTestTable(t, ctx, router)

	_, err := router.Exec(ctx, "insert into "+table+" (v) values ($1)", "new-value")
	require.NoError(t, err)

	require.Equal(t, "pgxpool_router_replica", queryApplicationName(t, ctx, router))
}

func TestRouterReadsAreRoundRobinAcrossReplicas(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	router := newRouterTestRouter(t, time.Minute, "pgxpool_router_replica_1", "pgxpool_router_replica_2", "pgxpool_router_replica_3")

	seen := map[string]int{}
	for i := 0; i < 6; i++ {
		seen[queryApplicationName(t, ctx, router)]++
	}
	require.Equal(t, map[string]int{
		"pgxpool_router_replica_1": 2,
		"pgxpool_router_replica_2": 2,
		"pgxpool_router_replica_3": 2,
	}, seen)
}

func TestRouterWriteStatementsGoToPrimary(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	router := newRouterTestRouter(t, time.Minute, "pgxpool_router_replica")
	table := newRouterTestTable(t, ctx, router)

	// Let the sticky window from the CREATE TABLE expire so only the statements under test influence routing.
	router2 := newRouterTestRouter(t, time.Minute, "pgxpool_router_replica")

	tests := []struct {
		name string
		sql  string
		args []any
	}{
		{name: "InsertReturning", sql: "insert into " + table + " (v) values ('a') returning v"},
		{name: "Update", sql: "update " + table + " set v = 'b' where false returning v"},
		{name: "Delete", sql: "delete from " + table + " where false returning v"},
		{name: "SelectForUpdate", sql: "select v from " + table + " for update"},
		{name: "SelectForShare", sql: "select v from " + table + " for share"},
		{name: "SelectForNoKeyUpdate", sql: "select v from " + table + " for no key update"},
		{name: "SelectForKeyShare", sql: "select v from " + table + " for key share"},
		{name: "CTE", sql: "with x as (select 1) select 1"},
		{name: "MultipleStatements", sql: "select 1; select 2", args: []any{pgx.QueryExecModeSimpleProtocol}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := router2.Query(ctx, tt.sql, tt.args...)
			require.NoError(t, err)
			rows.Close()
			require.NoError(t, rows.Err())

			// A write statement records a write, so the following read sticks to the primary.
			require.Equal(t, "pgxpool_router_primary", queryApplicationName(t, ctx, router2))
		})
	}
}

func TestRouterReadStatementsGoToReplica(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	router := newRouterTestRouter(t, time.Minute, "pgxpool_router_replica")

	tests := []struct {
		name string
		sql  string
	}{
		{"Select", "select current_setting('application_name')"},
		{"SelectWithLeadingComment", "/* hello */ -- world\n select current_setting('application_name')"},
		{"SelectWithStringContainingKeywords", "select 'for update', current_setting('application_name')"},
		{"SelectWithQuotedIdentifier", `select current_setting('application_name') as "for update"`},
		{"SelectWithDollarQuotedString", "select $tag$insert into t$tag$, current_setting('application_name')"},
		{"Show", "show application_name"},
		{"Values", "values (current_setting('application_name'))"},
		{"TrailingSemicolons", "select current_setting('application_name'); ; -- done\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var name string
			var scanErr error
			if tt.name == "SelectWithStringContainingKeywords" || tt.name == "SelectWithDollarQuotedString" {
				scanErr = router.QueryRow(ctx, tt.sql).Scan(&name, &name)
			} else {
				scanErr = router.QueryRow(ctx, tt.sql).Scan(&name)
			}
			require.NoError(t, scanErr)
			require.Equal(t, "pgxpool_router_replica", name)
		})
	}

	// TABLE cannot return application_name, so verify routing through the replica pool's acquire count.
	replica := router.Replicas()[0]
	acquiresBefore := replica.Stat().AcquireCount()
	rows, err := router.Query(ctx, "table pg_catalog.pg_tables")
	require.NoError(t, err)
	rows.Close()
	require.NoError(t, rows.Err())
	require.Greater(t, replica.Stat().AcquireCount(), acquiresBefore)
}

func TestRouterTransactionCommitRecordsWrite(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	router := newRouterTestRouter(t, time.Minute, "pgxpool_router_replica")
	table := newRouterTestTable(t, ctx, router)

	// Fresh router so only the transaction influences routing.
	router2 := newRouterTestRouter(t, time.Minute, "pgxpool_router_replica")

	tx, err := router2.Begin(ctx)
	require.NoError(t, err)

	// Transactions always run on the primary.
	var name string
	require.NoError(t, tx.QueryRow(ctx, "select current_setting('application_name')").Scan(&name))
	require.Equal(t, "pgxpool_router_primary", name)

	_, err = tx.Exec(ctx, "insert into "+table+" (v) values ($1)", "tx-value")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	// After a committed transaction reads stick to the primary.
	require.Equal(t, "pgxpool_router_primary", queryApplicationName(t, ctx, router2))

	var v string
	require.NoError(t, router2.QueryRow(ctx, "select v from "+table+" where v = 'tx-value'").Scan(&v))
	require.Equal(t, "tx-value", v)
}

func TestRouterTransactionRollbackDoesNotRecordWrite(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	router := newRouterTestRouter(t, time.Minute, "pgxpool_router_replica")

	tx, err := router.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback(ctx))

	require.Equal(t, "pgxpool_router_replica", queryApplicationName(t, ctx, router))
}

func TestRouterCustomIsReadOnlyQuery(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	config := &pgxpool.RouterConfig{
		Primary:           newRouterTestPoolConfig(t, "pgxpool_router_primary"),
		Replicas:          []*pgxpool.Config{newRouterTestPoolConfig(t, "pgxpool_router_replica")},
		StickyWriteWindow: time.Minute,
		IsReadOnlyQuery:   func(sql string) bool { return false },
	}
	router, err := pgxpool.NewRouter(ctx, config)
	require.NoError(t, err)
	t.Cleanup(router.Close)

	require.Equal(t, "pgxpool_router_primary", queryApplicationName(t, ctx, router))
}

func TestRouterSendBatchAndCopyFromGoToPrimary(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	router := newRouterTestRouter(t, time.Minute, "pgxpool_router_replica")
	table := newRouterTestTable(t, ctx, router)

	batch := &pgx.Batch{}
	batch.Queue("insert into "+table+" (v) values ($1)", "batch-value")
	require.NoError(t, router.SendBatch(ctx, batch).Close())
	require.Equal(t, "pgxpool_router_primary", queryApplicationName(t, ctx, router))

	router2 := newRouterTestRouter(t, time.Minute, "pgxpool_router_replica")
	_, err := router2.CopyFrom(ctx, pgx.Identifier{table}, []string{"v"}, pgx.CopyFromRows([][]any{{"copy-value"}}))
	require.NoError(t, err)
	require.Equal(t, "pgxpool_router_primary", queryApplicationName(t, ctx, router2))
}

func TestRouterPing(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	router := newRouterTestRouter(t, time.Minute, "pgxpool_router_replica")
	require.NoError(t, router.Ping(ctx))

	router.Close()
	require.Error(t, router.Ping(ctx))
}

func TestRouterConcurrentUse(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	router := newRouterTestRouter(t, 50*time.Millisecond, "pgxpool_router_replica_1", "pgxpool_router_replica_2")
	table := newRouterTestTable(t, ctx, router)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if j%2 == 0 {
					_, err := router.Exec(ctx, "insert into "+table+" (v) values ($1)", "x")
					require.NoError(t, err)
				} else {
					rows, err := router.Query(ctx, "select v from "+table+" limit 1")
					require.NoError(t, err)
					rows.Close()
					require.NoError(t, rows.Err())
				}
			}
		}()
	}
	wg.Wait()
}
