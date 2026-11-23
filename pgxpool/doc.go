// Package pgxpool is a concurrency-safe connection pool for pgx.
/*
pgxpool implements a nearly identical interface to pgx connections.

Creating a Pool

The primary way of creating a pool is with [pgxpool.New]:

    pool, err := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))

The database connection string can be in URL or keyword/value format. PostgreSQL settings, pgx settings, and pool settings can be
specified here. In addition, a config struct can be created by [ParseConfig].

    config, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
    if err != nil {
        // ...
    }
    config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
        // do something with every new connection
    }

    pool, err := pgxpool.NewWithConfig(context.Background(), config)

A pool returns without waiting for any connections to be established. Acquire a connection immediately after creating
the pool to check if a connection can successfully be established.

Read/Write Splitting

[Router] routes queries between a primary server and read-only replicas. Writes always go to the primary. Read-only
queries are distributed over the replicas, except during a short sticky window after a write, when they go to the
primary so a read never observes a stale value that has not replicated yet.

    router, err := pgxpool.NewRouter(context.Background(), &pgxpool.RouterConfig{
        Primary:  primaryConfig,
        Replicas: []*pgxpool.Config{replicaConfig1, replicaConfig2},
    })
*/
package pgxpool
