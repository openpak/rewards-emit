package emit

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// store is the durable side of the client (Postgres in production, a fake in tests).
type store interface {
	insert(ctx context.Context, source, id string, payload []byte) error
	pending(ctx context.Context, source string, limit int) ([]outboxRow, error)
	remove(ctx context.Context, source string, ids []string) error
	purge(ctx context.Context, source string, olderThan time.Time) (int64, error)
	loadState(ctx context.Context, source, name string) ([]byte, bool, error)
	saveState(ctx context.Context, source, name string, v []byte) error
	close()
}

type outboxRow struct {
	ID      string
	Payload []byte
}

type pgStore struct{ pool *pgxpool.Pool }

const schemaSQL = `
CREATE SCHEMA IF NOT EXISTS emit;
CREATE TABLE IF NOT EXISTS emit.outbox (
  source text NOT NULL, event_id text NOT NULL, payload jsonb NOT NULL,
  created timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (source, event_id));
CREATE INDEX IF NOT EXISTS outbox_created ON emit.outbox (source, created);
CREATE TABLE IF NOT EXISTS emit.emit_state (
  source text NOT NULL, name text NOT NULL, value bytea NOT NULL,
  updated timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (source, name));`

func openPG(ctx context.Context, url string) (store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		pool.Close()
		return nil, err
	}
	// serialize first-start schema creation across services
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7243001)`); err == nil {
		_, err = tx.Exec(ctx, schemaSQL)
	}
	if err == nil {
		err = tx.Commit(ctx)
	} else {
		_ = tx.Rollback(ctx)
	}
	if err != nil {
		pool.Close()
		return nil, err
	}
	return &pgStore{pool}, nil
}

func (p *pgStore) insert(ctx context.Context, source, id string, payload []byte) error {
	_, err := p.pool.Exec(ctx, `INSERT INTO emit.outbox(source,event_id,payload) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`,
		source, id, payload)
	return err
}

func (p *pgStore) pending(ctx context.Context, source string, limit int) ([]outboxRow, error) {
	rows, err := p.pool.Query(ctx, `SELECT event_id, payload FROM emit.outbox WHERE source=$1 ORDER BY created, event_id LIMIT $2`, source, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.ID, &r.Payload); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (p *pgStore) remove(ctx context.Context, source string, ids []string) error {
	_, err := p.pool.Exec(ctx, `DELETE FROM emit.outbox WHERE source=$1 AND event_id = ANY($2)`, source, ids)
	return err
}

func (p *pgStore) purge(ctx context.Context, source string, olderThan time.Time) (int64, error) {
	t, err := p.pool.Exec(ctx, `DELETE FROM emit.outbox WHERE source=$1 AND created < $2`, source, olderThan)
	return t.RowsAffected(), err
}

func (p *pgStore) loadState(ctx context.Context, source, name string) ([]byte, bool, error) {
	var v []byte
	err := p.pool.QueryRow(ctx, `SELECT value FROM emit.emit_state WHERE source=$1 AND name=$2`, source, name).Scan(&v)
	if err == pgx.ErrNoRows {
		return nil, false, nil
	}
	return v, err == nil, err
}

func (p *pgStore) saveState(ctx context.Context, source, name string, v []byte) error {
	_, err := p.pool.Exec(ctx, `INSERT INTO emit.emit_state(source,name,value) VALUES($1,$2,$3)
		ON CONFLICT (source,name) DO UPDATE SET value=EXCLUDED.value, updated=now()`, source, name, v)
	return err
}

func (p *pgStore) close() { p.pool.Close() }
