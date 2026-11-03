# Changelog

## 0.3.0 - 2026-11-03

- Durable queue: `REWARDS_EMIT_DATABASE_URL` / `Config.DatabaseURL` keeps events in Postgres (`emit.outbox`) so a restart
  or rewards outage loses nothing; delivered rows are deleted, default MaxAge is 7 days when DB-backed. Unset behaves as before.
  Database down at start: loud log, memory queue, reconnect retried.
- `Client.LoadState` / `SaveState` (`emit.emit_state`) and `Client.PersistSessions(name, sessions)` keep the counted-session clock in Postgres.
- New dependency: `github.com/jackc/pgx/v5` (consumers that vendor must re-vendor).

## 0.2.1

- `State`, `Restore`, `SetOnTick`: the session clock can be persisted (owner rule: nothing only in memory).

## 0.2.0

- `Sessions`, `Room`, `Registry`, `SessionMin`: room snapshots become `game_sessions.<game>` events (card #78).

## 0.1.0

- Initial release.
