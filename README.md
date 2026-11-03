# rewards-emit

Tiny client that OpenPak game servers use to report server-observed facts (a race finished, a
course uploaded, a trade completed) to the rewards service, `POST /internal/v1/events`.

- Async and buffered: `Event` never blocks and never returns an error.
- Batched (50 per request), retried with backoff, dropped after 6 hours undelivered.
- A full queue drops the new event rather than slowing the game.
- Disabled (a no-op) when `REWARDS_EVENTS_URL` or `REWARDS_EVENTS_KEY` is unset; a nil client is fine too.
- No dependencies beyond the standard library.

```go
rw := emit.FromEnv("nex-mario-kart-8")           // REWARDS_EVENTS_URL, REWARDS_EVENTS_KEY[_FILE]
defer rw.Close(ctx)
rw.Event(emit.Event{
    ID:     "mk8:race:" + raceID + ":" + pid,     // stable per occurrence: retries never double count
    Metric: "kart_races_completed.mk8",
    Player: &emit.Identity{Namespace: "wiiu", Subject: pid},
})
```

Only emit from the code path that proved the action. Metric names, the per-game signals and the
caps are in the rewards repo's `docs/metrics.md`. Events are deduplicated by `ID` for 60 days.

## Sessions (vendor hosts without a matchmaking store)

`emit.NewSessions(rw, "game_sessions.<game>", 0)` plus `Watch(ctx, 20*time.Second, snapshot)` turns a host's room
table into session events: a player counts once per room after `REWARDS_SESSION_MIN` (default 3 minutes) of
unbroken time with at least one other mapped player. `snapshot` returns `[]emit.Room{ID, Players []Identity}`;
a host that only sees join and leave events can keep an `emit.Registry` and pass `Registry.Snapshot`. Room ids
must be unique over time (they are the dedupe key). For a Switch title the identity is
`{Namespace: "switch", Subject: <BAAS user id>}`.
