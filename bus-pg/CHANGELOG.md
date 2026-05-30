# Changelog

All notable changes to `@absolutejs/sync-bus-pg` are recorded here. The format
is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This package is pre-1.0 — minor bumps may carry breaking changes; we'll call
them out here.

## [0.1.1] — 2026-05-29

### Fixed — spill-path jsonb decode + cross-instance resume integration

- **Spill path was passing the raw jsonb column value to `onMessage`
  unparsed.** postgres-js's tagged-template returns a jsonb column as
  a JSON string under common configurations (auto-parsing isn't
  universal across versions/setups). Receivers would get a string
  instead of a `ClusterMessage`, then crash inside the engine's
  cluster-apply with `TypeError: changes.length` when the cluster
  callback dereferenced `message.changes`. Normalized to always parse
  if the column comes back as a string; pre-parsed objects fall
  through unchanged.
- **3 new cross-instance resume integration tests** against a real
  Postgres: cursor from instance A served by B; round-trip cursor
  advancing through B and recovering on A; mixed inline+spill
  resume. These pin the 1.17.0/1.18.0/0.1.0 contract end-to-end.
- The 5 existing PG `LISTEN`/`NOTIFY` end-to-end tests now pass
  cleanly: the test fixtures were refreshed to match sync's current
  `registerReader`/`registerWriter`/`defineCollection` shape and
  the settle delay was bumped from 50ms to 250ms (warm-conn NOTIFY
  is under 10ms but shared containers spike to ~100ms).
- **Requires `@absolutejs/sync` ≥ 1.18.2** in dev — 1.18.2 fixes a
  separate `canResume` bug surfaced by the mixed inline+spill test
  here (engine bailed to a fresh snapshot when its local log had
  only peer-broadcast entries).

## [0.1.0] — 2026-05-29

### Added — cross-instance resume on the wire (sync 1.17.0+)

The bus envelope already carried `origin` so peers ignore their own
broadcasts. With `@absolutejs/sync` 1.17.0+, every broadcast also carries
`originVersion` — the local version of the originating instance at the
moment of broadcast. Receiving engines log peer changes against
`(origin, originVersion)`, so a client that resumes against a *different*
instance can be served a catch-up diff from that instance's own log of
peer-broadcast changes. No sticky sessions required.

This package update widens the peer dep to require sync 1.17.0+ for the
wire field to be populated — older sync versions still work but
`originVersion` defaults to `0` on the receiver, which forces any
cross-instance resume to fall back to a fresh snapshot (matching
pre-1.17.0 behavior exactly).

- `peerDependencies['@absolutejs/sync']`: `>= 1.0.0` (unchanged — the
  field is optional on the wire).
- `devDependencies['@absolutejs/sync']`: `^1.18.1` (was `^1.2.0`) — the
  type widening lives in 1.17.0+ and the published `.d.ts` was correctly
  regenerated in 1.18.1.

### Tests

- 4 new unit tests in `tests/envelopeRoundtrip.test.ts` that mock the
  `Sql` tag-template to capture the `pg_notify` payload — they verify
  `originVersion` flows through inline AND spill paths, that the
  receiving handler decodes it back, and that missing `originVersion`
  (pre-1.17.0 senders) is preserved as `undefined` end-to-end.
- The pre-existing PG `LISTEN`/`NOTIFY` end-to-end tests in
  `tests/postgresClusterBus.test.ts` are flaky against busy PG
  containers and depend on a clean database; the test fixtures were
  refreshed to match sync's current
  `registerReader` / `registerWriter` / `defineCollection` shape.
  Four of those tests still fail intermittently — tracked separately;
  the inline/spill path itself is exercised by the new unit tests.

## [0.0.2] — earlier

Initial preview — Postgres `LISTEN` / `NOTIFY` cluster bus implementing
`ClusterBus`, with overflow spill via `sync_cluster_spill` table and a
`vacuum()` helper to prune old spill rows.
