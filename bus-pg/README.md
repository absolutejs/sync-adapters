# @absolutejs/sync-bus-pg

Postgres `LISTEN/NOTIFY` cluster bus for [`@absolutejs/sync`](https://github.com/absolutejs/sync). Run sync horizontally across several Bun processes without standing up Redis or Kafka — your existing Postgres carries the cross-instance change feed.

## Why

`@absolutejs/sync` ships a `ClusterBus` seam: an in-memory bus for single-process dev + tests, and a contract you implement against your bus of choice for production. Until now that meant writing ~50 lines of `LISTEN/NOTIFY` plumbing yourself. This package is the first-party implementation, with the 8000-byte NOTIFY payload limit handled cleanly.

## Install

```bash
bun add @absolutejs/sync-bus-pg postgres
```

## Use

```ts
import postgres from 'postgres';
import { createSyncEngine } from '@absolutejs/sync/engine';
import { createPostgresClusterBus } from '@absolutejs/sync-bus-pg';

const sql = postgres(process.env.DATABASE_URL!);
const engine = createSyncEngine();
// ...registerReader/Writer/Reactive/Mutation as usual...

const bus = createPostgresClusterBus({ sql });
await engine.connectCluster(bus);
```

Every instance of your Elysia app does the same. A mutation committed on instance A now fans out to subscribers on B/C/... via Postgres `pg_notify`.

## Options

```ts
createPostgresClusterBus({
  sql,                                 // your postgres client
  channel: 'absolutejs_sync_cluster',  // override to scope multiple engines on the same PG
  spill: 'overflow',                   // 'overflow' (default) | 'always' | 'never'
  onError: (e) => log.warn(e)          // listener-side errors
});
```

`spill` strategies:

- **`'overflow'`** (default) — inline JSON when small, table-backed when oversized. Best for typical workloads.
- **`'always'`** — every message goes through the `sync_cluster_spill` table (durable, slightly slower; useful when you want every cross-instance change to survive a NOTIFY drop).
- **`'never'`** — throws if a message exceeds the inline budget. Useful in tests to assert payload-size discipline.

## Vacuum

Oversized messages spill to `sync_cluster_spill`. Rows aren't auto-deleted on consume (every listener on the channel needs to read them, including the publisher's own listener which fetches but doesn't double-apply via the engine's `origin` filter). Sweep periodically:

```ts
import { defineSchedule } from '@absolutejs/sync/engine';

engine.registerSchedule(
  defineSchedule({
    name: 'vacuum-cluster-spill',
    pattern: '*/5 * * * *', // every 5 minutes
    run: async () => {
      const pruned = await bus.vacuum(60_000); // older than 60s
      console.log(`pruned ${pruned} spill rows`);
    }
  })
);
```

For workloads where messages stay small (the common case), the spill table never gets touched and `vacuum()` always returns 0.

## Caveats inherited from the engine seam

- **Per-instance version cursors.** A client that reconnects to a *different* instance falls back to a fresh snapshot (cold-hydration cost, not catch-up diff). Use sticky sessions if cross-instance reconnect-with-`since` matters.
- **Best-effort delivery.** Inline NOTIFY can be lost if a listener connection drops mid-stream — every instance also has its own change log for resume, so a missed cross-instance fan-out is recovered on the next subscribe. For at-least-once cross-instance, run with `spill: 'always'`.

## License

Apache License 2.0. See [LICENSE](./LICENSE).
