# @absolutejs/sync-bus-redis

Redis pub/sub `ClusterBus` for
[@absolutejs/sync](https://github.com/absolutejs/sync). Sibling to
[`@absolutejs/sync-bus-pg`](../bus-pg) — same `ClusterBus` contract,
different transport.

## When to use Redis vs Postgres

| Concern | `sync-bus-redis` | `sync-bus-pg` |
|---|---|---|
| Payload size | No cap — JSON through | 8KB NOTIFY cap (spill-table fallback) |
| Fan-out latency at 10+ subscribers | In-memory, low | WAL-replicated, higher |
| Geo-replication | Native (Redis Cluster, ElastiCache, Memorystore, Upstash) | Heavy ops (PG logical replication) |
| Delivery semantics | At-most-once (no message retention) | At-most-once (NOTIFY) + spill rows for oversized |
| Already in your stack? | If yes → win-win | If yes → win-win |

**The headline tradeoff.** Redis is in-memory pub/sub — a subscriber
that's disconnected when a message fires misses it. For
cross-instance resume past shard reboot, pair with
`engine.exportChangeLog()` / `importChangeLog()` (sync 1.19.0+)
regardless of which bus you use.

## Install

```sh
bun add @absolutejs/sync @absolutejs/sync-bus-redis
# Plus a Redis client — bring your own:
bun add ioredis            # OR
bun add redis              # node-redis v4+
```

The adapter has no hard runtime dep on a specific Redis client.

## Usage with `ioredis`

```ts
import { Redis } from 'ioredis';
import { createSyncEngine } from '@absolutejs/sync/engine';
import { createRedisClusterBus } from '@absolutejs/sync-bus-redis';

const publisher = new Redis(process.env.REDIS_URL!);
const subscriberClient = new Redis(process.env.REDIS_URL!);

// ioredis: a subscribed connection can't issue other commands.
// Bridge its EventEmitter API into our (channel, listener) shape:
const subscriber = {
  subscribe: async (channel: string, listener: (msg: string) => void) => {
    await subscriberClient.subscribe(channel);
    const handler = (chan: string, msg: string) => {
      if (chan === channel) listener(msg);
    };
    subscriberClient.on('message', handler);
    return async () => {
      subscriberClient.off('message', handler);
      await subscriberClient.unsubscribe(channel);
    };
  },
};

const bus = createRedisClusterBus({ publisher, subscriber });
const engine = createSyncEngine({ instanceId: 'shard-A' });
await engine.connectCluster(bus);
```

## Usage with `node-redis` v4+

```ts
import { createClient } from 'redis';
import { createRedisClusterBus } from '@absolutejs/sync-bus-redis';

const publisher = createClient({ url: process.env.REDIS_URL });
const subscriberClient = publisher.duplicate();
await Promise.all([publisher.connect(), subscriberClient.connect()]);

// node-redis: subscribe takes a callback directly.
const subscriber = {
  subscribe: async (channel: string, listener: (msg: string) => void) => {
    await subscriberClient.subscribe(channel, listener);
    return async () => {
      await subscriberClient.unsubscribe(channel);
    };
  },
};

const bus = createRedisClusterBus({ publisher, subscriber });
```

## API

```ts
createRedisClusterBus({
  publisher,         // RedisPublisher — publish(channel, message)
  subscriber,        // RedisSubscriber — subscribe(channel, listener) → unsubscribe fn
  channel?,          // default 'absolutejs_sync_cluster'
  onError?,          // logger for parse / delivery failures
});
```

Returns `ClusterBus & { metrics() }`. Pass to `engine.connectCluster(bus)`.

### `bus.metrics()`

```ts
{
  published: number;             // PUBLISH calls
  received: number;              // messages parsed + delivered to onMessage
  publishErrors: number;         // publisher.publish() rejections
  subscribeErrors: number;       // JSON.parse failures on incoming messages
  totalSubscribersReached: number; // sum of counts Redis returned from PUBLISH
                                 // — a drop to 0 when you expect peers signals
                                 // subscriber disconnects (partition, restart)
}
```

## License

[Apache 2.0](../LICENSE). Tier B substrate-adjacent under the
AbsoluteJS licensing policy — rides `@absolutejs/sync` (BSL Tier A).
