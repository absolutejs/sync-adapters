# Changelog

## [0.0.1] — 2026-05-30

Initial preview. Redis pub/sub `ClusterBus` for `@absolutejs/sync`.

### Surface

- **`createRedisClusterBus({ publisher, subscriber, channel?, onError? })`** —
  returns `ClusterBus & { metrics() }`. Pass to
  `engine.connectCluster(bus)`.
- **Narrow client interface** — `RedisPublisher` (just
  `publish(channel, message)`) and `RedisSubscriber` (just
  `subscribe(channel, listener)` → unsubscribe fn). Works with
  ioredis, node-redis v4+, or any client wrapped to match. No
  hard SDK dep.
- **Default channel** `'absolutejs_sync_cluster'`. Override to
  isolate multiple buses on one Redis.

### Metrics

`published` / `received` / `publishErrors` / `subscribeErrors` +
`totalSubscribersReached` (PUBLISH return aggregate). A drop in
`totalSubscribersReached` when peers are expected = subscriber
disconnect signal.

### Tested

11 tests against a mock Redis publisher + subscriber: round-trip,
originVersion preservation, pre-1.17 legacy-sender handling, two
engines on one channel, channel-isolation between buses, metrics
zeroed start, publish/receive counters, totalSubscribersReached
aggregation, publishErrors on throw, subscribeErrors on
JSON.parse failure, unsubscribe lifecycle.

### License

Apache 2.0 (Tier B substrate-adjacent — rides `@absolutejs/sync`
Tier A).
