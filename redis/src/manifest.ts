import { defineImplementation, defineManifest } from '@absolutejs/manifest';
import { Type } from '@sinclair/typebox';
import type { CreateRedisClusterBusOptions } from './index';

/* Adapter package: everything rides the `sync/cluster-bus` implementation.
 * `publisher`/`subscriber` are instance-valued (any Redis client wrapped to
 * the narrow RedisPublisher/RedisSubscriber contracts) → wiring TODO
 * bindings; only the channel name is serializable. */
export const manifest = defineManifest<CreateRedisClusterBusOptions>()({
	contract: 1,
	identity: {
		accent: '#d82c20',
		category: 'sync',
		description:
			'Redis pub/sub `ClusterBus` for `@absolutejs/sync` — cross-instance fan-out via PUBLISH/SUBSCRIBE. No payload cap and lower fan-out latency than the Postgres sibling, at the cost of at-most-once delivery (a disconnected subscriber misses messages — use `@absolutejs/sync-bus-pg` when you need the durable spill table). Works with any Redis client (ioredis, node-redis) through a narrow publisher/subscriber interface.',
		docsUrl: 'https://github.com/absolutejs/sync-adapters/tree/main/redis',
		name: '@absolutejs/sync-bus-redis',
		tagline: 'Scale live updates across servers with Redis.'
	},
	implements: [
		defineImplementation<CreateRedisClusterBusOptions>()({
			contract: 'sync/cluster-bus',
			factory: 'createRedisClusterBus',
			from: '@absolutejs/sync-bus-redis',
			requires: {
				env: [
					{
						description:
							'Redis connection string the cluster bus publishes and subscribes on',
						example: 'redis://default:pass@host:6379',
						key: 'REDIS_URL',
						secret: true
					}
				],
				services: [
					{
						description:
							'Carries change fan-out between server instances',
						id: 'redis'
					}
				]
			},
			settings: Type.Object({
				channel: Type.Optional(
					Type.String({
						description:
							'Redis channel name. Two engines on the same Redis can stay separate by using different channels. Default absolutejs_sync_cluster.',
						title: 'Channel name'
					})
				)
			}),
			title: 'Redis pub/sub (fast fan-out, at-most-once)',
			wiring: {
				// `redisPublisher` / `redisSubscriber` are TODO bindings the
				// host declares: Redis pub/sub needs TWO connections, and the
				// subscriber wrapper differs per client library (the README
				// ships ready-made ioredis and node-redis wrappers).
				code: [
					'// TODO: create two Redis connections from ${env.REDIS_URL} with your',
					'// client (e.g. ioredis: `new Redis(url)` plus a `.duplicate()` for the',
					'// subscription) and wrap them as RedisPublisher / RedisSubscriber —',
					'// see the package README for ioredis and node-redis wrappers.',
					'createRedisClusterBus({ publisher: redisPublisher, subscriber: redisSubscriber, ...${settings} })'
				].join('\n'),
				imports: [
					{
						from: '@absolutejs/sync-bus-redis',
						names: ['createRedisClusterBus']
					}
				]
			}
		})
	],
	settings: Type.Object({}),
	wiring: []
});
