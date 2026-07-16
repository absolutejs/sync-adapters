/**
 * @absolutejs/sync-bus-redis — Redis pub/sub `ClusterBus` for
 * `@absolutejs/sync`. Cross-instance fan-out via PUBLISH/SUBSCRIBE.
 *
 * **Why Redis (vs the `sync-bus-pg` sibling).**
 *
 *   - **No payload cap.** Postgres `NOTIFY` is capped at 8000 bytes,
 *     forcing `sync-bus-pg` into a spill-table dance for large batches.
 *     Redis PUBLISH has no equivalent limit — JSON straight through.
 *   - **Lower latency at fan-out.** Redis pub/sub is in-memory; PG
 *     `LISTEN/NOTIFY` rides the WAL replication path. For multi-shard
 *     fan-out (10+ subscribers), Redis is typically faster.
 *   - **Better geo-replication story.** Redis Cluster / Redis
 *     Enterprise / managed Redis (ElastiCache, Memorystore, Upstash)
 *     all geo-replicate natively. Postgres logical replication is
 *     possible but heavier ops.
 *
 * **Why NOT Redis (when to use `sync-bus-pg` instead).**
 *
 *   - **At-most-once delivery.** Redis pub/sub does NOT durably store
 *     messages. A subscriber that's disconnected when a message fires
 *     misses it. If you need a durable cluster bus for replay across
 *     shard reboot, use `sync-bus-pg` (which has the spill table) and
 *     pair with `engine.exportChangeLog()` / `importChangeLog()` for
 *     post-reboot resume.
 *   - **Already have Postgres in your stack.** Adding Redis just for
 *     the cluster bus is a new dependency to operate.
 *
 * **Client compatibility.**
 *
 * The adapter takes a narrow interface (`publisher.publish(channel,
 * message)` and `subscriber.subscribe(channel, listener)`) so it works
 * with any 2026-era Redis client:
 *
 *   - `ioredis`: `const redis = new Redis(url); const sub = redis.duplicate();`
 *   - `node-redis` v4+: `const client = createClient({ url }); const sub = client.duplicate();`
 *
 * Each pub/sub-using consumer needs TWO connections — one for
 * publishing (your existing client), one for subscribing (a duplicate
 * dedicated to the subscription, because Redis prohibits other
 * commands on a subscribed connection). Pass both to
 * `createRedisClusterBus({ publisher, subscriber })`.
 *
 * See `README.md` for `ioredis` and `node-redis` adapter snippets.
 */

import type { ClusterBus, ClusterMessage } from '@absolutejs/sync/engine';

/**
 * Minimal Redis publisher contract. Both `ioredis` and `node-redis` v4
 * structurally satisfy this (`publish(channel, message)` is the
 * canonical signature; both return a Promise that resolves to the
 * number of subscribers that received the message).
 */
export type RedisPublisher = {
	publish: (channel: string, message: string) => Promise<number | unknown>;
};

/**
 * Minimal Redis subscriber contract. The shape diverges between
 * ioredis (EventEmitter-based) and node-redis (callback-based);
 * `RedisSubscriber` is the lowest-common-denominator:
 *
 *   - `subscribe(channel, listener)` registers `listener` for messages
 *     on `channel` AND returns an unsubscribe function. The listener
 *     receives the raw message string.
 *
 * The README shows how to wrap ioredis (where you'd call
 * `subscriber.on('message', handler)` once and route by channel) and
 * node-redis (where `subscribe(channel, listener)` is the SDK call
 * directly).
 */
export type RedisSubscriber = {
	subscribe: (
		channel: string,
		listener: (message: string) => void
	) => Promise<() => Promise<void>>;
};

export type CreateRedisClusterBusOptions = {
	/** The Redis client used for `publish()` calls. */
	publisher: RedisPublisher;
	/** The Redis subscriber. MUST be a dedicated connection (a
	 *  `duplicate()` of your main client) — Redis forbids other
	 *  commands on a subscribed connection. */
	subscriber: RedisSubscriber;
	/**
	 * Channel name. Defaults to `'absolutejs_sync_cluster'`. Two
	 * engines on the same Redis can scope themselves to different
	 * channels by overriding this.
	 */
	channel?: string;
	/**
	 * Called when message parsing / delivery fails. Defaults to
	 * `console.warn`. Note that Redis pub/sub doesn't surface delivery
	 * failures on the publisher side — `publish()` resolves to the
	 * count of subscribers that received the message (or 0); a 0 count
	 * does NOT fire onError (Redis treats "no subscribers" as success).
	 */
	onError?: (error: unknown) => void;
};

/**
 * Cumulative metrics since `createRedisClusterBus()`. Same shape
 * style as `sync-bus-pg`'s — minus the spill-table fields (Redis
 * has no equivalent).
 */
export type RedisClusterBusMetrics = {
	published: number;
	received: number;
	publishErrors: number;
	subscribeErrors: number;
	/**
	 * Cumulative count of subscribers reached on the publisher side.
	 * Returned by Redis's PUBLISH. Use it as a rough "is the cluster
	 * still wired up" signal — a drop to 0 when you expect peers means
	 * subscribers disconnected (replication lag, network partition).
	 */
	totalSubscribersReached: number;
};

export type RedisClusterBus = ClusterBus & {
	metrics: () => RedisClusterBusMetrics;
};

/** Payload-agnostic fan-out using the same Redis pub/sub transport. Messages
 * are at-most-once; durable commands and effects belong in `@absolutejs/queue`. */
export type RedisChannelBus<Message> = {
	metrics: () => RedisClusterBusMetrics;
	publish: (message: Message) => Promise<void>;
	subscribe: (
		onMessage: (message: Message) => void
	) => Promise<() => Promise<void>>;
};

const createRedisBus = <Message>(
	options: CreateRedisClusterBusOptions
): RedisChannelBus<Message> => {
	const channel = options.channel ?? 'absolutejs_sync_cluster';
	const onError =
		options.onError ?? ((error) => console.warn('[sync-bus-redis]', error));

	const counters: RedisClusterBusMetrics = {
		publishErrors: 0,
		published: 0,
		received: 0,
		subscribeErrors: 0,
		totalSubscribersReached: 0
	};

	return {
		metrics: () => ({ ...counters }),
		publish: async (message: Message): Promise<void> => {
			try {
				const serialized = JSON.stringify(message);
				const result = await options.publisher.publish(
					channel,
					serialized
				);
				if (typeof result === 'number') {
					counters.totalSubscribersReached += result;
				}
				counters.published += 1;
			} catch (error) {
				counters.publishErrors += 1;
				throw error;
			}
		},
		subscribe: async (
			onMessage: (message: Message) => void
		): Promise<() => Promise<void>> => {
			const listener = (raw: string): void => {
				try {
					const parsed = JSON.parse(raw) as Message;
					counters.received += 1;
					onMessage(parsed);
				} catch (error) {
					counters.subscribeErrors += 1;
					onError(error);
				}
			};
			return options.subscriber.subscribe(channel, listener);
		}
	};
};

export const createRedisChannelBus = <Message>(
	options: CreateRedisClusterBusOptions
): RedisChannelBus<Message> => createRedisBus<Message>(options);

export const createRedisClusterBus = (
	options: CreateRedisClusterBusOptions
): RedisClusterBus => createRedisBus<ClusterMessage>(options);
