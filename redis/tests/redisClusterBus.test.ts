import { describe, expect, test } from 'bun:test';
import type { ClusterMessage } from '@absolutejs/sync/engine';
import {
	createRedisClusterBus,
	type RedisPublisher,
	type RedisSubscriber
} from '../src/index';

/**
 * Mock Redis publisher + subscriber. Captures publish() calls and
 * delivers them synchronously through any registered subscribers on
 * the matching channel. Mirrors the in-memory cluster-bus pattern.
 */
const makeMockRedis = () => {
	type Sub = {
		channel: string;
		listener: (message: string) => void;
	};
	const subs: Sub[] = [];
	let subscribersToReport: number | undefined;
	const publisher: RedisPublisher = {
		publish: async (channel, message) => {
			const matching = subs.filter((s) => s.channel === channel);
			for (const s of matching) s.listener(message);
			return subscribersToReport ?? matching.length;
		}
	};
	const subscriber: RedisSubscriber = {
		subscribe: async (channel, listener) => {
			const entry: Sub = { channel, listener };
			subs.push(entry);
			return async () => {
				const idx = subs.indexOf(entry);
				if (idx >= 0) subs.splice(idx, 1);
			};
		}
	};
	return {
		publisher,
		setSubscribersToReport: (n: number) => {
			subscribersToReport = n;
		},
		subs,
		subscriber
	};
};

const sampleMessage: ClusterMessage = {
	changes: [
		{
			change: { op: 'insert', row: { id: 1, title: 'hi' } },
			table: 'tasks'
		}
	],
	origin: 'engine-A',
	originVersion: 3
};

describe('createRedisClusterBus — publish / subscribe round-trip', () => {
	test('a message published reaches the subscriber as a ClusterMessage', async () => {
		const mock = makeMockRedis();
		const bus = createRedisClusterBus({
			publisher: mock.publisher,
			subscriber: mock.subscriber
		});
		const received: ClusterMessage[] = [];
		await bus.subscribe((msg) => {
			received.push(msg);
		});
		await bus.publish(sampleMessage);
		expect(received).toHaveLength(1);
		expect(received[0]!.origin).toBe('engine-A');
		expect(received[0]!.originVersion).toBe(3);
		expect(received[0]!.changes).toHaveLength(1);
	});

	test('originVersion roundtrips end-to-end (1.17.0 cross-instance cursor)', async () => {
		const mock = makeMockRedis();
		const bus = createRedisClusterBus({
			publisher: mock.publisher,
			subscriber: mock.subscriber
		});
		const received: ClusterMessage[] = [];
		await bus.subscribe((msg) => {
			received.push(msg);
		});
		await bus.publish({
			changes: [],
			origin: 'engine-X',
			originVersion: 42
		});
		expect(received[0]!.originVersion).toBe(42);
	});

	test('pre-1.17.0 senders (no originVersion) preserve as undefined', async () => {
		const mock = makeMockRedis();
		const bus = createRedisClusterBus({
			publisher: mock.publisher,
			subscriber: mock.subscriber
		});
		const received: ClusterMessage[] = [];
		await bus.subscribe((msg) => {
			received.push(msg);
		});
		await bus.publish({ changes: [], origin: 'legacy-engine' });
		expect(received[0]!.originVersion).toBeUndefined();
	});

	test('two engines on same channel each receive their peer broadcasts', async () => {
		const mock = makeMockRedis();
		const bus = createRedisClusterBus({
			publisher: mock.publisher,
			subscriber: mock.subscriber
		});
		const a: ClusterMessage[] = [];
		const b: ClusterMessage[] = [];
		await bus.subscribe((msg) => a.push(msg));
		await bus.subscribe((msg) => b.push(msg));
		await bus.publish({
			changes: [],
			origin: 'A',
			originVersion: 1
		});
		expect(a).toHaveLength(1);
		expect(b).toHaveLength(1);
	});

	test('custom channel isolates two buses on the same Redis', async () => {
		const mock = makeMockRedis();
		const busA = createRedisClusterBus({
			channel: 'tenant_A',
			publisher: mock.publisher,
			subscriber: mock.subscriber
		});
		const busB = createRedisClusterBus({
			channel: 'tenant_B',
			publisher: mock.publisher,
			subscriber: mock.subscriber
		});
		const a: ClusterMessage[] = [];
		const b: ClusterMessage[] = [];
		await busA.subscribe((msg) => a.push(msg));
		await busB.subscribe((msg) => b.push(msg));
		await busA.publish({ changes: [], origin: 'engine-A' });
		expect(a).toHaveLength(1);
		expect(b).toHaveLength(0);
	});
});

describe('metrics()', () => {
	test('starts zeroed', () => {
		const mock = makeMockRedis();
		const bus = createRedisClusterBus({
			publisher: mock.publisher,
			subscriber: mock.subscriber
		});
		expect(bus.metrics()).toEqual({
			publishErrors: 0,
			published: 0,
			received: 0,
			subscribeErrors: 0,
			totalSubscribersReached: 0
		});
	});

	test('published + received bump per round-trip', async () => {
		const mock = makeMockRedis();
		const bus = createRedisClusterBus({
			publisher: mock.publisher,
			subscriber: mock.subscriber
		});
		await bus.subscribe(() => {});
		await bus.publish(sampleMessage);
		await bus.publish(sampleMessage);
		const m = bus.metrics();
		expect(m.published).toBe(2);
		expect(m.received).toBe(2);
	});

	test('totalSubscribersReached aggregates the count Redis returns from PUBLISH', async () => {
		const mock = makeMockRedis();
		mock.setSubscribersToReport(5);
		const bus = createRedisClusterBus({
			publisher: mock.publisher,
			subscriber: mock.subscriber
		});
		await bus.publish(sampleMessage);
		await bus.publish(sampleMessage);
		expect(bus.metrics().totalSubscribersReached).toBe(10);
	});

	test('publishErrors counter on publisher throw', async () => {
		const publisher: RedisPublisher = {
			publish: async () => {
				throw new Error('redis down');
			}
		};
		const mock = makeMockRedis();
		const bus = createRedisClusterBus({
			publisher,
			subscriber: mock.subscriber
		});
		await expect(bus.publish(sampleMessage)).rejects.toThrow(
			'redis down'
		);
		expect(bus.metrics().publishErrors).toBe(1);
		expect(bus.metrics().published).toBe(0);
	});

	test('subscribeErrors counter on JSON.parse failure', async () => {
		const subs: Array<(msg: string) => void> = [];
		const subscriber: RedisSubscriber = {
			subscribe: async (_channel, listener) => {
				subs.push(listener);
				return async () => {};
			}
		};
		const errors: unknown[] = [];
		const bus = createRedisClusterBus({
			onError: (error) => {
				errors.push(error);
			},
			publisher: { publish: async () => 0 },
			subscriber
		});
		await bus.subscribe(() => {});
		// Inject a malformed payload that JSON.parse will reject.
		subs[0]!('not-valid-json{');
		expect(bus.metrics().subscribeErrors).toBe(1);
		expect(bus.metrics().received).toBe(0);
		expect(errors).toHaveLength(1);
	});
});

describe('unsubscribe lifecycle', () => {
	test('the returned function detaches the listener', async () => {
		const mock = makeMockRedis();
		const bus = createRedisClusterBus({
			publisher: mock.publisher,
			subscriber: mock.subscriber
		});
		const received: ClusterMessage[] = [];
		const off = await bus.subscribe((msg) => received.push(msg));
		await bus.publish(sampleMessage);
		await off();
		await bus.publish(sampleMessage);
		expect(received).toHaveLength(1);
	});
});
