import { describe, expect, test } from 'bun:test';
import type { ClusterMessage } from '@absolutejs/sync/engine';
import { createPostgresClusterBus } from '../src/index';

/**
 * Mock the postgres tag-template enough to walk the publish/subscribe/
 * vacuum paths and assert the counters. Real PG round-trips live in
 * `postgresClusterBus.test.ts`; these are purely about the metrics shape
 * and accumulation.
 */
const makeMockSql = () => {
	const notifies: string[] = [];
	let spillRow: { id: string; message: ClusterMessage } | undefined;
	let listener: ((payload: string) => void) | undefined;
	let listenerReady: (() => void) | undefined;

	const tag = ((strings: TemplateStringsArray, ...values: unknown[]) => {
		const raw = strings.join('?').toLowerCase();
		if (raw.includes('pg_notify')) {
			notifies.push(String(values[1]));
			return Promise.resolve([]);
		}
		if (raw.includes('insert into sync_cluster_spill')) {
			const id = `spill-${notifies.length}`;
			const serialized = values[0] as string;
			spillRow = {
				id,
				message: JSON.parse(serialized) as ClusterMessage
			};
			return Promise.resolve([{ id }]);
		}
		if (raw.includes('select message from sync_cluster_spill')) {
			return Promise.resolve(
				spillRow !== undefined
					? [{ message: JSON.stringify(spillRow.message) }]
					: []
			);
		}
		if (raw.includes('delete from sync_cluster_spill')) {
			const had = spillRow !== undefined ? [{ id: spillRow.id }] : [];
			spillRow = undefined;
			return Promise.resolve(had);
		}
		return Promise.resolve([]);
	}) as any;
	tag.unsafe = (_sql: string) => Promise.resolve([]);
	tag.listen = (
		_channel: string,
		cb: (payload: string) => void,
		onlisten?: () => void
	) => {
		listener = cb;
		listenerReady = onlisten;
		onlisten?.();
		return Promise.resolve({ unlisten: () => Promise.resolve() });
	};

	return {
		deliver: (payload: string) => {
			if (listener) listener(payload);
		},
		notifies,
		reconnect: () => listenerReady?.(),
		sql: tag
	};
};

describe('PostgresClusterBus.metrics() — 0.1.2', () => {
	test('tracks listener connection, reconnect, probe, and unsubscribe lifecycle', async () => {
		const mock = makeMockSql();
		const bus = createPostgresClusterBus({
			listenerHealth: false,
			sql: mock.sql
		});
		expect(bus.listenerHealth().state).toBe('idle');
		const unsubscribe = await bus.subscribe(() => {});
		expect(bus.listenerHealth()).toMatchObject({
			activeSubscriptions: 1,
			connections: 1,
			reconnects: 0,
			state: 'connected'
		});

		mock.reconnect();
		expect(bus.listenerHealth()).toMatchObject({
			connections: 2,
			reconnects: 1,
			state: 'connected'
		});

		const probe = bus.probeListener();
		await new Promise((resolve) => setTimeout(resolve, 0));
		mock.deliver(mock.notifies.at(-1)!);
		expect(await probe).toBe(true);
		expect(bus.listenerHealth()).toMatchObject({
			probeAttempts: 1,
			probeFailures: 0,
			probeSuccesses: 1,
			state: 'connected'
		});

		await unsubscribe();
		expect(bus.listenerHealth()).toMatchObject({
			activeSubscriptions: 0,
			state: 'idle'
		});
	});

	test('marks a listener reconnecting when its end-to-end probe times out', async () => {
		const mock = makeMockSql();
		const errors: unknown[] = [];
		const bus = createPostgresClusterBus({
			listenerHealth: {
				probeIntervalMs: 60_000,
				probeTimeoutMs: 5
			},
			onError: (error) => errors.push(error),
			sql: mock.sql
		});
		const unsubscribe = await bus.subscribe(() => {});

		expect(await bus.probeListener()).toBe(false);
		expect(bus.listenerHealth()).toMatchObject({
			probeAttempts: 1,
			probeFailures: 1,
			probeSuccesses: 0,
			state: 'reconnecting'
		});
		expect(bus.metrics().subscribeErrors).toBe(1);
		expect(errors).toHaveLength(1);

		await unsubscribe();
	});

	test('starts with zeroed counters', () => {
		const { sql } = makeMockSql();
		const bus = createPostgresClusterBus({ sql });
		expect(bus.metrics()).toEqual({
			published: 0,
			publishedInline: 0,
			publishedSpilled: 0,
			publishErrors: 0,
			received: 0,
			spillFetchFailed: 0,
			spillFetched: 0,
			spillVacuumed: 0,
			subscribeErrors: 0
		});
	});

	test('inline publish bumps publishedInline + published', async () => {
		const { sql } = makeMockSql();
		const bus = createPostgresClusterBus({ sql });
		await bus.publish({
			changes: [],
			origin: 'engine-A',
			originVersion: 1
		});
		const m = bus.metrics();
		expect(m.published).toBe(1);
		expect(m.publishedInline).toBe(1);
		expect(m.publishedSpilled).toBe(0);
	});

	test('spill publish bumps publishedSpilled + published', async () => {
		const { sql } = makeMockSql();
		const bus = createPostgresClusterBus({ sql, spill: 'always' });
		await bus.publish({
			changes: [],
			origin: 'engine-A',
			originVersion: 1
		});
		const m = bus.metrics();
		expect(m.published).toBe(1);
		expect(m.publishedSpilled).toBe(1);
		expect(m.publishedInline).toBe(0);
	});

	test('received counter bumps for inline + spill deliveries', async () => {
		const mock = makeMockSql();
		const bus = createPostgresClusterBus({ sql: mock.sql });
		await bus.subscribe(() => {});

		mock.deliver(
			JSON.stringify({
				kind: 'inline',
				message: { changes: [], origin: 'peer-X', originVersion: 1 }
			})
		);
		// Spill path needs the row to exist on the mock first.
		await bus.publish({
			changes: [],
			origin: 'engine-A',
			originVersion: 2
		});
		// Drain microtasks so the async receiver path finishes its
		// JSON.parse + onMessage call.
		await new Promise((resolve) => setTimeout(resolve, 5));
		expect(bus.metrics().received).toBe(1);
	});

	test('spillFetched bumps on successful spill delivery', async () => {
		const mock = makeMockSql();
		const bus = createPostgresClusterBus({
			sql: mock.sql,
			spill: 'always'
		});
		await bus.subscribe(() => {});

		// Publish first so the mock has a spill row to return.
		await bus.publish({
			changes: [],
			origin: 'engine-A',
			originVersion: 1
		});
		// The mock recorded the spill envelope as a pg_notify payload —
		// deliver it back through the listener. (spill:'always' produces
		// ONE pg_notify per publish: the spill pointer.)
		const spillEnvelope = mock.notifies[0]!;
		mock.deliver(spillEnvelope);
		await new Promise((resolve) => setTimeout(resolve, 5));

		const m = bus.metrics();
		expect(m.spillFetched).toBe(1);
		expect(m.received).toBe(1);
		expect(m.spillFetchFailed).toBe(0);
	});

	test('spillFetchFailed bumps when the row is gone (e.g. vacuumed)', async () => {
		const mock = makeMockSql();
		let lastError: unknown;
		const bus = createPostgresClusterBus({
			onError: (error) => {
				lastError = error;
			},
			sql: mock.sql
		});
		await bus.subscribe(() => {});

		// Deliver a spill envelope for an id the mock has no row for.
		mock.deliver(JSON.stringify({ id: 'never-existed', kind: 'spill' }));
		await new Promise((resolve) => setTimeout(resolve, 5));

		const m = bus.metrics();
		expect(m.spillFetchFailed).toBe(1);
		expect(m.subscribeErrors).toBe(1);
		expect(lastError).toBeDefined();
	});

	test('vacuum() bumps spillVacuumed by the deleted-row count', async () => {
		const mock = makeMockSql();
		const bus = createPostgresClusterBus({
			sql: mock.sql,
			spill: 'always'
		});
		await bus.publish({
			changes: [],
			origin: 'engine-A',
			originVersion: 1
		});
		const deleted = await bus.vacuum(0);
		expect(deleted).toBeGreaterThanOrEqual(1);
		expect(bus.metrics().spillVacuumed).toBe(deleted);
	});

	test('publishErrors bumps when publish() throws (e.g. spill:never overflow)', async () => {
		const { sql } = makeMockSql();
		const bus = createPostgresClusterBus({ sql, spill: 'never' });
		// Build an oversized message — title alone exceeds the inline budget.
		const huge = 'x'.repeat(8000);
		await expect(
			bus.publish({
				changes: [
					{
						change: {
							op: 'insert',
							row: { id: 1, title: huge }
						},
						table: 'tasks'
					}
				] as any,
				origin: 'engine-A',
				originVersion: 1
			})
		).rejects.toThrow(/exceeds inline budget/);
		expect(bus.metrics().publishErrors).toBe(1);
		expect(bus.metrics().published).toBe(0);
	});
});
