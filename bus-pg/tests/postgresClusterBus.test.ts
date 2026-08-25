import { afterAll, describe, expect, test } from 'bun:test';
import postgres from 'postgres';
import { createSyncEngine, defineMutation } from '@absolutejs/sync/engine';
import type { ViewDiff } from '@absolutejs/sync/engine';
import { defineCollection } from '@absolutejs/sync/engine';
import { createPostgresClusterBus } from '../src/index';

/**
 * These tests run against a real Postgres — the existing benchmarks container
 * (`docker run sync-bench-pg`) is reused for convenience. If PG isn't
 * reachable, every test fails loudly rather than passing silently.
 *
 * Strategy: spin up two engines wired by a shared PG-NOTIFY bus on a
 * test-scoped channel, fire a mutation on engine A, assert engine B's
 * subscriber sees it.
 */
// Default to a clean test DB without unrelated Postgres extensions / triggers.
// Override with SYNC_BUS_PG_TEST_URL when pointing at your own instance.
// Create with: docker exec sync-bench-pg psql -U postgres -c 'create database sync_bus_pg_tests'
const PG_URL =
	process.env.SYNC_BUS_PG_TEST_URL ??
	'postgresql://postgres:postgres@localhost:54330/sync_bus_pg_tests';

// Each engine in a test grabs a dedicated LISTEN connection that's held for
// the test's lifetime. With 5 tests × up to 2 engines × (listen + query), the
// default pool gets exhausted partway through the suite — bump it.
const sql = process.env.SYNC_BUS_PG_TEST_SOCKET
	? postgres({
			database: 'sync_bus_pg_tests',
			host: process.env.SYNC_BUS_PG_TEST_SOCKET,
			max: 20,
			password: 'postgres',
			user: 'postgres'
		})
	: postgres(PG_URL, { max: 20 });
afterAll(() => sql.end());

type Task = { id: number; title: string };

const wireEngine = () => {
	const store = new Map<number, Task>();
	const engine = createSyncEngine();
	engine.registerReader('tasks', { all: () => [...store.values()] });
	engine.registerWriter<Task>('tasks', {
		delete: (row) => {
			store.delete(row.id);
		},
		insert: (data) => {
			store.set(data.id, data);
			return data;
		},
		update: (data) => {
			store.set(data.id, data);
			return data;
		}
	});
	engine.register(
		defineCollection<Task>({
			hydrate: () => [...store.values()],
			key: (task) => task.id,
			match: () => true,
			name: 'tasks'
		})
	);
	return { engine, store };
};

const collect = () => {
	const diffs: ViewDiff<Task>[] = [];

	return {
		diffs,
		onDiff: (diff: ViewDiff<Task>) => {
			diffs.push(diff);
		}
	};
};

// LISTEN/NOTIFY round-trip on a warm conn is under 10ms, but the test
// container is shared with other suites and can spike past 100ms during
// concurrent DDL. 250ms is a safe ceiling that still keeps the suite snappy.
const settle = () => new Promise((resolve) => setTimeout(resolve, 250));

describe('PostgresClusterBus — real PG LISTEN/NOTIFY end-to-end', () => {
	test('a mutation on engine A fans out to engine B over PG NOTIFY', async () => {
		const channel = `sync_bus_pg_test_${Date.now()}_${Math.random()
			.toString(36)
			.slice(2, 8)}`;
		const bus = createPostgresClusterBus({ sql, channel });

		const { engine: a } = wireEngine();
		const { engine: b } = wireEngine();
		for (const engine of [a, b]) {
			engine.registerMutation(
				defineMutation({
					name: 'add',
					handler: async (args: Task, _ctx, actions) => {
						await actions.change('tasks', {
							op: 'insert',
							row: args
						});
					}
				})
			);
		}
		const disconnectA = await a.connectCluster(bus);
		const disconnectB = await b.connectCluster(bus);

		const onB = collect();
		await b.subscribe<Task>({
			collection: 'tasks',
			params: undefined,
			ctx: {},
			onDiff: onB.onDiff
		});

		await a.runMutation('add', { id: 1, title: 'over the wire' }, {});
		// LISTEN/NOTIFY delivery is async — give the listener a tick.
		await settle();

		expect(onB.diffs.at(-1)?.added).toEqual([
			{ id: 1, title: 'over the wire' }
		]);

		await disconnectA();
		await disconnectB();
	});

	test('an instance ignores its own broadcast (origin filter)', async () => {
		const channel = `sync_bus_pg_test_${Date.now()}_${Math.random()
			.toString(36)
			.slice(2, 8)}`;
		const bus = createPostgresClusterBus({ sql, channel });
		const { engine: a } = wireEngine();
		const disconnect = await a.connectCluster(bus);

		const onA = collect();
		await a.subscribe<Task>({
			collection: 'tasks',
			params: undefined,
			ctx: {},
			onDiff: onA.onDiff
		});

		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 7, title: 'echo' }
		});
		await settle();

		// Exactly one diff — the engine's origin tag suppresses the echoed
		// NOTIFY when it loops back through this same instance's LISTEN.
		expect(onA.diffs).toHaveLength(1);
		expect(onA.diffs[0]?.added).toEqual([{ id: 7, title: 'echo' }]);

		await disconnect();
	});

	test('oversized payload spills to sync_cluster_spill and is fetched on the other side', async () => {
		const channel = `sync_bus_pg_test_${Date.now()}_${Math.random()
			.toString(36)
			.slice(2, 8)}`;
		const bus = createPostgresClusterBus({ sql, channel });
		const { engine: a } = wireEngine();
		const { engine: b } = wireEngine();
		await a.connectCluster(bus);
		const disconnectB = await b.connectCluster(bus);

		const onB = collect();
		await b.subscribe<Task>({
			collection: 'tasks',
			params: undefined,
			ctx: {},
			onDiff: onB.onDiff
		});

		// Title big enough that the inline JSON envelope exceeds the 6 KB
		// inline budget and the bus is forced to use the spill table.
		const bigTitle = 'x'.repeat(8000);
		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 42, title: bigTitle }
		});
		// Spill round-trip = NOTIFY id + SELECT + DELETE — a few PG hops.
		await new Promise((resolve) => setTimeout(resolve, 200));

		const added = onB.diffs.at(-1)?.added;
		expect(added).toHaveLength(1);
		expect(added?.[0]?.id).toBe(42);
		expect(added?.[0]?.title.length).toBe(8000);

		await disconnectB();
	});

	test('vacuum() prunes spill rows older than the cutoff', async () => {
		const channel = `sync_bus_pg_test_${Date.now()}_${Math.random()
			.toString(36)
			.slice(2, 8)}`;
		const bus = createPostgresClusterBus({ sql, channel });
		const { engine: a } = wireEngine();
		await a.connectCluster(bus);

		// Force a spill by emitting an oversized change.
		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 100, title: 'y'.repeat(8000) }
		});
		await settle();

		// Row just landed — vacuuming with a 1-hour cutoff should not touch it.
		const recentDeleted = await bus.vacuum(60 * 60 * 1000);
		expect(recentDeleted).toBe(0);

		// Vacuuming with cutoff 0 should sweep everything we just wrote.
		const allDeleted = await bus.vacuum(0);
		expect(allDeleted).toBeGreaterThanOrEqual(1);
	});

	test('after disconnect, no further fan-out arrives', async () => {
		const channel = `sync_bus_pg_test_${Date.now()}_${Math.random()
			.toString(36)
			.slice(2, 8)}`;
		const bus = createPostgresClusterBus({ sql, channel });
		const { engine: a } = wireEngine();
		const { engine: b } = wireEngine();
		await a.connectCluster(bus);
		const disconnectB = await b.connectCluster(bus);

		const onB = collect();
		await b.subscribe<Task>({
			collection: 'tasks',
			params: undefined,
			ctx: {},
			onDiff: onB.onDiff
		});

		await disconnectB();
		await settle();

		const beforeCount = onB.diffs.length;
		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 99, title: 'after disconnect' }
		});
		await settle();

		expect(onB.diffs.length).toBe(beforeCount);
	});
});
