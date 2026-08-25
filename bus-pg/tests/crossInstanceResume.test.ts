import { afterAll, describe, expect, test } from 'bun:test';
import postgres from 'postgres';
import { createSyncEngine, defineCollection } from '@absolutejs/sync/engine';
import type { ViewDiff } from '@absolutejs/sync/engine';
import { createPostgresClusterBus } from '../src/index';

/**
 * The headline 1.17.0/1.18.0/bus-pg-0.1.0 contract: a client carrying a
 * cursor from instance A can reconnect to instance B (over a SHARED PG
 * cluster bus) and receive a catch-up diff for the changes it missed —
 * no fresh snapshot, no sticky session.
 *
 * These tests exercise the FULL wire path (engine → bus-pg → pg_notify →
 * peer engine → cursor resume). The unit-level envelope tests pin the
 * wire format with a mocked Sql; this file proves the contract against
 * a real Postgres.
 */
const PG_URL =
	process.env.SYNC_BUS_PG_TEST_URL ??
	'postgresql://postgres:postgres@localhost:54330/sync_bus_pg_tests';

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

const wireEngine = (instanceId: string) => {
	const store = new Map<number, Task>();
	const engine = createSyncEngine({ instanceId });
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

const settle = () => new Promise((resolve) => setTimeout(resolve, 250));

describe('cross-instance resume — sync 1.17.0+ over sync-bus-pg', () => {
	test('cursor from instance A serves catch-up on instance B (over PG NOTIFY)', async () => {
		const channel = `xinst_${Date.now()}_${Math.random()
			.toString(36)
			.slice(2, 8)}`;
		const bus = createPostgresClusterBus({ sql, channel });

		const { engine: a } = wireEngine('engine-A');
		const { engine: b } = wireEngine('engine-B');

		const offA = await a.connectCluster(bus);
		const offB = await b.connectCluster(bus);

		// Subscribe on A, get the cursor, then unsubscribe — the client
		// is now "offline" carrying a cursor.
		const subA = await a.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined
		});
		const cursorFromA = subA.cursor;
		subA.unsubscribe();

		// Server changes via A while the client is moving between shards.
		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'via-A' }
		});
		// Bus propagation is async — wait for B to log the peer change.
		await settle();

		// Client reconnects to B with the cursor from A. B serves a
		// catch-up diff from its own log of peer-A changes — no
		// snapshot, no sticky session.
		const onB = collect();
		const resumedOnB = await b.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: onB.onDiff,
			params: undefined,
			since: cursorFromA
		});

		expect(resumedOnB.catchup).toBeDefined();
		expect(resumedOnB.catchup!.changed).toContainEqual({
			id: 1,
			title: 'via-A'
		});

		resumedOnB.unsubscribe();
		await offA();
		await offB();
	});

	test('cursor advances through B and stays resumable on A (round-trip)', async () => {
		const channel = `xinst_${Date.now()}_${Math.random()
			.toString(36)
			.slice(2, 8)}`;
		const bus = createPostgresClusterBus({ sql, channel });

		const { engine: a } = wireEngine('engine-A');
		const { engine: b } = wireEngine('engine-B');

		const offA = await a.connectCluster(bus);
		const offB = await b.connectCluster(bus);

		// Initial subscribe on A, capture starting cursor.
		const startSub = await a.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined
		});
		const startCursor = startSub.cursor;
		startSub.unsubscribe();

		// Mutations happen on BOTH instances while the client roams.
		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 10, title: 'a-1' }
		});
		await b.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 20, title: 'b-1' }
		});
		await settle();

		// Client lands on B with the original cursor — should catch up
		// on BOTH origins (peer-A change + B's own change). The cursor
		// vector covers all instances, not just the connecting one.
		const resumedOnB = await b.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined,
			since: startCursor
		});
		expect(resumedOnB.catchup).toBeDefined();
		const titles = (resumedOnB.catchup!.changed as Task[]).map(
			(t) => t.title
		);
		expect(titles).toContain('a-1');
		expect(titles).toContain('b-1');

		resumedOnB.unsubscribe();
		await offA();
		await offB();
	});

	test('catching up across both inline and spill paths', async () => {
		const channel = `xinst_${Date.now()}_${Math.random()
			.toString(36)
			.slice(2, 8)}`;
		const bus = createPostgresClusterBus({ sql, channel });

		const { engine: a } = wireEngine('engine-A');
		const { engine: b } = wireEngine('engine-B');

		const offA = await a.connectCluster(bus);
		const offB = await b.connectCluster(bus);

		const startSub = await b.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined
		});
		const startCursor = startSub.cursor;
		startSub.unsubscribe();

		// Small change → inline NOTIFY payload.
		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 1, title: 'small' }
		});
		// Oversized change → spill table + pointer NOTIFY.
		await a.applyChange<Task>('tasks', {
			op: 'insert',
			row: { id: 2, title: 'x'.repeat(8000) }
		});
		await settle();

		const resumedOnB = await b.subscribe<Task>({
			collection: 'tasks',
			ctx: {},
			onDiff: () => {},
			params: undefined,
			since: startCursor
		});

		expect(resumedOnB.catchup).toBeDefined();
		const ids = (resumedOnB.catchup!.changed as Task[]).map((t) => t.id);
		expect(ids).toContain(1);
		expect(ids).toContain(2);

		resumedOnB.unsubscribe();
		await offA();
		await offB();
	});
});
