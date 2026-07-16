/**
 * Unit tests for the bus envelope — verify the wire shape round-trips
 * without touching real Postgres. Drives publish through a mock `sql`
 * tag-template that captures every NOTIFY payload, then re-parses the
 * envelope and confirms the structured `ClusterMessage` survives the
 * trip — including the `originVersion` field added in sync 1.17.0
 * (the field that enables cross-instance resume).
 */

import { describe, expect, test } from 'bun:test';
import type { ClusterMessage } from '@absolutejs/sync/engine';
import {
	createPostgresChannelBus,
	createPostgresClusterBus
} from '../src/index';

type Captured = { channel?: string; payload?: string };

const makeMockSql = (captured: Captured) => {
	// `postgres` exposes a tag-template + chained methods. For envelope-
	// roundtrip we only need `select pg_notify(...)`, plus a fake `listen`
	// returning a never-firing handle.
	const sql = ((strings: TemplateStringsArray, ...values: unknown[]) => {
		const joined = strings.join('?');
		if (joined.includes('pg_notify')) {
			captured.channel = values[0] as string;
			captured.payload = values[1] as string;
		}
		return Promise.resolve([]);
	}) as unknown as Parameters<typeof createPostgresClusterBus>[0]['sql'];

	(sql as unknown as { listen: unknown }).listen = (
		_channel: string,
		_handler: (payload: string) => void,
	) => Promise.resolve({ unlisten: () => Promise.resolve() });

	(sql as unknown as { unsafe: unknown }).unsafe = () => Promise.resolve([]);

	return sql;
};

describe('envelope roundtrip — originVersion (sync 1.17.0+)', () => {
	test('originVersion is preserved through publish + decode', async () => {
		const captured: Captured = {};
		const sql = makeMockSql(captured);
		const bus = createPostgresClusterBus({ sql });

		const message: ClusterMessage = {
			changes: [
				{
					change: { op: 'insert', row: { id: 1, title: 'x' } },
					table: 'tasks',
				},
			],
			origin: 'engine-A',
			originVersion: 42,
		};

		await bus.publish(message);
		expect(captured.payload).toBeDefined();

		const envelope = JSON.parse(captured.payload!);
		expect(envelope.kind).toBe('inline');
		expect(envelope.message.origin).toBe('engine-A');
		expect(envelope.message.originVersion).toBe(42);
		expect(envelope.message.changes).toEqual(message.changes);
	});

	test('omitted originVersion (pre-1.17 callers) is left undefined', async () => {
		const captured: Captured = {};
		const sql = makeMockSql(captured);
		const bus = createPostgresClusterBus({ sql });

		const message: ClusterMessage = {
			changes: [
				{ change: { op: 'insert', row: { id: 1 } }, table: 'tasks' },
			],
			origin: 'engine-A',
			// originVersion intentionally omitted
		};
		await bus.publish(message);
		const envelope = JSON.parse(captured.payload!);
		expect(envelope.message.originVersion).toBeUndefined();
		// Receiving engine on 1.17+ falls back to version 0 in this case,
		// matching pre-1.17 behavior (cross-instance resume → snapshot).
	});

	test('default channel used when none passed', async () => {
		const captured: Captured = {};
		const sql = makeMockSql(captured);
		const bus = createPostgresClusterBus({ sql });

		await bus.publish({
			changes: [],
			origin: 'engine-A',
			originVersion: 1,
		});
		expect(captured.channel).toBe('absolutejs_sync_cluster');
	});

	test('custom channel respected', async () => {
		const captured: Captured = {};
		const sql = makeMockSql(captured);
		const bus = createPostgresClusterBus({
			channel: 'demo:tenants',
			sql,
		});
		await bus.publish({
			changes: [],
			origin: 'engine-A',
			originVersion: 1,
		});
		expect(captured.channel).toBe('demo:tenants');
	});
});

describe('typed channel bus', () => {
	test('round-trips a non-Sync payload without inventing row changes', async () => {
		const captured: Captured = {};
		const bus = createPostgresChannelBus<{ requestId: string }>({
			sql: makeMockSql(captured)
		});
		await bus.publish({ requestId: 'elicit_123' });
		const envelope = JSON.parse(captured.payload!);
		expect(envelope.message).toEqual({ requestId: 'elicit_123' });
	});
});
