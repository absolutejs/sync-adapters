import { expect, test } from 'bun:test';
import { SyncLocalStoreSchemaError } from '@absolutejs/sync/client';
import { assertSyncLocalStoreConformance } from '@absolutejs/sync/testing';
import {
	createCapacitorSyncLocalStore,
	createCapacitorSyncProtection
} from '../src/index';
import { createFakeSqliteConnection } from './support/fakeSqlite';

test('Capacitor SQLite passes the shared SyncLocalStore contract', async () => {
	const connection = createFakeSqliteConnection();
	await expect(
		assertSyncLocalStoreConformance({
			store: createCapacitorSyncLocalStore({
				connection: () => connection
			})
		})
	).resolves.toBeUndefined();
});

test('Capacitor SQLite captures generated conflict policy in the native outbox', async () => {
	const connection = createFakeSqliteConnection();
	const store = createCapacitorSyncLocalStore({
		connection: () => connection,
		storageSchema: {
			components: [
				{
					id: '@absolutejs/app',
					localData: {
						mutations: [
							{
								conflict: {
									maxAttempts: 2,
									strategy: 'client-wins'
								},
								match: 'tasks:*'
							}
						]
					},
					version: 1
				}
			]
		}
	});
	await store.transaction('account-a', 'readwrite', async (tx) => {
		const conflictPolicy =
			tx.resolveMutationPolicy?.('tasks:update').conflict;
		await tx.putMutation({
			args: { id: 1 },
			attempts: 0,
			createdAt: 1,
			...(conflictPolicy ? { conflictPolicy } : {}),
			inverse: [],
			name: 'tasks:update',
			operationId: 'install:intent',
			optimistic: []
		});
	});
	await expect(
		store.transaction('account-a', 'readonly', (tx) =>
			tx.getMutation('install:intent')
		)
	).resolves.toMatchObject({
		conflictPolicy: { maxAttempts: 2, strategy: 'client-wins' }
	});
});

test('Capacitor SQLite encrypts records with a vault-held key', async () => {
	const connection = createFakeSqliteConnection();
	const values = new Map<string, string>();
	const protection = createCapacitorSyncProtection({
		secureStorage: {
			capability: async () => ({ available: true, fidelity: 'native' }),
			clear: async () => values.clear(),
			get: async (key) => values.get(key) ?? null,
			keys: async () => [...values.keys()],
			remove: async (key) => void values.delete(key),
			set: async (key, value) => void values.set(key, value),
			withLock: async (_key, run) => run()
		}
	});
	const schema = {
		components: [
			{
				id: '@absolutejs/app',
				version: 1,
				localData: {
					collections: [
						{ match: 'private', protection: 'required' as const }
					]
				}
			}
		]
	};
	const store = createCapacitorSyncLocalStore({
		connection: () => connection,
		protection,
		storageSchema: schema
	});
	await store.transaction('account-a', 'readwrite', (tx) =>
		tx.putCollection('private', {
			collection: 'private',
			rows: [{ id: 1, secret: 'not-on-disk' }],
			version: 1
		})
	);
	const raw = await connection.query(
		'SELECT record_json FROM absolute_sync_collections'
	);
	expect(String(raw.values?.[0]?.record_json)).toContain(
		'__absoluteSyncProtected'
	);
	expect(String(raw.values?.[0]?.record_json)).not.toContain('not-on-disk');
	const reopened = createCapacitorSyncLocalStore({
		connection: () => connection,
		protection,
		storageSchema: schema
	});
	await expect(
		reopened.transaction('account-a', 'readonly', (tx) =>
			tx.getCollection('private')
		)
	).resolves.toMatchObject({ rows: [{ secret: 'not-on-disk' }] });
});

test('Capacitor SQLite applies the same migration across principal partitions', async () => {
	const connection = createFakeSqliteConnection();
	const legacy = createCapacitorSyncLocalStore({
		connection: () => connection
	});
	for (const namespace of ['account-a', 'account-b'])
		await legacy.transaction(namespace, 'readwrite', async (tx) => {
			await tx.putCollection('tasks', {
				rows: [{ id: 1, title: namespace }],
				version: 1
			});
			await tx.putMutation({
				args: {},
				attempts: 0,
				createdAt: 1,
				inverse: [],
				name: 'tasks:create',
				operationId: `${namespace}:op-1`,
				optimistic: []
			});
		});

	const upgraded = createCapacitorSyncLocalStore({
		connection: () => connection,
		storageSchema: {
			version: 3,
			migrations: [
				{
					toVersion: 2,
					migrateCollection: (record, context) => ({
						...record,
						rows: record.rows.map((row) => ({
							...(row as object),
							migratedFor: context.namespace
						}))
					})
				},
				{
					toVersion: 3,
					migrateMutation: (record) => ({
						...record,
						lastError: 'retained through upgrade'
					})
				}
			]
		}
	});
	await expect(upgraded.getSchemaStatus?.()).resolves.toEqual({
		minimumCompatibleVersion: 1,
		state: 'ready',
		storedVersion: 3,
		targetVersion: 3
	});
	for (const namespace of ['account-a', 'account-b']) {
		const state = await upgraded.transaction(
			namespace,
			'readonly',
			async (tx) => ({
				collection: await tx.getCollection('tasks'),
				mutations: await tx.listMutations()
			})
		);
		expect(state.collection?.rows).toEqual([
			{ id: 1, migratedFor: namespace, title: namespace }
		]);
		expect(state.mutations[0]?.lastError).toBe('retained through upgrade');
	}

	const olderRuntime = createCapacitorSyncLocalStore({
		connection: () => connection,
		storageSchema: { version: 2, migrations: [{ toVersion: 2 }] }
	});
	const error = await olderRuntime
		.getSchemaStatus?.()
		.catch((cause) => cause);
	expect(error).toBeInstanceOf(SyncLocalStoreSchemaError);
	expect((error as SyncLocalStoreSchemaError).code).toBe('SCHEMA_TOO_NEW');
});

test('Capacitor SQLite rolls back records and version after a migration crash', async () => {
	const connection = createFakeSqliteConnection();
	const legacy = createCapacitorSyncLocalStore({
		connection: () => connection
	});
	await legacy.transaction('account-a', 'readwrite', async (tx) => {
		await tx.putCollection('first', { rows: [{ id: 1 }], version: 1 });
		await tx.putCollection('second', { rows: [{ id: 2 }], version: 1 });
	});

	const failed = createCapacitorSyncLocalStore({
		connection: () => connection,
		storageSchema: {
			version: 2,
			migrations: [
				{
					toVersion: 2,
					migrateCollection: (record, context) => {
						if (context.key === 'second')
							throw new Error('simulated crash');
						return { ...record, cursor: 'partially-migrated' };
					}
				}
			]
		}
	});
	await expect(failed.getSchemaStatus?.()).rejects.toThrow('simulated crash');

	const recovered = createCapacitorSyncLocalStore({
		connection: () => connection,
		storageSchema: { version: 2, migrations: [{ toVersion: 2 }] }
	});
	await expect(recovered.getSchemaStatus?.()).resolves.toMatchObject({
		storedVersion: 2
	});
	const rows = await recovered.transaction(
		'account-a',
		'readonly',
		async (tx) => [
			await tx.getCollection('first'),
			await tx.getCollection('second')
		]
	);
	expect(rows.map((record) => record?.cursor)).toEqual([
		undefined,
		undefined
	]);
});

test('Capacitor SQLite composes JSON pack schemas and retains orphan ledgers', async () => {
	const connection = createFakeSqliteConnection();
	const legacy = createCapacitorSyncLocalStore({
		connection: () => connection
	});
	await legacy.transaction('account-a', 'readwrite', async (tx) => {
		await tx.putCollection('tasks', {
			collection: 'tasks',
			rows: [{ id: 1, title: 'ship it' }],
			version: 1
		});
	});

	const storageSchema = JSON.parse(
		JSON.stringify({
			components: [
				{ id: '@absolutejs/app', version: 1 },
				{
					id: '@absolutejs/tasks-pack',
					version: 2,
					migrations: [
						{
							toVersion: 2,
							operations: [
								{
									collection: 'tasks',
									field: 'completed',
									type: 'set-default',
									value: false
								}
							]
						}
					]
				},
				{ id: '@absolutejs/labels-pack', version: 1 }
			]
		})
	);
	const upgraded = createCapacitorSyncLocalStore({
		connection: () => connection,
		storageSchema
	});
	await expect(upgraded.getSchemaStatus?.()).resolves.toMatchObject({
		components: [
			{ id: '@absolutejs/app', storedVersion: 1 },
			{ id: '@absolutejs/labels-pack', storedVersion: 1 },
			{ id: '@absolutejs/tasks-pack', storedVersion: 2 }
		]
	});
	const tasks = await upgraded.transaction('account-a', 'readonly', (tx) =>
		tx.getCollection('tasks')
	);
	expect(tasks?.rows).toEqual([
		{ completed: false, id: 1, title: 'ship it' }
	]);

	const withoutTasks = createCapacitorSyncLocalStore({
		connection: () => connection,
		storageSchema: {
			components: [{ id: '@absolutejs/app', version: 1 }]
		}
	});
	await expect(withoutTasks.getSchemaStatus?.()).resolves.toMatchObject({
		orphanedComponents: [
			'@absolutejs/labels-pack',
			'@absolutejs/tasks-pack'
		]
	});
});
