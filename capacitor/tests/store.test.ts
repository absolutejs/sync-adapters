import { expect, test } from 'bun:test';
import { SyncLocalStoreSchemaError } from '@absolutejs/sync/client';
import { assertSyncLocalStoreConformance } from '@absolutejs/sync/testing';
import { createCapacitorSyncLocalStore } from '../src/index';
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
