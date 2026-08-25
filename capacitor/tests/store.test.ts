import { expect, test } from 'bun:test';
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
