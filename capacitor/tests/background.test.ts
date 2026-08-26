import { expect, test } from 'bun:test';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { configureCapacitorBackgroundSync } from '../src';

test('forwards managed background configuration to the native plugin', async () => {
	let received: unknown;
	await configureCapacitorBackgroundSync(
		{
			clientId: 'native-client',
			endpoint: 'https://app.example/__absolute/sync/background',
			issuer: 'https://app.example',
			namespace: 'principal'
		},
		{
			configure: async (value) => {
				received = value;
			},
			clear: async () => undefined,
			runNow: async () => ({ configured: true, running: false }),
			status: async () => ({ configured: true, running: false })
		}
	);
	expect(received).toMatchObject({
		clientId: 'native-client',
		namespace: 'principal'
	});
});

test('native workers fail closed to the issuer origin and use the secure vault', async () => {
	const root = join(import.meta.dir, '..');
	const android = await readFile(
		join(
			root,
			'android/src/main/java/js/absolute/sync/AbsoluteBackgroundSyncEngine.java'
		),
		'utf8'
	);
	const ios = await readFile(
		join(
			root,
			'ios/Sources/AbsoluteSyncCapacitor/AbsoluteBackgroundSyncEngine.swift'
		),
		'utf8'
	);
	for (const source of [android, ios]) {
		expect(source).toContain('AbsoluteSecureStorageVault');
		expect(source).toContain('refresh_token');
		expect(source).toContain('issuer origin');
		expect(source).toContain('absolute_sync_mutations');
		expect(source).toContain('absolute_sync_collections');
		expect(source).toContain('maxPulls');
		expect(source).toContain('Background request is too large.');
		expect(source).toContain('advertised an unsafe token endpoint');
		expect(source).toContain('setIfLease');
		expect(source).toContain('absolutejs.sync.data-key.v1');
		expect(source).toContain('__absoluteSyncProtected');
		expect(source).toContain('aes-256-gcm-v1');
		expect(source).toContain('absolute-sync-v1');
		expect(source).toContain('Protected Sync data key is unavailable.');
	}
});
