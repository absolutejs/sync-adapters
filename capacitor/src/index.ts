import {
	CapacitorSQLite,
	SQLiteConnection,
	type SQLiteDBConnection
} from '@capacitor-community/sqlite';
import type {
	LocalCollectionRecord,
	LocalMutationRecord,
	SyncLocalStore,
	SyncLocalStoreMode,
	SyncLocalStoreSchemaInput,
	SyncLocalStoreSchemaStatus,
	SyncLocalProtectionProvider,
	SyncLocalRecordProtector,
	SyncLocalTransaction
} from '@absolutejs/sync/client';
import {
	createSyncLocalSchemaStatus,
	migrateSyncLocalCollectionRecord,
	migrateSyncLocalMutationRecord,
	resolveSyncLocalDataPolicy,
	resolveSyncLocalSchemaComponents,
	runSyncLocalPolicyTransaction
} from '@absolutejs/sync/client';
import type {
	DeviceLifecycleCapability,
	DeviceNetworkCapability,
	DeviceSubscription
} from '@absolutejs/devices';
import type { SyncClient } from '@absolutejs/sync/client';
import { registerPlugin } from '@capacitor/core';
import { createCapacitorSecureStorage } from '@absolutejs/devices-capacitor';
import type { DeviceSecureStorageCapability } from '@absolutejs/devices';
import { gcm } from '@noble/ciphers/aes.js';
import { randomBytes } from '@noble/ciphers/utils.js';

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const base64 = (value: Uint8Array) => {
	let binary = '';
	for (const byte of value) binary += String.fromCharCode(byte);
	return btoa(binary);
};
const unbase64 = (value: string) =>
	Uint8Array.from(atob(value), (character) => character.charCodeAt(0));

export type CapacitorSyncProtectionOptions = {
	secureStorage?: DeviceSecureStorageCapability;
};

/** AES-256-GCM record protection whose key is sealed by Keychain/Keystore. */
export const createCapacitorSyncProtection = (
	options: CapacitorSyncProtectionOptions = {}
): SyncLocalProtectionProvider => {
	const storage =
		options.secureStorage ??
		createCapacitorSecureStorage({ prefix: 'absolutejs.sync.' });
	const keyName = 'data-key.v1';
	return {
		prepare: async (): Promise<SyncLocalRecordProtector> => {
			const capability = await storage.capability();
			if (!capability.available)
				throw new Error(
					'Native Sync data protection requires persistent Keychain/Keystore storage.'
				);
			const load = async () => {
				const existing = await storage.get(keyName);
				if (existing) return unbase64(existing);
				const created = randomBytes(32);
				await storage.set(keyName, base64(created));
				return created;
			};
			const key = storage.withLock
				? await storage.withLock(keyName, load)
				: await load();
			if (key.byteLength !== 32)
				throw new Error('Native Sync data-protection key is invalid.');
			const additionalData = (context: {
				kind: string;
				name: string;
				namespace: string;
			}) =>
				textEncoder.encode(
					`absolute-sync-v1\u0000${context.kind}\u0000${context.namespace}\u0000${context.name}`
				);
			return {
				id: 'aes-256-gcm-v1',
				open: (value, context) => {
					const bytes = unbase64(value);
					const nonce = bytes.slice(0, 12);
					return textDecoder.decode(
						gcm(key, nonce, additionalData(context)).decrypt(
							bytes.slice(12)
						)
					);
				},
				seal: (value, context) => {
					const nonce = randomBytes(12);
					const encrypted = gcm(
						key,
						nonce,
						additionalData(context)
					).encrypt(textEncoder.encode(value));
					const output = new Uint8Array(
						nonce.length + encrypted.length
					);
					output.set(nonce);
					output.set(encrypted, nonce.length);
					return base64(output);
				}
			};
		}
	};
};

export type CapacitorBackgroundSyncConfig = {
	endpoint: string;
	issuer: string;
	clientId: string;
	namespace: string;
	databaseName?: string;
	intervalMinutes?: number;
	maxMutations?: number;
	maxPulls?: number;
	maxAttempts?: number;
};

export type CapacitorBackgroundSyncStatus = {
	configured: boolean;
	running: boolean;
	lastRunAt?: number;
	lastError?: string;
	lastAcknowledged?: number;
	lastPulled?: number;
};

export type AbsoluteBackgroundSyncPlugin = {
	configure(options: CapacitorBackgroundSyncConfig): Promise<void>;
	clear(): Promise<void>;
	runNow(): Promise<CapacitorBackgroundSyncStatus>;
	status(): Promise<CapacitorBackgroundSyncStatus>;
};

export const AbsoluteBackgroundSync =
	registerPlugin<AbsoluteBackgroundSyncPlugin>('AbsoluteBackgroundSync');

/** Configure the managed scheduler. Auth credentials remain in the native vault. */
export const configureCapacitorBackgroundSync = (
	options: CapacitorBackgroundSyncConfig,
	plugin: AbsoluteBackgroundSyncPlugin = AbsoluteBackgroundSync
) => plugin.configure(options);

export type CapacitorSyncSqliteConnection = Pick<
	SQLiteDBConnection,
	| 'beginTransaction'
	| 'commitTransaction'
	| 'execute'
	| 'open'
	| 'query'
	| 'rollbackTransaction'
	| 'run'
>;

export type CapacitorSyncSqliteFactory = () =>
	| CapacitorSyncSqliteConnection
	| Promise<CapacitorSyncSqliteConnection>;

export type CapacitorSyncLocalStoreOptions = {
	/** Defaults to `absolutejs-sync-local-v1`. */
	databaseName?: string;
	/** Injection seam for tests and custom SQLCipher provisioning. */
	connection?: CapacitorSyncSqliteFactory;
	/** Same generated logical migration plan used by web IndexedDB. */
	storageSchema?: SyncLocalStoreSchemaInput;
	protection?: SyncLocalProtectionProvider;
	now?: () => number;
};

const SCHEMA = `
CREATE TABLE IF NOT EXISTS absolute_sync_schema (
  singleton_id INTEGER PRIMARY KEY NOT NULL CHECK (singleton_id = 1),
  logical_version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS absolute_sync_schema_components (
  component_id TEXT PRIMARY KEY NOT NULL,
  logical_version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS absolute_sync_metadata (
  namespace TEXT PRIMARY KEY NOT NULL,
  installation_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS absolute_sync_collections (
  namespace TEXT NOT NULL,
  collection_key TEXT NOT NULL,
  record_json TEXT NOT NULL,
  PRIMARY KEY (namespace, collection_key)
);
CREATE TABLE IF NOT EXISTS absolute_sync_mutations (
  namespace TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  record_json TEXT NOT NULL,
  PRIMARY KEY (namespace, operation_id)
);
CREATE INDEX IF NOT EXISTS absolute_sync_mutations_order
  ON absolute_sync_mutations (namespace, created_at, operation_id);
`;

type SqliteRow = Record<string, unknown>;

const firstString = (rows: SqliteRow[] | undefined, field: string) => {
	const value = rows?.[0]?.[field];
	return typeof value === 'string' ? value : undefined;
};

type ProtectedRecordEnvelope = {
	__absoluteSyncProtected: {
		name: string;
		protector: string;
		value: string;
	};
};

const parseRecord = <T>(
	value: unknown,
	label: string,
	context?: { kind: 'collection' | 'mutation'; namespace: string },
	protector?: SyncLocalRecordProtector
): T | undefined => {
	if (value === undefined) return undefined;
	if (typeof value !== 'string')
		throw new Error(
			`Capacitor Sync SQLite returned invalid ${label} JSON.`
		);
	try {
		const parsed: unknown = JSON.parse(value);
		if (
			typeof parsed === 'object' &&
			parsed !== null &&
			'__absoluteSyncProtected' in parsed
		) {
			const envelope = (parsed as ProtectedRecordEnvelope)
				.__absoluteSyncProtected;
			if (!context || !protector || protector.id !== envelope.protector)
				throw new Error(
					`Capacitor Sync ${label} requires unavailable protection provider "${envelope.protector}".`
				);
			return JSON.parse(
				protector.open(envelope.value, {
					...context,
					name: envelope.name
				})
			) as T;
		}
		return parsed as T;
	} catch (cause) {
		throw new Error(
			`Capacitor Sync SQLite could not parse ${label} JSON.`,
			{
				cause
			}
		);
	}
};

const serializeRecord = (
	value: LocalCollectionRecord | LocalMutationRecord,
	context: {
		kind: 'collection' | 'mutation';
		name: string;
		namespace: string;
	},
	protector?: SyncLocalRecordProtector
) =>
	protector
		? JSON.stringify({
				__absoluteSyncProtected: {
					name: context.name,
					protector: protector.id,
					value: protector.seal(JSON.stringify(value), context)
				}
			} satisfies ProtectedRecordEnvelope)
		: JSON.stringify(value);

const defaultConnection = async (
	databaseName: string
): Promise<CapacitorSyncSqliteConnection> => {
	const sqlite = new SQLiteConnection(CapacitorSQLite);
	const existing = await sqlite.isConnection(databaseName, false);
	const connection = existing.result
		? await sqlite.retrieveConnection(databaseName, false)
		: await sqlite.createConnection(
				databaseName,
				false,
				'no-encryption',
				1,
				false
			);
	await connection.open();
	return connection;
};

const requireNamespace = (namespace: string) => {
	if (namespace.length === 0)
		throw new Error('Sync local-store namespace must not be empty');
};

/**
 * Native SQLite implementation of Sync's principal-partitioned atomic cache
 * and mutation outbox. All calls are serialized because a Capacitor database
 * connection owns one explicit transaction at a time.
 */
export const createCapacitorSyncLocalStore = ({
	databaseName = 'absolutejs-sync-local-v1',
	connection: createConnection = () => defaultConnection(databaseName),
	storageSchema = { version: 1 },
	protection,
	now = Date.now
}: CapacitorSyncLocalStoreOptions = {}): SyncLocalStore => {
	if (databaseName.length === 0)
		throw new TypeError('Capacitor Sync databaseName cannot be empty.');
	const localData = resolveSyncLocalDataPolicy(storageSchema);
	let protectorPromise: Promise<SyncLocalRecordProtector> | undefined;
	const prepareProtector = () => (protectorPromise ??= protection?.prepare());
	let schemaStatus: SyncLocalStoreSchemaStatus | undefined;
	const prepareSchema = async (
		database: CapacitorSyncSqliteConnection,
		protector: SyncLocalRecordProtector | undefined
	): Promise<void> => {
		await database.beginTransaction();
		try {
			const legacySaved = (
				await database.query(
					'SELECT logical_version FROM absolute_sync_schema WHERE singleton_id = 1 LIMIT 1'
				)
			).values?.[0]?.logical_version;
			const componentRows = (
				await database.query(
					'SELECT component_id, logical_version FROM absolute_sync_schema_components ORDER BY component_id'
				)
			).values;
			const storedVersions: Record<string, number> = {};
			for (const row of componentRows ?? []) {
				if (
					typeof row.component_id !== 'string' ||
					typeof row.logical_version !== 'number'
				)
					throw new Error(
						'Capacitor Sync SQLite returned an invalid schema component ledger.'
					);
				storedVersions[row.component_id] = row.logical_version;
			}
			if (
				storedVersions['@absolutejs/app'] === undefined &&
				typeof legacySaved === 'number'
			)
				storedVersions['@absolutejs/app'] = legacySaved;
			const resolved = resolveSyncLocalSchemaComponents(
				storedVersions,
				storageSchema
			);
			const steps = resolved.components.flatMap(
				(component) => component.steps
			);
			if (steps.length > 0) {
				const collections = (
					await database.query(
						'SELECT namespace, collection_key, record_json FROM absolute_sync_collections ORDER BY namespace, collection_key'
					)
				).values;
				for (const row of collections ?? []) {
					const namespace = row.namespace;
					const key = row.collection_key;
					if (
						typeof namespace !== 'string' ||
						typeof key !== 'string'
					)
						throw new Error(
							'Capacitor Sync SQLite returned an invalid collection identity.'
						);
					const record = parseRecord<LocalCollectionRecord>(
						row.record_json,
						'collection',
						{ kind: 'collection', namespace },
						protector
					);
					if (record === undefined)
						throw new Error(
							'Capacitor Sync SQLite returned a missing collection record.'
						);
					const migrated = migrateSyncLocalCollectionRecord(
						record,
						{ key, namespace },
						steps
					);
					if (migrated === null)
						await database.run(
							'DELETE FROM absolute_sync_collections WHERE namespace = ? AND collection_key = ?',
							[namespace, key],
							false
						);
					else
						await database.run(
							'UPDATE absolute_sync_collections SET record_json = ? WHERE namespace = ? AND collection_key = ?',
							[
								serializeRecord(
									migrated,
									{
										kind: 'collection',
										name: migrated.collection ?? key,
										namespace
									},
									protector
								),
								namespace,
								key
							],
							false
						);
				}

				const mutations = (
					await database.query(
						'SELECT namespace, operation_id, record_json FROM absolute_sync_mutations ORDER BY namespace, operation_id'
					)
				).values;
				for (const row of mutations ?? []) {
					const namespace = row.namespace;
					const operationId = row.operation_id;
					if (
						typeof namespace !== 'string' ||
						typeof operationId !== 'string'
					)
						throw new Error(
							'Capacitor Sync SQLite returned an invalid mutation identity.'
						);
					const record = parseRecord<LocalMutationRecord>(
						row.record_json,
						'mutation',
						{ kind: 'mutation', namespace },
						protector
					);
					if (record === undefined)
						throw new Error(
							'Capacitor Sync SQLite returned a missing mutation record.'
						);
					const migrated = migrateSyncLocalMutationRecord(
						record,
						{ key: operationId, namespace },
						steps
					);
					if (migrated === null)
						await database.run(
							'DELETE FROM absolute_sync_mutations WHERE namespace = ? AND operation_id = ?',
							[namespace, operationId],
							false
						);
					else
						await database.run(
							'UPDATE absolute_sync_mutations SET created_at = ?, record_json = ? WHERE namespace = ? AND operation_id = ?',
							[
								migrated.createdAt,
								serializeRecord(
									migrated,
									{
										kind: 'mutation',
										name: migrated.name,
										namespace
									},
									protector
								),
								namespace,
								operationId
							],
							false
						);
				}
			}
			for (const component of resolved.components)
				await database.run(
					'INSERT INTO absolute_sync_schema_components (component_id, logical_version) VALUES (?, ?) ON CONFLICT(component_id) DO UPDATE SET logical_version = excluded.logical_version',
					[component.id, component.targetVersion],
					false
				);
			const app = resolved.components.find(
				(component) => component.id === '@absolutejs/app'
			);
			if (app !== undefined)
				await database.run(
					'INSERT INTO absolute_sync_schema (singleton_id, logical_version) VALUES (1, ?) ON CONFLICT(singleton_id) DO UPDATE SET logical_version = excluded.logical_version',
					[app.targetVersion],
					false
				);
			await database.commitTransaction();
			schemaStatus = createSyncLocalSchemaStatus(
				resolved.components,
				resolved.orphanedComponents,
				'components' in storageSchema
			);
		} catch (error) {
			try {
				await database.rollbackTransaction();
			} catch (rollbackError) {
				throw new AggregateError(
					[error, rollbackError],
					'Capacitor Sync SQLite migration and rollback both failed'
				);
			}
			throw error;
		}
	};

	let connectionPromise: Promise<CapacitorSyncSqliteConnection> | undefined;
	const connection = () => {
		connectionPromise ??= Promise.all([
			Promise.resolve(createConnection()),
			prepareProtector()
		]).then(async ([database, protector]) => {
			await database.execute(SCHEMA);
			await prepareSchema(database, protector);
			return database;
		});
		return connectionPromise;
	};
	let tail = Promise.resolve();
	const locked = async <R>(run: () => Promise<R>): Promise<R> => {
		let release!: () => void;
		const previous = tail;
		tail = new Promise<void>((resolve) => {
			release = resolve;
		});
		await previous;
		try {
			return await run();
		} finally {
			release();
		}
	};

	const transaction = async <R>(
		namespace: string,
		mode: SyncLocalStoreMode,
		run: (tx: SyncLocalTransaction) => Promise<R>
	): Promise<R> => {
		requireNamespace(namespace);
		return locked(async () => {
			const database = await connection();
			const protector = await prepareProtector();
			await database.beginTransaction();
			const writable = () => {
				if (mode !== 'readwrite')
					throw new Error(
						'Cannot write in a readonly Sync local transaction'
					);
			};
			const tx: SyncLocalTransaction = {
				getInstallationId: async () =>
					firstString(
						(
							await database.query(
								'SELECT installation_id FROM absolute_sync_metadata WHERE namespace = ? LIMIT 1',
								[namespace]
							)
						).values,
						'installation_id'
					),
				setInstallationId: async (installationId) => {
					writable();
					if (installationId.length === 0)
						throw new Error(
							'Sync installation id must not be empty'
						);
					await database.run(
						'INSERT INTO absolute_sync_metadata (namespace, installation_id) VALUES (?, ?) ON CONFLICT(namespace) DO UPDATE SET installation_id = excluded.installation_id',
						[namespace, installationId],
						false
					);
				},
				getCollection: async <T>(key: string) =>
					parseRecord<LocalCollectionRecord<T>>(
						(
							await database.query(
								'SELECT record_json FROM absolute_sync_collections WHERE namespace = ? AND collection_key = ? LIMIT 1',
								[namespace, key]
							)
						).values?.[0]?.record_json,
						'collection',
						{ kind: 'collection', namespace },
						protector
					),
				listCollections: async () => {
					const rows = (
						await database.query(
							'SELECT collection_key, record_json FROM absolute_sync_collections WHERE namespace = ? ORDER BY collection_key ASC',
							[namespace]
						)
					).values;
					return (rows ?? [])
						.map((row) => {
							const key = row.collection_key;
							const record = parseRecord<LocalCollectionRecord>(
								row.record_json,
								'collection',
								{ kind: 'collection', namespace },
								protector
							);
							return typeof key === 'string' && record
								? { key, record }
								: undefined;
						})
						.filter(
							(
								entry
							): entry is {
								key: string;
								record: LocalCollectionRecord;
							} => entry !== undefined
						);
				},
				putCollection: async (key, record) => {
					writable();
					await database.run(
						'INSERT INTO absolute_sync_collections (namespace, collection_key, record_json) VALUES (?, ?, ?) ON CONFLICT(namespace, collection_key) DO UPDATE SET record_json = excluded.record_json',
						[
							namespace,
							key,
							serializeRecord(
								record,
								{
									kind: 'collection',
									name: record.collection ?? key,
									namespace
								},
								protector
							)
						],
						false
					);
				},
				deleteCollection: async (key) => {
					writable();
					await database.run(
						'DELETE FROM absolute_sync_collections WHERE namespace = ? AND collection_key = ?',
						[namespace, key],
						false
					);
				},
				listMutations: async () => {
					const rows = (
						await database.query(
							'SELECT record_json FROM absolute_sync_mutations WHERE namespace = ? ORDER BY created_at ASC, operation_id ASC',
							[namespace]
						)
					).values;
					return (rows ?? [])
						.map((row) =>
							parseRecord<LocalMutationRecord>(
								row.record_json,
								'mutation',
								{ kind: 'mutation', namespace },
								protector
							)
						)
						.filter(
							(record): record is LocalMutationRecord =>
								record !== undefined
						);
				},
				getMutation: async (operationId) =>
					parseRecord<LocalMutationRecord>(
						(
							await database.query(
								'SELECT record_json FROM absolute_sync_mutations WHERE namespace = ? AND operation_id = ? LIMIT 1',
								[namespace, operationId]
							)
						).values?.[0]?.record_json,
						'mutation',
						{ kind: 'mutation', namespace },
						protector
					),
				putMutation: async (record) => {
					writable();
					await database.run(
						'INSERT INTO absolute_sync_mutations (namespace, operation_id, created_at, record_json) VALUES (?, ?, ?, ?) ON CONFLICT(namespace, operation_id) DO UPDATE SET created_at = excluded.created_at, record_json = excluded.record_json',
						[
							namespace,
							record.operationId,
							record.createdAt,
							serializeRecord(
								record,
								{
									kind: 'mutation',
									name: record.name,
									namespace
								},
								protector
							)
						],
						false
					);
				},
				deleteMutation: async (operationId) => {
					writable();
					await database.run(
						'DELETE FROM absolute_sync_mutations WHERE namespace = ? AND operation_id = ?',
						[namespace, operationId],
						false
					);
				}
			};
			try {
				const result = await runSyncLocalPolicyTransaction({
					mode,
					now: now(),
					policy: localData,
					protected: protector !== undefined,
					raw: tx,
					run
				});
				await database.commitTransaction();
				return result;
			} catch (error) {
				try {
					await database.rollbackTransaction();
				} catch (rollbackError) {
					throw new AggregateError(
						[error, rollbackError],
						'Capacitor Sync SQLite transaction and rollback both failed'
					);
				}
				throw error;
			}
		});
	};

	return {
		transaction,
		getSchemaStatus: async () => {
			await connection();
			if (schemaStatus === undefined)
				throw new Error('Capacitor Sync schema was not prepared.');
			return { ...schemaStatus };
		},
		deleteNamespace: async (namespace) => {
			requireNamespace(namespace);
			await locked(async () => {
				const database = await connection();
				await database.beginTransaction();
				try {
					for (const table of [
						'absolute_sync_metadata',
						'absolute_sync_collections',
						'absolute_sync_mutations'
					])
						await database.run(
							`DELETE FROM ${table} WHERE namespace = ?`,
							[namespace],
							false
						);
					await database.commitTransaction();
				} catch (error) {
					await database.rollbackTransaction();
					throw error;
				}
			});
		}
	};
};

export type CapacitorSyncLifecycleOptions = {
	client: Pick<SyncClient, 'reconnect'> & Partial<Pick<SyncClient, 'flush'>>;
	lifecycle: DeviceLifecycleCapability;
	network: DeviceNetworkCapability;
	/** Finite outbox budget after a wake-up. Defaults to 10 seconds. */
	flushTimeoutMs?: number;
	/** Lifecycle flush failures are observable without becoming unhandled. */
	onError?: (error: unknown) => void;
};

/** Refreshes the socket/ticket and runs a bounded outbox flush after wake-up. */
export const installCapacitorSyncLifecycle = async ({
	client,
	lifecycle,
	network,
	flushTimeoutMs = 10_000,
	onError
}: CapacitorSyncLifecycleOptions): Promise<DeviceSubscription> => {
	if (!Number.isFinite(flushTimeoutMs) || flushTimeoutMs < 0)
		throw new TypeError(
			'Capacitor Sync flushTimeoutMs must be a non-negative number.'
		);
	const wake = () => {
		client.reconnect();
		void client
			.flush?.({ timeoutMs: flushTimeoutMs })
			.catch((error) => onError?.(error));
	};
	const removers = await Promise.all([
		lifecycle.onResume?.(wake) ?? (() => undefined),
		network.onChange((status) => {
			if (status.connected) wake();
		})
	]);
	let active = true;
	return async () => {
		if (!active) return;
		active = false;
		await Promise.all(removers.map((remove) => remove()));
	};
};
