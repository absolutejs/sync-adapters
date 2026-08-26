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
	SyncLocalStoreSchema,
	SyncLocalStoreSchemaStatus,
	SyncLocalTransaction
} from '@absolutejs/sync/client';
import {
	migrateSyncLocalCollectionRecord,
	migrateSyncLocalMutationRecord,
	resolveSyncLocalMigrations
} from '@absolutejs/sync/client';
import type {
	DeviceLifecycleCapability,
	DeviceNetworkCapability,
	DeviceSubscription
} from '@absolutejs/devices';
import type { SyncClient } from '@absolutejs/sync/client';
import { registerPlugin } from '@capacitor/core';

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
	storageSchema?: SyncLocalStoreSchema;
};

const SCHEMA = `
CREATE TABLE IF NOT EXISTS absolute_sync_schema (
  singleton_id INTEGER PRIMARY KEY NOT NULL CHECK (singleton_id = 1),
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

const parseRecord = <T>(value: unknown, label: string): T | undefined => {
	if (value === undefined) return undefined;
	if (typeof value !== 'string')
		throw new Error(
			`Capacitor Sync SQLite returned invalid ${label} JSON.`
		);
	try {
		return JSON.parse(value) as T;
	} catch (cause) {
		throw new Error(
			`Capacitor Sync SQLite could not parse ${label} JSON.`,
			{
				cause
			}
		);
	}
};

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
	storageSchema = { version: 1 }
}: CapacitorSyncLocalStoreOptions = {}): SyncLocalStore => {
	if (databaseName.length === 0)
		throw new TypeError('Capacitor Sync databaseName cannot be empty.');
	let schemaStatus: SyncLocalStoreSchemaStatus | undefined;
	const prepareSchema = async (
		database: CapacitorSyncSqliteConnection
	): Promise<void> => {
		await database.beginTransaction();
		try {
			const saved = (
				await database.query(
					'SELECT logical_version FROM absolute_sync_schema WHERE singleton_id = 1 LIMIT 1'
				)
			).values?.[0]?.logical_version;
			const storedVersion = typeof saved === 'number' ? saved : 1;
			const resolved = resolveSyncLocalMigrations(
				storedVersion,
				storageSchema
			);
			if (resolved.steps.length > 0) {
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
						'collection'
					);
					if (record === undefined)
						throw new Error(
							'Capacitor Sync SQLite returned a missing collection record.'
						);
					const migrated = migrateSyncLocalCollectionRecord(
						record,
						{ key, namespace },
						resolved.steps
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
							[JSON.stringify(migrated), namespace, key],
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
						'mutation'
					);
					if (record === undefined)
						throw new Error(
							'Capacitor Sync SQLite returned a missing mutation record.'
						);
					const migrated = migrateSyncLocalMutationRecord(
						record,
						{ key: operationId, namespace },
						resolved.steps
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
								JSON.stringify(migrated),
								namespace,
								operationId
							],
							false
						);
				}
			}
			await database.run(
				'INSERT INTO absolute_sync_schema (singleton_id, logical_version) VALUES (1, ?) ON CONFLICT(singleton_id) DO UPDATE SET logical_version = excluded.logical_version',
				[resolved.targetVersion],
				false
			);
			await database.commitTransaction();
			schemaStatus = {
				minimumCompatibleVersion: resolved.minimumCompatibleVersion,
				state: 'ready',
				storedVersion: resolved.targetVersion,
				targetVersion: resolved.targetVersion
			};
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
		connectionPromise ??= Promise.resolve(createConnection()).then(
			async (database) => {
				await database.execute(SCHEMA);
				await prepareSchema(database);
				return database;
			}
		);
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
						'collection'
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
								'collection'
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
						[namespace, key, JSON.stringify(record)],
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
								'mutation'
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
						'mutation'
					),
				putMutation: async (record) => {
					writable();
					await database.run(
						'INSERT INTO absolute_sync_mutations (namespace, operation_id, created_at, record_json) VALUES (?, ?, ?, ?) ON CONFLICT(namespace, operation_id) DO UPDATE SET created_at = excluded.created_at, record_json = excluded.record_json',
						[
							namespace,
							record.operationId,
							record.createdAt,
							JSON.stringify(record)
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
				const result = await run(tx);
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
