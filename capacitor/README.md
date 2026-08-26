# @absolutejs/sync-capacitor

Native persistence and lifecycle wiring for `@absolutejs/sync` applications
running in Capacitor. It stores confirmed rows, cursors, installation identity,
and the durable mutation outbox in one principal-partitioned SQLite database.
It also exposes namespace-scoped collection discovery so finite native workers
can resume safe `id`-keyed pulls without application-authored worker code.

AbsoluteJS provisions this automatically when Mobile, Sync, and Auth are enabled.
Direct Capacitor applications can opt in explicitly:

```ts
import { createSyncClient } from '@absolutejs/sync/client';
import { lifecycle, network } from '@absolutejs/devices';
import {
	createCapacitorSyncLocalStore,
	installCapacitorSyncLifecycle
} from '@absolutejs/sync-capacitor';

const client = createSyncClient({
	url: 'wss://app.example.com/sync/ws',
	durable: {
		store: createCapacitorSyncLocalStore(),
		namespace: authenticatedPrincipalNamespace
	}
});

const removeLifecycle = await installCapacitorSyncLifecycle({
	client,
	lifecycle,
	network
});
```

Resume and restored-connectivity events refresh the Auth-backed socket and ask
the client to flush within a finite ten-second budget. Explicitly retryable
failures obey the client's delivery ceiling; conflicts and permanent failures
remain in the principal's dead-letter partition for explicit remediation rather
than replaying forever.

The namespace must come from a verified Auth principal, never from an untrusted
route or form value. AbsoluteJS derives an opaque namespace from the verified
issuer, public client ID, and subject. Signing out locks that partition by
removing it from the active runtime; it does not silently destroy offline data.
Signing back in as the same verified principal unlocks the same partition.

## Installed-data upgrades

`storageSchema` accepts the same generated `SyncLocalStoreSchema` or
component bundle used by
`createIndexedDbSyncLocalStore`. Before any foreground transaction begins, the
adapter migrates every principal's collections, durable mutations, and logical
schema marker inside one SQLite transaction. A transform failure or process
death rolls back the entire upgrade; a runtime older than the stored schema
fails closed.

Migration callbacks are synchronous and deterministic. They may replace or
delete persisted records, but cannot change a mutation's stable operation ID.
AbsoluteJS will generate and provision the plan for ordinary applications;
direct Capacitor integrations can pass it explicitly:

```ts
const store = createCapacitorSyncLocalStore({
	storageSchema: {
		version: 2,
		migrations: [
			{
				toVersion: 2,
				migrateCollection(record) {
					return {
						...record,
						rows: record.rows.map((row) => ({
							...(row as object),
							archived: false
						}))
					};
				}
			}
		]
	}
});
```

Absolute composes the app schema and every installed Sync pack into a
deterministic component bundle. SQLite tracks each component independently,
keeps removed-pack ledgers as orphan diagnostics, and migrates all records and
ledger updates in one transaction.

## Managed native background Sync

AbsoluteJS also configures `AbsoluteBackgroundSync` when the application uses
both `@absolutejs/auth` and `@absolutejs/sync`. The native worker is deliberately
finite: Android WorkManager or iOS `BGProcessingTask` wakes it, it refreshes a
short-lived access token, pushes a bounded durable outbox batch, pulls the
foreground client's persisted collection descriptors, commits the response to
the same SQLite database, and exits. Foreground/resume Sync remains the
correctness path because neither operating system guarantees when background
work will run.

The worker does not run application JavaScript and does not expose Capacitor
APIs to a background WebView. Its credential and network boundary is fixed:

- the OAuth refresh token is read from the shared native Keychain/Keystore
  vault and sent only to the issuer-advertised HTTPS token endpoint;
- the resulting bearer token, mutation arguments, and collection parameters
  are sent only to the configured same-origin AbsoluteJS endpoint;
- redirects fail closed, and response bodies are bounded before parsing;
- a rotated refresh token is written back to the vault, while returned Sync
  data is written only to the principal's SQLite partition.

Direct Capacitor applications can configure the worker after resolving a
verified principal:

```ts
import { configureCapacitorBackgroundSync } from '@absolutejs/sync-capacitor';

await configureCapacitorBackgroundSync({
	issuer: 'https://app.example.com',
	clientId: 'mobile-public-client',
	endpoint: 'https://app.example.com/__absolute/sync/background',
	namespace: authenticatedPrincipalNamespace
});
```

Call `AbsoluteBackgroundSync.clear()` on sign-out. On iOS, register
`AbsoluteBackgroundSyncPlugin.registerBackgroundTask()` during application
launch and list `<bundle-id>.absolutejs.background-sync` in
`BGTaskSchedulerPermittedIdentifiers`; the AbsoluteJS mobile CLI owns those
generated regions. Android scheduling is registered by the plugin.

## Platform notes

- Native Android and iOS use SQLCipher through
  `@capacitor-community/sqlite`, including for unencrypted databases. Review the
  plugin's encryption-export compliance notice before shipping.
- Web/PWA builds should use `createIndexedDbSyncLocalStore` from
  `@absolutejs/sync/client`; they do not need the plugin's WASM/web component.
- The adapter serializes transactions so an app cannot overlap two explicit
  transactions on the same Capacitor connection.
- The native iOS worker currently targets the SQLite plugin's default Documents
  location. Applications that override `CapacitorSQLite.iosDatabaseLocation`
  must keep foreground and background database locations aligned before
  enabling managed background Sync.

## License

Apache-2.0.
