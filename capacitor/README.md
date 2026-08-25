# @absolutejs/sync-capacitor

Native persistence and lifecycle wiring for `@absolutejs/sync` applications
running in Capacitor. It stores confirmed rows, cursors, installation identity,
and the durable mutation outbox in one principal-partitioned SQLite database.

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

The namespace must come from a verified Auth principal, never from an untrusted
route or form value. AbsoluteJS derives an opaque namespace from the verified
issuer, public client ID, and subject. Signing out locks that partition by
removing it from the active runtime; it does not silently destroy offline data.
Signing back in as the same verified principal unlocks the same partition.

## Platform notes

- Native Android and iOS use SQLCipher through
  `@capacitor-community/sqlite`, including for unencrypted databases. Review the
  plugin's encryption-export compliance notice before shipping.
- Web/PWA builds should use `createIndexedDbSyncLocalStore` from
  `@absolutejs/sync/client`; they do not need the plugin's WASM/web component.
- The adapter serializes transactions so an app cannot overlap two explicit
  transactions on the same Capacitor connection.

## License

Apache-2.0.
