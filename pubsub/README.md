# @absolutejs/sync-pubsub

Google Cloud Pub/Sub **StreamingPull** reaction for AbsoluteJS Sync. The Google
SDK keeps an open connection, extends message leases and applies flow control.
There is no mailbox polling or public callback URL.

```ts
import { PubSub } from '@google-cloud/pubsub';
import { createPubSubReaction } from '@absolutejs/sync-pubsub';
const client = new PubSub({ projectId }); // host-owned credentials
const subscription = client.subscription(subscriptionName, {
	flowControl: { maxMessages: 10, allowExcessMessages: false }
});
const disconnect = await engine.connectSource(
	createPubSubReaction({
		subscription,
		process: async (message) => {
			// Validate payload and resolve tenant from trusted server configuration.
			// Atomically deduplicate message.id and enqueue durable work before return.
			await persistAndEnqueue(message);
		},
		onError: reportSanitizedError
	})
);
// Shutdown waits for in-flight handlers, then closes its subscription.
await disconnect();
await client.close();
```

Delivery is at least once. A crash after commit but before acknowledgment can
redeliver a message. Your handler must be idempotent; never acknowledge before
its durable transaction commits. Handler failure nacks for provider redelivery.
Configure retry/dead-letter policy on the subscription for poison messages.

Use a dedicated SDK subscription instance. `closeOnStop: false` leaves its
connection open for a host that owns cleanup. A stopped reaction cannot restart;
create another instance. Stream reconnect is owned by the SDK. `start` registers
listeners; it does not prove remote authorization or stream health. Monitor errors.
Handlers must bound their own work so graceful shutdown can complete. Credentials
and SDK network policy remain host concerns; message data never grants authority.
Database writes reach UI collections through the usual Sync database source.

License: Apache-2.0, matching the Sync adapter repository.
