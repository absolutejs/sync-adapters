import type { ChangeSource } from '@absolutejs/sync/engine';
import type { Message, Subscription } from '@google-cloud/pubsub';

/** Host-owned SDK subscription, configured with bounded flow control.
 * The host supplies credentials, subscription identity, and durable processing.
 * The adapter never turns untrusted message fields into an authenticated actor.
 */
export type PubSubReactionOptions = {
	subscription: Pick<Subscription, 'on' | 'removeListener' | 'close'>;
	/** Resolve only after committing idempotent durable work. A failure nacks. */
	process: (
		message: Pick<Message, 'id' | 'data' | 'attributes' | 'publishTime'>
	) => Promise<void>;
	/** This adapter owns the supplied subscription by default. */
	closeOnStop?: boolean;
	onError?: (error: unknown) => void;
};

/** A lifecycle-owned StreamingPull reaction. SDK handles the stream, flow
 * control and lease extension. Ack follows durable processing; a crash between
 * commit and ack can redeliver, so process MUST deduplicate by message identity.
 * No provider polling and no direct collection emissions: database writes flow
 * through the application's normal Sync revision source.
 */
export const createPubSubReaction = (
	options: PubSubReactionOptions
): ChangeSource => {
	let running = false;
	let closed = false;
	let stopping: Promise<void> | undefined;
	const inFlight = new Set<Promise<void>>();
	const report = (error: unknown) => {
		try {
			options.onError?.(error);
		} catch {
			/* Diagnostics cannot prevent nack/drain. */
		}
	};
	const onMessage = (message: Message) => {
		if (!running) {
			message.nack();
			return;
		}
		const work = (async () => {
			try {
				await options.process({
					id: message.id,
					data: message.data,
					attributes: message.attributes,
					publishTime: message.publishTime
				});
				message.ack();
			} catch (error) {
				report(error);
				try {
					message.nack();
				} catch (error) {
					report(error);
				}
			}
		})();
		inFlight.add(work);
		void work.finally(() => inFlight.delete(work));
	};
	return {
		start() {
			if (closed || stopping)
				throw new Error(
					'Pub/Sub reaction has stopped; create a new reaction to reconnect'
				);
			if (running) return;
			running = true;
			options.subscription.on('error', report);
			options.subscription.on('message', onMessage);
		},
		stop() {
			if (stopping) return stopping;
			running = false;
			closed = true;
			options.subscription.removeListener('message', onMessage);
			stopping = (async () => {
				try {
					await Promise.allSettled([...inFlight]);
					if (options.closeOnStop !== false)
						await options.subscription.close();
				} finally {
					options.subscription.removeListener('error', report);
				}
			})();
			return stopping;
		}
	};
};
